import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from astrbot.api.platform import AstrBotMessage, Platform, PlatformMetadata, register_platform_adapter
from astrbot.core.platform import MessageType
from astrbot.core.platform.astrbot_message import Group, MessageMember
from astrbot.api.message_components import At, Image, Plain

from .client import OttoHubClient
from .event import OttoHubMessageEvent

logger = logging.getLogger("astrbot")
_SHARED_CONTEXT = None
PLUGIN_CONFIG_PATH = Path("data/config/astrbot_plugin_ottohub_config.json")
_PLACEHOLDER_IMAGE_PATH = Path(__file__).parent / "image_failed_placeholder.png"


@dataclass
class ParsedNotification:
    kind: str
    tt: str
    suid: str
    sn: str
    rc: str
    pid: int = 0
    tid: int = 0


@dataclass
class ResolvedComment:
    text: str
    images: list[str]
    comment_id: int = 0
    reply_parent_id: int = 0
    is_child: bool = False
    parent_text: str = ""
    parent_uid: str = ""
    parent_author: str = ""
    matched: bool = False
    embedded_context: str = ""
    comment_time: str = ""


OTTOHUB_CONFIG_METADATA = {
    "cookie_json": {
        "description": "Cookie JSON",
        "type": "text",
        "condition": {"type": "ottohub"},
        "hint": "账号的 Cookie JSON 数据"
    },
    "user_agent": {
        "description": "User Agent",
        "type": "string",
        "condition": {"type": "ottohub"},
        "hint": "请求使用的 User-Agent"
    },
    "logo_token": {
        "description": "Logo Token",
        "type": "string",
        "invisible": True
    }
}


@register_platform_adapter(
    "ottohub",
    "OttoHub 适配器",
    logo_path="logo.png",
    config_metadata=OTTOHUB_CONFIG_METADATA,
    default_config_tmpl={
        "id": "ottohub",
        "type": "ottohub",
        "enable": True,
        "cookie_json": "{}",
        "user_agent": "Mozilla/5.0",
    },
)
class OttoHubPlatformAdapter(Platform):
    def __init__(self, platform_config: dict, platform_settings: dict, event_queue: asyncio.Queue):
        try:
            super().__init__(event_queue)
        except TypeError:
            super().__init__(platform_config, event_queue)
        self.event_queue = event_queue
        self.config = platform_config or {}
        self.context = _SHARED_CONTEXT
        self.client: OttoHubClient | None = None
        self.processed_ids: set[str] = set()
        self.processed_comment_keys: dict[str, float] = {}
        self.sent_post_context_keys: dict[str, float] = {}
        self.post_context_dedupe_path = Path("data/config/astrbot_plugin_ottohub_post_context_dedupe.json")
        self._load_post_context_dedupe()
        logger.info("[OttoHub] 适配器初始化完成")

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _as_int(value: Any) -> int:
        try:
            return int(value or 0)
        except Exception:
            return 0

    @staticmethod
    def _as_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        if isinstance(value, str):
            return value.strip().lower() not in {"0", "false", "no", "off", "关闭"}
        return bool(value)

    def _load_plugin_config(self) -> dict[str, Any]:
        try:
            with PLUGIN_CONFIG_PATH.open(encoding="utf-8-sig") as file_obj:
                data = json.load(file_obj)
                return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _config_value(self, key: str, default: Any, plugin_config: dict[str, Any] | None = None) -> Any:
        if plugin_config is None:
            plugin_config = self._load_plugin_config()
        if key in plugin_config:
            return plugin_config.get(key)
        return self.config.get(key, default)

    # ------------------------------------------------------------------ post-context dedupe persistence

    def _load_post_context_dedupe(self) -> None:
        try:
            if self.post_context_dedupe_path.exists():
                data = json.loads(self.post_context_dedupe_path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    self.sent_post_context_keys = {
                        str(key): float(value)
                        for key, value in data.items()
                        if isinstance(value, (int, float))
                    }
                    logger.info("[OttoHub] 已加载 %d 条帖子上下文去重记录", len(self.sent_post_context_keys))
        except Exception as exc:
            logger.warning("[OttoHub] 加载帖子上下文去重记录失败: %s", exc)

    def _save_post_context_dedupe(self) -> None:
        try:
            self.post_context_dedupe_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.post_context_dedupe_path.with_suffix(self.post_context_dedupe_path.suffix + ".tmp")
            tmp_path.write_text(
                json.dumps(self.sent_post_context_keys, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp_path.replace(self.post_context_dedupe_path)
        except Exception as exc:
            logger.warning("[OttoHub] 保存帖子上下文去重记录失败: %s", exc)

    # ------------------------------------------------------------------ text utilities

    @staticmethod
    def _extract_images(text: str) -> list[str]:
        return re.findall(r"https?://[^\s\"'<>]+\.(?:jpg|jpeg|png|gif|webp|bmp|svg)", text or "", re.I)

    @staticmethod
    def _dedupe(values: list[str]) -> list[str]:
        seen = set()
        result = []
        for value in values:
            if value and value not in seen:
                seen.add(value)
                result.append(value)
        return result

    @staticmethod
    def _normalize_mentions(text: str) -> str:
        def repl(match: re.Match) -> str:
            label = str(match.group(1) or "").strip()
            uid = str(match.group(2) or "").strip()
            if not label:
                label = "@用户"
            if not label.startswith("@"):
                label = "@" + label
            return f"{label}(UID:{uid})"

        text = str(text or "")
        text = re.sub(r"\[([^\]]+)\]\(https?://www\.ottohub\.cn/u/(\d+)\)", repl, text)
        text = re.sub(r"\[([^\]]+)\]\(/u/(\d+)\)", repl, text)
        return text

    @staticmethod
    def _replace_invalid_images(text: str, invalid_urls: list[str]) -> str:
        text = str(text or "")
        for url in invalid_urls:
            if not url:
                continue
            escaped = re.escape(url)
            text = re.sub(r"!\[[^\]]*\]\(" + escaped + r"\)", "[图片加载失败]", text)
            text = text.replace(url, "[图片加载失败]")
        return re.sub(r"(?:\[图片加载失败\]\s*){2,}", "[图片加载失败]\n", text).strip()

    @classmethod
    def _strip_context_images(cls, text: str) -> str:
        text = str(text or "")
        text = re.sub(r"!\[[^\]]*\]\(https?://[^\s)]+\)", "[此处图片已省略]", text)
        text = re.sub(r"https?://[^\s\"'<>]+\.(?:jpg|jpeg|png|gif|webp|bmp|svg)", "[此处图片已省略]", text, flags=re.I)
        return re.sub(r"(?:\[此处图片已省略\]\s*){2,}", "[此处图片已省略]\n", text).strip()

    @staticmethod
    def _split_embedded_context(text: str) -> tuple[str, str]:
        text = str(text or "").strip()
        if not text:
            return "", ""
        context_parts: list[str] = []
        cleaned = re.sub(
            r"<system_reminder>.*?</system_reminder>",
            lambda match: context_parts.append(match.group(0)) or "\n",
            text,
            flags=re.S | re.I,
        )
        cleaned = re.sub(
            r"^\s*\[Image Attachment:[^\]]+\]\s*$",
            lambda match: context_parts.append(match.group(0)) or "",
            cleaned,
            flags=re.M,
        )
        markers = ("帖子信息：", "帖子信息:", "帖子内容：", "帖子内容:")
        if any(marker in cleaned for marker in markers):
            lines = [line.strip() for line in cleaned.splitlines()]
            user_lines: list[str] = []
            context_lines: list[str] = []
            seen_context = False
            for line in lines:
                if not line:
                    continue
                is_context = (
                    line.startswith(("帖子信息：", "帖子信息:", "帖子内容：", "帖子内容:"))
                    or line.startswith("![")
                    or line.startswith("[Image Attachment:")
                )
                if is_context:
                    seen_context = True
                    context_lines.append(line)
                    continue
                if seen_context:
                    user_lines.append(line)
                else:
                    context_lines.append(line)
            if user_lines:
                return "\n".join(user_lines).strip(), "\n".join(context_lines + context_parts).strip()
        return cleaned.strip(), "\n".join(context_parts).strip()

    def _strip_bot_mentions_from_user_text(self, text: str) -> str:
        text = str(text or "").strip()
        if not text:
            return ""
        uid = re.escape(str(getattr(self.client, "uid", "") or ""))
        patterns = [
            r"@[^\s()（）]+\(UID:" + uid + r"\)",
            r"@[^\s()（）]+（UID:" + uid + r"）",
            r"\[@[^\]]+\]\(https?://www\.ottohub\.cn/u/" + uid + r"\)",
            r"\[@typer\]\(https?://www\.ottohub\.cn/u/" + uid + r"\)",
            r"\[@typer\]\(/u/" + uid + r"\)",
            r"\[@typer\]\([^)]+\)",
            r"@typer\b",
        ]
        if uid:
            patterns.extend([
                r"\[[^\]]+\]\(https?://www\.ottohub\.cn/u/" + uid + r"\)",
                r"\[[^\]]+\]\(/u/" + uid + r"\)",
                r"@" + uid + r"\b",
            ])
        for pattern in patterns:
            text = re.sub(pattern, "", text, flags=re.I)
        text = re.sub(r"\[\]\([^)]+\)", "", text)
        if uid:
            text = re.sub(r"^\s*[\(（]UID[:：]" + uid + r"[\)）]\s*", "", text, flags=re.I)
        text = re.sub(r"^\s*[,，:：;；、\-\s]+", "", text)
        return self._normalize_mentions(re.sub(r"[ \t]{2,}", " ", text).strip())

    @staticmethod
    def _command_text_for_dispatch(text: str) -> str:
        text = str(text or "").strip()
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _comment_text(comment: dict[str, Any]) -> str:
        return OttoHubPlatformAdapter._normalize_mentions(
            str(comment.get("content") or comment.get("message") or comment.get("text") or "").strip()
        )

    @staticmethod
    def _comment_uid(comment: dict[str, Any]) -> str:
        return str(comment.get("uid") or comment.get("author_uid") or comment.get("user_id") or "").strip()

    def _comment_id(self, kind: str, comment: dict[str, Any]) -> int:
        key = "bcid" if kind == "blog" else "vcid"
        return self._as_int(comment.get(key) or comment.get("cid") or comment.get("id"))

    # ------------------------------------------------------------------ comment resolution

    def _nearest_previous_bot_child(self, children: list[dict[str, Any]], matched_child: dict[str, Any]) -> dict[str, Any] | None:
        bot_uid = str(getattr(self.client, "uid", "") or "")
        if not bot_uid:
            return None
        child_time = str(matched_child.get("time") or "")
        candidates = []
        for item in children:
            if item is matched_child:
                continue
            if self._comment_uid(item) != bot_uid:
                continue
            item_time = str(item.get("time") or "")
            if child_time and item_time and item_time > child_time:
                continue
            candidates.append(item)
        if not candidates:
            return None
        return max(candidates, key=lambda item: str(item.get("time") or ""))

    def _mentions_bot(self, text: str) -> bool:
        uid = str(getattr(self.client, "uid", "") or "")
        return bool(uid and (uid in text or f"/u/{uid}" in text)) or "@typer" in text

    async def _get_comments(self, kind: str, post_id: int, parent_id: int, offset: int, num: int):
        if kind == "blog":
            return await self.client.get_blog_comments(post_id, num=num, parent_bcid=parent_id, offset=offset)
        return await self.client.get_video_comments(post_id, num=num, parent_vcid=parent_id, offset=offset)

    async def _iter_recent_root_comments(self, kind: str, post_id: int):
        for offset in (0, 12):
            rows = await self._get_comments(kind, post_id, 0, offset, 12)
            for row in rows:
                yield row
            if len(rows) < 12:
                break

    async def _find_matching_child(self, n: ParsedNotification, root: dict[str, Any]) -> ResolvedComment | None:
        root_id = self._comment_id(n.kind, root)
        if not root_id:
            return None
        for offset in (0, 6):
            children = await self._get_comments(n.kind, n.pid, root_id, offset, 6)
            for child in children:
                child_id = self._comment_id(n.kind, child)
                child_uid = self._comment_uid(child)
                text = self._comment_text(child)
                id_match = n.tid and child_id == n.tid
                uid_match = child_uid and child_uid == str(n.suid)
                mention_match = n.tt == "at_mention" and self._mentions_bot(text)
                if id_match or (uid_match and (not n.tid or mention_match or "回复" in n.rc)):
                    parent = self._nearest_previous_bot_child(children, child) or root
                    return ResolvedComment(
                        text=text or n.rc,
                        images=self._extract_images(text),
                        comment_id=child_id,
                        reply_parent_id=root_id,
                        is_child=True,
                        parent_text=self._comment_text(parent),
                        parent_uid=self._comment_uid(parent),
                        parent_author=str(parent.get("username") or parent.get("author") or ""),
                        matched=True,
                        comment_time=str(child.get("time") or ""),
                    )
            if len(children) < 6:
                break
        return None

    async def _resolve_comment(self, n: ParsedNotification) -> ResolvedComment:
        if n.kind == "dm" or not n.pid:
            return ResolvedComment(text=n.rc, images=self._extract_images(n.rc), matched=True)
        best_uid_match: ResolvedComment | None = None
        try:
            async for root in self._iter_recent_root_comments(n.kind, n.pid):
                root_id = self._comment_id(n.kind, root)
                root_uid = self._comment_uid(root)
                root_text = self._comment_text(root)
                if root_id and n.tid and root_id == n.tid:
                    child = await self._find_matching_child(n, root)
                    if child:
                        return child
                    return ResolvedComment(root_text or n.rc, self._extract_images(root_text), root_id, root_id, matched=True, comment_time=str(root.get("time") or ""))
                if root_uid == str(n.suid):
                    root_match = ResolvedComment(root_text or n.rc, self._extract_images(root_text), root_id, root_id, matched=True, comment_time=str(root.get("time") or ""))
                    if n.tt == "at_mention" and self._mentions_bot(root_text):
                        return root_match
                    if best_uid_match is None:
                        best_uid_match = root_match
                child = await self._find_matching_child(n, root)
                if child:
                    return child
            if best_uid_match:
                return best_uid_match
        except Exception as exc:
            logger.error("[OttoHub] 评论解析失败: %s", exc, exc_info=True)
        fallback_parent = n.tid if n.tid else 0
        return ResolvedComment(n.rc, self._extract_images(n.rc), n.tid, fallback_parent, matched=False)

    # ------------------------------------------------------------------ post context

    async def _post_context(self, kind: str, post_id: int) -> dict[str, Any]:
        if not post_id:
            return {}
        try:
            data = await (self.client.get_blog_detail(post_id) if kind == "blog" else self.client.get_video_detail(post_id))
            if data.get("status") != "success":
                return {}
            return data
        except Exception:
            return {}

    @classmethod
    def _post_author_uid(cls, post: dict[str, Any]) -> str:
        if not isinstance(post, dict):
            return ""
        for key in ("uid", "author_uid", "user_id", "owner_uid", "creator_uid", "uploader_uid", "mid"):
            value = post.get(key)
            if value not in (None, ""):
                return str(value).strip()
        for key in ("author", "user", "owner", "creator", "uploader"):
            value = post.get(key)
            if isinstance(value, dict):
                nested = cls._post_author_uid(value)
                if nested:
                    return nested
        return ""

    def _is_own_post_comment_reply(self, post: dict[str, Any], resolved: ResolvedComment) -> bool:
        if not resolved.is_child:
            return False
        bot_uid = str(getattr(self.client, "uid", "") or "").strip()
        return bool(bot_uid and self._post_author_uid(post) == bot_uid)

    def _relation_text(self, relation: dict[str, Any]) -> str:
        if relation.get("is_mutual"):
            return "互相关注；该用户是你的粉丝；你已关注该用户"
        if relation.get("is_fan"):
            return "该用户是你的粉丝"
        if relation.get("is_following"):
            return "你已关注该用户"
        return "该用户不是你的粉丝"

    def _event_type_text(self, n: ParsedNotification, resolved: ResolvedComment | None = None) -> str:
        child_suffix = "下的回复" if resolved and resolved.is_child else ""
        if n.kind == "blog":
            return f"动态评论{child_suffix}（@）" if n.tt == "at_mention" else f"动态评论{child_suffix}"
        if n.kind == "video":
            return f"视频评论{child_suffix}（@）" if n.tt == "at_mention" else f"视频评论{child_suffix}"
        return "私信"

    def _post_images(self, post: dict[str, Any]) -> list[str]:
        images = self._extract_images(str(post.get("content") or ""))
        thumbnails = post.get("thumbnails")
        if isinstance(thumbnails, str):
            images.extend(self._extract_images(thumbnails))
        elif isinstance(thumbnails, list):
            for item in thumbnails:
                if isinstance(item, str):
                    images.extend(self._extract_images(item))
                elif isinstance(item, dict):
                    images.extend(self._extract_images(" ".join(str(v) for v in item.values())))
        return self._dedupe(images)

    def _post_content_text(self, post: dict[str, Any], use_external_ocr: bool = False, start_index: int = 1) -> str:
        invalid_images = list(post.get("_ottohub_invalid_images") or [])
        valid_images = list(post.get("_ottohub_valid_images") or [])

        content = str(post.get("content") or "").strip()
        post_images_in_content = self._extract_images(content)
        all_post_images = self._dedupe(post_images_in_content + valid_images + invalid_images)

        for url in invalid_images:
            if url:
                escaped = re.escape(url)
                content = re.sub(r"!\[[^\]]*\]\(" + escaped + r"\)", "[图片加载失败]", content)
                content = content.replace(url, "[图片加载失败]")

        if use_external_ocr:
            url_to_placeholder = {}
            for idx, url in enumerate(all_post_images):
                num = start_index + idx
                if url in valid_images:
                    url_to_placeholder[url] = f"[图片 {num}]"
                else:
                    url_to_placeholder[url] = f"[图片 {num} (图片加载失败)]"

            def repl_markdown(match: re.Match) -> str:
                url = match.group(1) or ""
                return url_to_placeholder.get(url, match.group(0))

            content = re.sub(r"!\[[^\]]*\]\((https?://[^\)]+)\)", repl_markdown, content)
            for url, placeholder in url_to_placeholder.items():
                content = content.replace(url, placeholder)

            extra_lines = [
                url_to_placeholder[url]
                for url in all_post_images
                if url not in post_images_in_content
            ]
        else:
            extra_lines = []
            for url in all_post_images:
                if url not in post_images_in_content:
                    extra_lines.append(f"![]({url})" if url in valid_images else "[图片加载失败]")

        parts = [content] if content else []
        parts.extend(extra_lines)
        return "\n".join(parts).strip() or "[无文字内容]"

    async def _classify_reachable_images(self, urls: list[str], limit: int = 10) -> tuple[list[str], list[str]]:
        valid: list[str] = []
        invalid: list[str] = []
        for url in urls:
            if await self.client.validate_image_url(url):
                if len(valid) < limit:
                    valid.append(url)
            else:
                invalid.append(url)
                logger.warning("[OttoHub] 图片不可达，已丢弃: %s", url)
        return valid, invalid

    # ------------------------------------------------------------------ prompt building

    def _build_system_context_prompt(self) -> str:
        return (
            "[OttoHub处理规则]\n"
            "用户 prompt 采用结构化事件格式，其中帖子信息、动态内容和图片附件都是上下文。\n"
            "回复时应回应第一行里的本次评论者和本次评论内容；父评论和动态内容只作为上下文，不要把父评论当成本次用户输入。"
            "如果评论内容是\"[仅 @ 机器人或空内容]\"，则结合动态内容自然回应。"
        )

    def _build_structured_user_prompt(
        self,
        n: ParsedNotification,
        resolved: ResolvedComment,
        relation: dict[str, Any],
        post: dict[str, Any],
        raw_msg: dict[str, Any],
        include_post_context: bool = True,
        use_external_ocr: bool = False,
        plugin_config: dict[str, Any] | None = None,
    ) -> str:
        comment_text = resolved.text.strip() or "[仅 @ 机器人或空内容]"
        comment_time = resolved.comment_time or str(raw_msg.get("time") or "")
        if not comment_time:
            comment_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        if not include_post_context and n.kind != "dm":
            lines = [f"有人@了你：{n.sn}(UID:{n.suid})：{comment_text}"]
            if resolved.is_child:
                parent_desc = "机器人评论" if str(resolved.parent_uid) == str(getattr(self.client, "uid", "")) else "父评论"
                lines.append(f"类型：回复{parent_desc}")
            parent_text = self._strip_context_images(resolved.parent_text)
            if parent_text and resolved.is_child and parent_text.strip() != comment_text:
                if str(resolved.parent_uid) != str(getattr(self.client, "uid", "")):
                    parent_header = f"父评论(by {resolved.parent_author or '未知'})"
                    lines.extend(["", f"[{parent_header}]\n{parent_text[:200]}"])
            if resolved.embedded_context:
                lines.append(resolved.embedded_context[:500])
            return "\n".join(lines).strip()

        lines = [
            f"有人@了你：{n.sn}(UID:{n.suid})：{comment_text}",
            "",
            "[说明]",
            f"类型：{self._event_type_text(n, resolved)}",
            f"评论时间：{comment_time}",
            f"用户关系：{self._relation_text(relation)}",
        ]
        if resolved.is_child:
            parent_desc = "机器人评论" if str(resolved.parent_uid) == str(getattr(self.client, "uid", "")) else "父评论"
            lines.append(f"线程关系：本次评论是{parent_desc}下的回复；第一行是本次新评论，父评论只作上下文。")
        if n.kind != "dm":
            title = str(post.get("title") or "").strip()
            section_name = "动态内容" if n.kind == "blog" else "视频内容"
            lines.extend(["", f"[{section_name}：@评论所在的{'动态' if n.kind == 'blog' else '视频'}]"])
            if title:
                lines.append(f"帖子标题：{title}")
            if include_post_context:
                author = str(post.get("username") or post.get("author") or "").strip() or "未知"
                post_time = str(post.get("time") or "").strip() or "未知"
                info_parts = [f"作者：{author}", f"时间：{post_time}"]
                show_comment = self._as_bool(self._config_value("include_post_comment_count", True, plugin_config), True)
                show_like = self._as_bool(self._config_value("include_post_like_count", True, plugin_config), True)
                show_cold = self._as_bool(self._config_value("include_post_cold_count", True, plugin_config), True)
                show_view = self._as_bool(self._config_value("include_post_view_count", True, plugin_config), True)
                if show_comment:
                    info_parts.append(f"评论数：{post.get('comment_count', '未知')}")
                if show_like:
                    info_parts.append(f"点赞数：{post.get('like_count', '未知')}")
                if show_cold:
                    info_parts.append(f"冷藏数：{post.get('favorite_count', '未知')}")
                if show_view:
                    info_parts.append(f"观看次数：{post.get('view_count', '未知')}")
                lines.append("帖子信息：" + "；".join(info_parts))
                raw_image_urls = self._dedupe(resolved.images)
                lines.append(f"帖子内容：{self._post_content_text(post, use_external_ocr, start_index=len(raw_image_urls) + 1)}")
            else:
                lines.append("帖子内容：已在本会话前文提供，本次不重复附带。")
            parent_text = self._strip_context_images(resolved.parent_text)
            if parent_text and resolved.is_child and parent_text.strip() != comment_text:
                if str(resolved.parent_uid) != str(getattr(self.client, "uid", "")):
                    parent_header = "父评论"
                    if resolved.parent_author:
                        parent_header = f"父评论：{resolved.parent_author}(UID:{resolved.parent_uid or '未知'})"
                    lines.extend(["", f"[{parent_header}]\n{parent_text[:500]}"])
            if resolved.embedded_context:
                lines.append(resolved.embedded_context[:1000])
        return "\n".join(lines).strip()

    # ------------------------------------------------------------------ deduplication / filtering

    def _is_enabled_for(self, kind: str, plugin_config: dict[str, Any] | None = None) -> bool:
        if kind == "dm":
            return bool(self._config_value("reply_in_dm", True, plugin_config))
        if kind == "blog":
            return bool(self._config_value("reply_in_blog_comments", True, plugin_config))
        if kind == "video":
            return bool(self._config_value("reply_in_video_comments", True, plugin_config))
        return True

    def _seen_comment_key(self, kind: str, post_id: int, comment_id: int) -> bool:
        if kind == "dm" or not post_id or not comment_id:
            return False
        now = time.time()
        for key, created_at in list(self.processed_comment_keys.items()):
            if now - created_at > 6 * 3600:
                self.processed_comment_keys.pop(key, None)
        key = f"{kind}:{post_id}:{comment_id}"
        if key in self.processed_comment_keys:
            return True
        self.processed_comment_keys[key] = now
        return False

    def _should_include_post_context(
        self, session_id: str, kind: str, post_id: int, parent_id: int, plugin_config: dict[str, Any] | None = None
    ) -> bool:
        if kind == "dm" or not post_id:
            return True
        enabled = self._as_bool(self._config_value("dedupe_post_context", True, plugin_config), True)
        if not enabled:
            return True
        ttl = self._as_int(self._config_value("post_context_dedupe_ttl_seconds", 21600, plugin_config)) or 21600
        now = time.time()
        changed = False
        for key, created_at in list(self.sent_post_context_keys.items()):
            if now - created_at > ttl:
                self.sent_post_context_keys.pop(key, None)
                changed = True
        key = f"{session_id}:{kind}:{post_id}:{parent_id}"
        if key in self.sent_post_context_keys:
            if changed:
                self._save_post_context_dedupe()
            return False
        self.sent_post_context_keys[key] = now
        self._save_post_context_dedupe()
        return True

    # ------------------------------------------------------------------ placeholder image

    def _ensure_placeholder_image(self) -> bool:
        """确保图片加载失败占位图存在，返回是否可用。"""
        if os.path.exists(_PLACEHOLDER_IMAGE_PATH):
            return True
        try:
            from PIL import Image as PILImage, ImageDraw
            img = PILImage.new("RGB", (300, 100), color=(220, 220, 220))
            d = ImageDraw.Draw(img)
            d.line([(0, 0), (300, 100)], fill=(255, 0, 0), width=3)
            d.line([(0, 100), (300, 0)], fill=(255, 0, 0), width=3)
            d.text((70, 45), "Image Load Failed", fill=(255, 0, 0))
            img.save(_PLACEHOLDER_IMAGE_PATH)
            return True
        except Exception as exc:
            logger.warning("[OttoHub] 创建图片占位图失败: %s", exc)
            return False

    # ------------------------------------------------------------------ message processing

    async def _process_message(self, msg: dict[str, Any]) -> None:
        mid = msg.get("msg_id")
        mk = str(mid) if mid is not None else None
        if not mk or mk in self.processed_ids:
            if mk:
                await self.client.mark_message_read(int(mid))
            return
        self.processed_ids.add(mk)
        rc = str(msg.get("content") or "")
        suid = str(msg.get("sender") or "0")
        logger.debug("[OttoHub] 收到原始消息 msg_id=%s: %s", mk, msg)

        kind = "dm"
        pid = 0
        tid = 0
        tt = "dm"
        if suid == "0":
            ruid_m = re.search(r"UID[:：](\d+)", rc) or re.search(r"\(UID:(\d+)\)", rc)
            ruid = ruid_m.group(1) if ruid_m else "0"
            bid_m = re.search(r"BID[:：](\d+)", rc) or re.search(r"\(BID:(\d+)\)", rc)
            vid_m = re.search(r"VID[:：](\d+)", rc) or re.search(r"\(VID:(\d+)\)", rc)
            bc_m = re.search(r"BCID[:：](\d+)", rc) or re.search(r"\(BCID:(\d+)\)", rc)
            vc_m = re.search(r"VCID[:：](\d+)", rc) or re.search(r"\(VCID:(\d+)\)", rc)
            if bid_m:
                kind = "blog"
                pid = int(bid_m.group(1))
                tid = int(bc_m.group(1)) if bc_m else 0
            elif vid_m:
                kind = "video"
                pid = int(vid_m.group(1))
                tid = int(vc_m.group(1)) if vc_m else 0
            sn = rc.split("(", 1)[0].strip() if "(" in rc else "系统"
            tt = "at_mention" if "@了你" in rc else "comment_reply"
        else:
            ruid = suid
            sn = str(msg.get("sender_name") or "用户")

        parsed = ParsedNotification(kind, tt, ruid, sn, rc, pid, tid)
        plugin_config = self._load_plugin_config()

        if not self._is_enabled_for(kind, plugin_config):
            await self.client.mark_message_read(int(mid))
            return
        if str(ruid) == str(getattr(self.client, "uid", "")):
            await self.client.mark_message_read(int(mid))
            return

        resolved = await self._resolve_comment(parsed)
        if self._seen_comment_key(kind, pid, resolved.comment_id):
            logger.info(
                "[OttoHub] 跳过重复通知 kind=%s post_id=%s comment_id=%s msg_id=%s",
                kind, pid, resolved.comment_id, mid,
            )
            await self.client.mark_message_read(int(mid))
            return

        raw_image_urls = self._dedupe(resolved.images)
        user_text, embedded_context = self._split_embedded_context(resolved.text)
        user_text = self._strip_bot_mentions_from_user_text(user_text)
        if embedded_context:
            resolved.embedded_context = embedded_context

        relation = await self.client.get_bot_relationship(ruid)
        post = await self._post_context(kind, pid)

        if kind != "dm" and self._is_own_post_comment_reply(post, resolved):
            logger.info(
                "[OttoHub] 跳过自己帖子下的评论回复 kind=%s post_id=%s comment_id=%s msg_id=%s",
                kind, pid, resolved.comment_id, mid,
            )
            await self.client.mark_message_read(int(mid))
            return

        sid = f"dm-{ruid}" if kind == "dm" else f"{kind}-{pid or ruid}"

        # 检查会话历史（仅用于诊断，不影响去重逻辑）
        history_is_empty = True
        umo = f"ottohub:{'GroupMessage' if kind != 'dm' else 'FriendMessage'}:{sid}"
        try:
            if self.context and hasattr(self.context, "conversation_manager"):
                cid = await self.context.conversation_manager.get_curr_conversation_id(umo)
                if cid:
                    conv = await self.context.conversation_manager.get_conversation(umo, cid)
                    if conv and conv.history:
                        try:
                            history_list = json.loads(conv.history)
                            if isinstance(history_list, list) and len(history_list) > 0:
                                history_is_empty = False
                        except Exception:
                            pass
        except Exception as exc:
            logger.error("[OttoHub] 检查会话历史失败: %s", exc)

        is_reset = "/reset" in (user_text or "").lower() or "/reset" in (rc or "").lower()
        if is_reset:
            keys_to_remove = [k for k in self.sent_post_context_keys if k.startswith(f"{sid}:")]
            for k in keys_to_remove:
                self.sent_post_context_keys.pop(k, None)
            self._save_post_context_dedupe()
            logger.info("[OttoHub] 已清除会话 %s 的帖子上下文去重记录（reset 指令）", sid)
            include_post_context = True
        else:
            if history_is_empty:
                logger.debug("[OttoHub] 会话 %s 历史为空，保留帖子上下文去重状态", sid)
            include_post_context = self._should_include_post_context(sid, kind, pid, resolved.reply_parent_id, plugin_config)

        post_images = self._post_images(post) if include_post_context else []
        all_image_urls = self._dedupe(raw_image_urls + post_images)
        all_images, invalid_images = await self._classify_reachable_images(all_image_urls)
        post["_ottohub_valid_images"] = [url for url in all_images if url in post_images]
        post["_ottohub_invalid_images"] = [url for url in invalid_images if url in post_images]
        post["_ottohub_context_deduped"] = not include_post_context

        has_placeholder = self._ensure_placeholder_image()

        resolved.text = self._replace_invalid_images(user_text or "", [url for url in invalid_images if url in raw_image_urls])

        use_external_ocr = self._as_bool(self._config_value("use_external_ocr", False, plugin_config), False)
        if use_external_ocr and raw_image_urls:
            for idx, url in enumerate(raw_image_urls):
                num = idx + 1
                placeholder = f"[图片 {num}]" if url in all_images else f"[图片 {num} (图片加载失败)]"
                resolved.text = resolved.text.replace(url, placeholder)
                resolved.text = re.sub(r"!\[[^\]]*\]\(" + re.escape(url) + r"\)", placeholder, resolved.text)

        dispatch_text = self._command_text_for_dispatch(resolved.text)
        event_text = dispatch_text or resolved.text.strip() or "[仅 @ 机器人或空内容]"

        components = []
        if kind != "dm":
            components.append(At(qq=str(self.client.uid)))
        components.append(Plain(event_text))

        logo_fallback = str(Path(__file__).parent / "logo.png")
        for url in all_image_urls:
            if url in all_images:
                components.append(Image.fromURL(url))
            elif has_placeholder:
                components.append(Image.fromFileSystem(_PLACEHOLDER_IMAGE_PATH))
            elif os.path.exists(logo_fallback):
                components.append(Image.fromFileSystem(logo_fallback))
            else:
                components.append(Plain("[图片加载失败]"))

        mo = AstrBotMessage()
        mo.timestamp = int(time.time())
        mo.raw_message = msg
        mo.self_id = str(self.client.uid or "")
        mo.session_id = sid
        mo.message_id = mk
        mo.type = MessageType.GROUP_MESSAGE if kind != "dm" else MessageType.FRIEND_MESSAGE
        mo.group_id = str(pid) if kind != "dm" else ""
        mo.sender = MessageMember(user_id=ruid, nickname=sn)
        if kind != "dm":
            mo.group = Group(group_id=str(pid))
        mo.components = components
        mo.message = components
        mo.message_str = event_text

        event = OttoHubMessageEvent(
            event_text,
            mo,
            self.meta(),
            sid,
            self.client,
            self.context if self.context else _SHARED_CONTEXT,
            kind + "_comment" if kind != "dm" else "dm",
            pid,
            resolved.reply_parent_id,
            int(ruid),
        )
        use_external_ocr = self._as_bool(self._config_value("use_external_ocr", False, plugin_config), False)
        structured_prompt = self._build_structured_user_prompt(
            parsed, resolved, relation, post, msg,
            include_post_context=include_post_context,
            use_external_ocr=use_external_ocr,
            plugin_config=plugin_config,
        )
        event.set_extra("_ottohub_user_prompt", structured_prompt)
        event.set_extra("_ottohub_context_prompt", self._build_system_context_prompt())
        event.set_extra("_ottohub_llm_prompt", structured_prompt)
        event.set_extra("_ottohub_reply_parent_id", resolved.reply_parent_id)
        event.set_extra("_ottohub_comment_id", resolved.comment_id)
        event.set_extra("_ottohub_relation", relation)
        event.is_at_or_wake_command = True
        event.should_call_llm(False)
        self.commit_event(event)
        await self.client.mark_message_read(int(mid))
        logger.info(
            "[OttoHub] 已提交事件 kind=%s session=%s user=%s(%s) comment_id=%s",
            kind, sid, sn, ruid, resolved.comment_id,
        )

    # ------------------------------------------------------------------ main loop

    async def run(self):
        cs = self._config_value("cookie_json", "{}")
        ua = self._config_value("user_agent", "")
        try:
            cookies = json.loads(cs) if isinstance(cs, str) else cs
            if isinstance(cookies, list):
                cookies = {c["name"]: c["value"] for c in cookies if "name" in c and "value" in c}
        except Exception as exc:
            logger.error("[OttoHub] cookie_json 解析失败: %s", exc)
            return
        plugin_config = self._load_plugin_config()
        self.client = OttoHubClient(
            cookies=cookies,
            user_agent=ua,
            image_upload_validate=self._config_value("image_upload_validate", True, plugin_config),
            image_upload_attempts=self._config_value("image_upload_attempts", 3, plugin_config),
            image_upload_check_delay=self._config_value("image_upload_check_delay_seconds", 5, plugin_config),
            image_upload_retry_delay=self._config_value("image_upload_retry_delay_seconds", 5, plugin_config),
            resend_failed_messages=self._config_value("resend_failed_messages", False, plugin_config),
            resend_attempts=self._config_value("resend_attempts", 3, plugin_config),
            resend_delay_seconds=self._config_value("resend_delay_seconds", 15, plugin_config),
            resend_dedupe_ttl_seconds=self._config_value("resend_dedupe_ttl_seconds", 3600, plugin_config),
        )
        if await self.client.verify_session():
            logger.info("[OttoHub] 登录成功，UID=%s", self.client.uid)
        else:
            logger.warning("[OttoHub] Session 验证失败，仍继续轮询")
        while True:
            try:
                messages = await self.client.get_unread_messages()
                for message in messages:
                    await self._process_message(message)
            except Exception as exc:
                logger.error("[OttoHub] 轮询异常: %s", exc, exc_info=True)
            await asyncio.sleep(5)

    def meta(self) -> PlatformMetadata:
        return PlatformMetadata(id="ottohub", name="OttoHub", description="OttoHub Adapter", logo_path="logo.png")
