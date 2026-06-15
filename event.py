import asyncio
import logging
import re

from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import At, Image, Plain
from astrbot.api.platform import AstrBotMessage, PlatformMetadata
from astrbot.api.provider import LLMResponse
from astrbot.api.star import Context

from .client import OttoHubClient
from .text_utils import sanitize_text, truncate_chars

logger = logging.getLogger("astrbot")

# OttoHub 平台单条消息/评论的字符硬上限
OTTOHUB_PLATFORM_MAX_CHARS = 400
# 单条默认字符上限（自动分段时为每段上限，关闭分段时为截断上限；默认 350 给表情包预留空间）
DEFAULT_MESSAGE_MAX_CHARS = 350
OTTOHUB_SEGMENT_DELAY_SECONDS = 5.0
OTTOHUB_SPLIT_MARKER_RE = re.compile(
    r"(?im)(?:[ \t\r\n]*(?:<SPLIT>|<ottohub:split>|<OTTOHUB_SPLIT>|\[SPLIT\]|\[OTTOHUB_SPLIT\]|---SPLIT---|---OTTOHUB_SPLIT---)[ \t\r\n]*|^[ \t]*(?:SPLIT|OTTOHUB_SPLIT)[ \t]*$)"
)


class OttoHubMessageEvent(AstrMessageEvent):
    def __init__(
        self,
        message_str: str,
        message_obj: AstrBotMessage,
        platform_meta: PlatformMetadata,
        session_id: str,
        client: OttoHubClient,
        context: Context,
        reply_type: str = "dm",
        context_id: int = 0,
        parent_id: int = 0,
        receiver_uid: int | None = None,
    ):
        super().__init__(message_str=message_str, message_obj=message_obj, platform_meta=platform_meta, session_id=session_id)
        self.client = client
        self.reply_type = reply_type
        self.context_id = context_id
        self.parent_id = parent_id
        self.receiver_uid = receiver_uid
        self.context = context

    # ------------------------------------------------------------------ text sanitization

    @staticmethod
    def _sanitize_outgoing_text(text: str) -> str:
        return sanitize_text(text)

    # ------------------------------------------------------------------ meme manager integration

    def _get_meme_plugin(self):
        if not self.context:
            return None
        for meta in self.context.get_all_stars():
            if getattr(meta, "name", "") == "meme_manager":
                return getattr(meta, "star_cls", None)
        return None

    async def _detect_meme_for_text(self, text: str) -> str:
        meme_plugin = self._get_meme_plugin()
        if not meme_plugin or not hasattr(meme_plugin, "resp"):
            return text
        try:
            response = LLMResponse(role="assistant", completion_text=text)
            await meme_plugin.resp(self, response)
            return str(response.completion_text or text).strip()
        except Exception as exc:
            logger.error("[OttoHub] Meme 检测失败: %s", exc, exc_info=True)
            return text

    async def _apply_meme_manager(self, text: str) -> tuple[str, list[str]]:
        from astrbot.core.message.components import Image as CoreImage
        from astrbot.core.message.components import Plain as CorePlain
        from astrbot.core.message.message_event_result import MessageChain as CoreMessageChain
        from astrbot.core.message.message_event_result import MessageEventResult

        clean_text = self._sanitize_outgoing_text(text or "")
        chain = CoreMessageChain([CorePlain(clean_text)])
        original_result = self.get_result()
        self.set_result(MessageEventResult(chain=chain))
        text_parts: list[str] = []
        image_urls: list[str] = []
        try:
            meme_plugin = self._get_meme_plugin()
            if meme_plugin and hasattr(meme_plugin, "on_decorating_result"):
                await meme_plugin.on_decorating_result(self)
            result = self.get_result()
            new_chain = result.chain if result else []
            comp_list = new_chain.chain if hasattr(new_chain, "chain") else new_chain
            for comp in comp_list:
                if isinstance(comp, str):
                    value = self._sanitize_outgoing_text(comp)
                    if value:
                        text_parts.append(value)
                elif isinstance(comp, (Plain, CorePlain)) or hasattr(comp, "text"):
                    value = self._sanitize_outgoing_text(str(getattr(comp, "text", "")))
                    if value:
                        text_parts.append(value)
                elif isinstance(comp, (Image, CoreImage)) or comp.__class__.__name__ == "Image" or hasattr(comp, "path") or hasattr(comp, "file"):
                    url = await self._image_component_to_url(comp)
                    if url:
                        image_urls.append(url)
            pending_images = self.get_extra("meme_manager_pending_images") or []
            for comp in pending_images:
                url = await self._image_component_to_url(comp)
                if url:
                    image_urls.append(url)
            self.set_extra("meme_manager_pending_images", [])
        except Exception as exc:
            logger.error("[OttoHub] Meme 处理失败: %s", exc, exc_info=True)
            return clean_text, []
        finally:
            if original_result is not None:
                self.set_result(original_result)
            else:
                self.clear_result()
        return " ".join(text_parts).strip() or clean_text, image_urls

    # ------------------------------------------------------------------ image handling

    async def _image_component_to_url(self, comp) -> str | None:
        path = getattr(comp, "path", None) or getattr(comp, "file", None)
        url = getattr(comp, "url", None)

        target = None
        if path:
            target = str(path)
        elif url:
            target = str(url)

        if not target:
            return None

        if target.startswith("file://"):
            target = target[7:]

        if target.lower().startswith(("http://", "https://")):
            is_local = (
                "localhost" in target.lower()
                or "127.0.0.1" in target.lower()
                or "0.0.0.0" in target.lower()
                or "/file/" in target.lower()
            )
            if is_local:
                try:
                    import os
                    import tempfile
                    import urllib.request
                    logger.info("[OttoHub] 下载本地服务器图片: %s", target)
                    req = urllib.request.Request(target, headers={"User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        data = resp.read()
                    suffix = ".png"
                    if "." in target.split("?")[0].split("/")[-1]:
                        suffix = "." + target.split("?")[0].split("/")[-1].split(".")[-1]
                    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                        tmp.write(data)
                        tmp_path = tmp.name
                    uploaded_url = await self.client.upload_image(tmp_path)
                    try:
                        os.unlink(tmp_path)
                    except Exception:
                        pass
                    return uploaded_url
                except Exception as exc:
                    logger.error("[OttoHub] 下载并上传本地图片失败 %s: %s", target, exc)
                    return None
            return target

        return await self.client.upload_image(target)

    @staticmethod
    def _is_meme_manager_image(comp) -> bool:
        path = str(getattr(comp, "path", None) or getattr(comp, "file", None) or "")
        normalized = path.replace("\\", "/").lower()
        return "astrbot_plugin_meme_manager" in normalized or "/meme_manager/" in normalized

    # ------------------------------------------------------------------ text splitting

    @staticmethod
    def _has_split_marker(text: str) -> bool:
        return bool(OTTOHUB_SPLIT_MARKER_RE.search(text or ""))

    @staticmethod
    def _content_size(text: str) -> int:
        return len(str(text or ""))

    @staticmethod
    def _take_prefix_by_size(text: str, max_size: int) -> str:
        return truncate_chars(text, max_size)

    @staticmethod
    def _split_text(text: str, max_len: int) -> list[str]:
        text = re.sub(r"\[图片\]\s*", "", text or "").strip()
        if not text:
            return []
        if OTTOHUB_SPLIT_MARKER_RE.search(text):
            parts: list[str] = []
            for part in OTTOHUB_SPLIT_MARKER_RE.split(text):
                part = part.strip()
                if part:
                    parts.extend(OttoHubMessageEvent._split_text(part, max_len))
            return parts

        # 分隔符优先级组：优先在段落/句子边界切分，避免在数字内部切分
        priority_groups = [
            ("\n\n", "\n"),
            ("。", "！", "？", "；", "……", "…", ".", "!", "?", ";"),
            ("，", "、", "：", ",", ":", " "),
        ]

        parts: list[str] = []
        remaining = text
        while OttoHubMessageEvent._content_size(remaining) > max_len:
            window = OttoHubMessageEvent._take_prefix_by_size(remaining, max_len)
            threshold = int(len(window) * 0.45)
            cut = -1
            for group in priority_groups:
                best_idx = -1
                best_sep = ""
                for sep in group:
                    idx = window.rfind(sep)
                    while idx != -1:
                        is_valid = True
                        if idx > 0 and idx + len(sep) < len(window):
                            char_before = window[idx - 1]
                            char_after = window[idx + len(sep)]
                            if char_before.isdigit() and char_after.isdigit():
                                if sep in (".", ",", ":", "，", "："):
                                    is_valid = False
                        if is_valid:
                            if idx > best_idx:
                                best_idx = idx
                                best_sep = sep
                            break
                        idx = window.rfind(sep, 0, idx)
                if best_idx >= threshold:
                    cut = best_idx + len(best_sep)
                    break
            if cut == -1:
                cut = len(window)
            parts.append(remaining[:cut].strip())
            remaining = remaining[cut:].strip()
        if remaining:
            parts.append(remaining)
        return parts

    @staticmethod
    def _resolve_message_limit(plugin_config: dict) -> int:
        """解析单条字符上限：自动分段时为每段上限，关闭分段时为截断上限。

        默认 350（给表情包预留空间），钳制到平台硬上限 [50, 400]。
        """
        try:
            limit = int(plugin_config.get("message_max_length", DEFAULT_MESSAGE_MAX_CHARS))
        except (TypeError, ValueError):
            limit = DEFAULT_MESSAGE_MAX_CHARS
        return min(max(limit, 50), OTTOHUB_PLATFORM_MAX_CHARS)

    @staticmethod
    def _truncate_to_limit(text: str, char_limit: int) -> tuple[str, bool]:
        """按字符数截断，尽量在句末标点处收尾。返回 (文本, 是否发生截断)。"""
        text = (text or "").strip()
        if len(text) <= char_limit:
            return text, False
        window = text[:char_limit]
        # 在窗口后段寻找句末标点，避免在句子中间硬切
        boundary = max((window.rfind(ch) for ch in "。！？；…\n.!?;"), default=-1)
        if boundary >= int(char_limit * 0.6):
            return window[: boundary + 1].rstrip(), True
        return window.rstrip(), True

    @staticmethod
    def _merge_image_parts(
        message_parts: list[str],
        image_urls: list[str],
        max_len: int,
        *,
        allow_standalone: bool = True,
    ) -> list[str]:
        parts = [part for part in message_parts if part.strip()]
        seen: set[str] = set()
        for url in image_urls[:4]:
            if not url or url in seen:
                continue
            seen.add(url)
            # 已嵌入正文（如 meme 插件直接在文本里插了 ![image](url)）的图片不再重复附加
            if any(url in part for part in parts):
                continue
            item = f"![image]({url})"
            if parts and OttoHubMessageEvent._content_size(parts[-1] + "\n" + item) <= max_len:
                parts[-1] = parts[-1] + "\n" + item
            elif allow_standalone and OttoHubMessageEvent._content_size(item) <= max_len:
                parts.append(item)
            elif allow_standalone:
                # 仅当允许独立成条、且图片链接本身就超过单条上限时才真正无法发送
                logger.warning("[OttoHub] 图片链接超过单条上限(%d 字)，无法发送: %s", max_len, url)
            else:
                # 表情包等装饰图：放不下当前段则跳过，不单独成条、不报警
                logger.debug("[OttoHub] 装饰图片无法内联到当前段，已跳过: %s", url)
        return parts

    # ------------------------------------------------------------------ decoration

    async def _decorate_single_text_part(self, part: str, max_len: int, *, detect_meme: bool = True) -> list[str]:
        if not detect_meme:
            split_parts = self._split_text(part, max_len)
            return self._merge_image_parts(split_parts, [], max_len, allow_standalone=False)
        detected_text = await self._detect_meme_for_text(part)
        decorated_text, decorated_images = await self._apply_meme_manager(detected_text)
        split_parts = self._split_text(decorated_text, max_len)
        # 表情包只内联，不单独成条
        return self._merge_image_parts(split_parts, decorated_images, max_len, allow_standalone=False)

    async def _decorate_text_parts(self, parts: list[str], max_len: int, detect_each_part: bool = True) -> list[str]:
        decorated_parts: list[str] = []
        for part in parts:
            decorated_parts.extend(await self._decorate_single_text_part(part, max_len, detect_meme=detect_each_part))
        return decorated_parts

    # ------------------------------------------------------------------ send

    async def _send_single_message(self, content: str) -> bool:
        if self.reply_type == "blog_comment":
            return await self.client.comment_blog(self.context_id, content, self.parent_id)
        if self.reply_type == "video_comment":
            return await self.client.comment_video(self.context_id, content, self.parent_id)
        uid = self.receiver_uid
        if uid is None:
            uid = int(getattr(getattr(self.message_obj, "sender", None), "user_id", 0) or 0)
        if uid:
            return await self.client.send_message(uid, content)
        return False

    def _load_plugin_config(self) -> dict:
        try:
            from .adapter import load_plugin_config
            return load_plugin_config()
        except Exception:
            pass
        return {}

    async def _dispatch_final_parts(self, parts: list[str]) -> int:
        """顺序发送已定稿的分段列表，失败时入补发队列。返回成功发送的段数。"""
        sent = 0
        for idx, part in enumerate(parts):
            if not part:
                continue
            ok = await self._send_single_message(part)
            if ok:
                sent += 1
            else:
                queued = self.client.enqueue_resend(
                    reply_type=self.reply_type,
                    context_id=self.context_id,
                    parent_id=self.parent_id,
                    receiver_uid=self.receiver_uid,
                    parts=parts[idx:],
                    msg_id=getattr(self.message_obj, "message_id", None),
                )
                if queued:
                    logger.warning("[OttoHub] 第 %d 段发送失败，剩余 %d 段已加入补发队列", idx, len(parts[idx:]))
                    return sent
                if sent == 0:
                    raise RuntimeError(f"OttoHub send failed at segment {idx}")
                return sent
            if idx < len(parts) - 1:
                await asyncio.sleep(OTTOHUB_SEGMENT_DELAY_SECONDS)
        return sent

    async def send(self, message: MessageChain):
        pending_meme_images = self.get_extra("meme_manager_pending_images") or []
        self.set_extra("meme_manager_pending_images", [])
        text_parts: list[str] = []
        image_urls: list[str] = []
        meme_image_urls: list[str] = []

        for comp in pending_meme_images:
            url = await self._image_component_to_url(comp)
            if url:
                meme_image_urls.append(url)

        comp_list = message.chain if hasattr(message, "chain") else message
        if not isinstance(comp_list, (list, tuple)):
            comp_list = [comp_list]
        for comp in comp_list:
            if isinstance(comp, str):
                text_parts.append(comp)
            elif isinstance(comp, Plain) or hasattr(comp, "text"):
                text_parts.append(str(getattr(comp, "text", "")))
            elif isinstance(comp, Image) or comp.__class__.__name__ == "Image" or hasattr(comp, "path") or hasattr(comp, "file"):
                if self._is_meme_manager_image(comp):
                    url = await self._image_component_to_url(comp)
                    if url:
                        meme_image_urls.append(url)
                    continue
                url = await self._image_component_to_url(comp)
                if url:
                    image_urls.append(url)
            elif isinstance(comp, At):
                text_parts.append(f"@{comp.qq} ")

        raw_text = self._sanitize_outgoing_text("".join(text_parts))
        
        plugin_config = self._load_plugin_config()
        if plugin_config.get("reply_with_at", False) and self.reply_type != "dm" and self.receiver_uid:
            nickname = ""
            if hasattr(self.message_obj, "sender") and self.message_obj.sender:
                nickname = getattr(self.message_obj.sender, "nickname", "") or ""
            if not nickname:
                nickname = "用户"
            
            if plugin_config.get("use_effective_at", True):
                at_prefix = f"@{nickname} "
            else:
                at_prefix = f"[@{nickname}](/u/{self.receiver_uid}) "
                
            if at_prefix.strip() not in raw_text:
                raw_text = at_prefix + raw_text

        # LLM 报错时不输出任何固定话术：直接放弃本次回复，并在开启重新响应时交由后台重试
        is_llm_error = raw_text.startswith("LLM 响应错误") or raw_text.startswith("LLM response error")
        if is_llm_error:
            logger.warning("[OttoHub] LLM 响应错误，放弃本次回复（不发送任何固定话术）: %s", raw_text[:300])
            msg_id = getattr(self.message_obj, "message_id", None)
            if (
                self.reply_type in ("blog_comment", "video_comment")
                and getattr(self.client, "resend_re_respond", False)
                and getattr(self.client, "adapter", None)
                and msg_id
            ):
                logger.warning("[OttoHub] LLM 错误已触发重新响应 msg_id=%s", msg_id)
                self.client.adapter.trigger_re_response(str(msg_id))
            return

        max_len = self._resolve_message_limit(plugin_config)
        has_explicit_split = self._has_split_marker(raw_text)
        parts = self._split_text(raw_text, max_len)

        # 判断消息是否为指令（指令不附加 meme/emoji）
        incoming_text = ""
        try:
            getter = getattr(self, "get_message_str", None)
            if callable(getter):
                incoming_text = str(getter() or "")
            else:
                incoming_text = str(getattr(self, "message_str", "") or "")
        except Exception:
            pass

        incoming_clean = re.sub(r'^(?:\[At:\d+\]|@\S+)\s*', '', incoming_text.strip(), flags=re.I).strip()
        is_command = (
            incoming_clean.startswith("/")
            or incoming_clean.startswith(".")
            or incoming_clean.startswith("!")
            or incoming_clean.lower() in {"reset", "new", "help", "stop"}
        )

        should_attach_meme = (len(parts) <= 10) and not is_command
        if not should_attach_meme:
            logger.info(
                "[OttoHub] 抑制 Meme/表情（is_command=%s，分段数=%d）",
                is_command, len(parts),
            )
            meme_image_urls = []

        detect_each_part = has_explicit_split or len(parts) > 1
        sent = 0

        # 关闭自动分段：截断为单条消息（按字符上限，附带图片仍可单独成条）
        if not bool(plugin_config.get("auto_split_segments", True)):
            body = raw_text
            if should_attach_meme:
                detected_text = await self._detect_meme_for_text(raw_text)
                decorated_text, decorated_images = await self._apply_meme_manager(detected_text)
                body = decorated_text or raw_text
                meme_image_urls = decorated_images or meme_image_urls
            truncated, did_truncate = self._truncate_to_limit(body, max_len)
            if did_truncate:
                logger.info(
                    "[OttoHub] 已关闭自动分段，输出超过 %d 字已截断（原 %d 字）",
                    max_len, len(body),
                )
            final_parts = [truncated] if truncated else []
            final_parts = self._merge_image_parts(
                final_parts, (meme_image_urls or []) + image_urls, max_len,
            )
            logger.info("[OttoHub] 准备发送单条回复（分段关闭）type=%s", self.reply_type)
            sent += await self._dispatch_final_parts(final_parts)
            if sent > 0:
                self.set_extra("meme_manager_pending_images", [])
                await super().send(message)
            return

        if detect_each_part:
            logger.info(
                "[OttoHub] 流式发送 %d 段回复 explicit_split=%s type=%s",
                len(parts), has_explicit_split, self.reply_type,
            )
            for base_idx, base_part in enumerate(parts):
                decorated_parts = await self._decorate_single_text_part(base_part, max_len, detect_meme=should_attach_meme)
                if base_idx == len(parts) - 1:
                    decorated_parts = self._merge_image_parts(decorated_parts, image_urls, max_len)
                for part_idx, part in enumerate(decorated_parts):
                    if not part:
                        continue
                    ok = await self._send_single_message(part)
                    if ok:
                        sent += 1
                    else:
                        queued = self.client.enqueue_resend(
                            reply_type=self.reply_type,
                            context_id=self.context_id,
                            parent_id=self.parent_id,
                            receiver_uid=self.receiver_uid,
                            parts=decorated_parts[part_idx:] + parts[base_idx + 1:],
                            msg_id=getattr(self.message_obj, "message_id", None),
                        )
                        if queued:
                            logger.warning(
                                "[OttoHub] 流式段 %d.%d 发送失败，剩余段已加入补发队列",
                                base_idx, part_idx,
                            )
                            break
                        if sent == 0:
                            raise RuntimeError(f"OttoHub send failed at streamed segment {base_idx}.{part_idx}")
                        break
                    if part_idx < len(decorated_parts) - 1 or base_idx < len(parts) - 1:
                        await asyncio.sleep(OTTOHUB_SEGMENT_DELAY_SECONDS)
                else:
                    continue
                break
        else:
            parts = await self._decorate_text_parts(
                parts, max_len,
                detect_each_part=(detect_each_part or not meme_image_urls) and should_attach_meme,
            )
            if should_attach_meme and not detect_each_part and meme_image_urls and not any("![image](" in part for part in parts):
                parts = self._merge_image_parts(parts, meme_image_urls, max_len, allow_standalone=False)
            parts = self._merge_image_parts(parts, image_urls, max_len)
            logger.info(
                "[OttoHub] 准备发送 %d 段回复 explicit_split=%s type=%s",
                len(parts), has_explicit_split, self.reply_type,
            )
            sent += await self._dispatch_final_parts(parts)

        if sent > 0:
            self.set_extra("meme_manager_pending_images", [])
            await super().send(message)
