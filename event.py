import asyncio
import logging
import re

from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import At, Image, Plain
from astrbot.api.platform import AstrBotMessage, PlatformMetadata
from astrbot.api.provider import LLMResponse
from astrbot.api.star import Context

from .client import OttoHubClient

logger = logging.getLogger("astrbot")

SAFE_OTTOHUB_MESSAGE_LEN = 360
SAFE_OTTOHUB_COMMENT_LEN = 360
OTTOHUB_SEGMENT_DELAY_SECONDS = 5.0
OTTOHUB_SPLIT_MARKER_RE = re.compile(
    r"(?im)(?:[ \t\r\n]*(?:<SPLIT>|<ottohub:split>|<OTTOHUB_SPLIT>|\[SPLIT\]|\[OTTOHUB_SPLIT\]|---SPLIT---|---OTTOHUB_SPLIT---)[ \t\r\n]*|^[ \t]*(?:SPLIT|OTTOHUB_SPLIT)[ \t]*$)"
)
IMAGE_PLACEHOLDER_RE = re.compile(r"(?<![!\[])[ \t]*\[(?:图片|image)\][ \t]*(?!\()", re.I)
TOOL_CALL_BLOCK_RE = re.compile(r"(?is)\b[a-zA-Z_][\w.:-]*\s*\n(?:\s*<arg_key>.*?</arg_key>\s*(?:\n\s*<arg_value>.*?</arg_value>\s*)?)+\s*(?:</tool_call>)?")
TOOL_CALL_TAG_RE = re.compile(r"(?is)</?tool_call>|<arg_key>.*?</arg_key>|<arg_value>.*?</arg_value>")


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
    def _strip_image_placeholders(text: str) -> str:
        cleaned = IMAGE_PLACEHOLDER_RE.sub(" ", str(text or ""))
        return re.sub(r"[ \t]{2,}", " ", cleaned).strip()

    @classmethod
    def _sanitize_outgoing_text(cls, text: str) -> str:
        cleaned = TOOL_CALL_BLOCK_RE.sub(" ", str(text or ""))
        cleaned = TOOL_CALL_TAG_RE.sub(" ", cleaned)
        cleaned = cls._strip_image_placeholders(cleaned)
        cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
        cleaned = re.sub(r"\n[ \t]+", "\n", cleaned)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

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
        return len(str(text or "").encode("utf-8"))

    @staticmethod
    def _take_prefix_by_size(text: str, max_size: int) -> str:
        total = 0
        chars = []
        for char in str(text or ""):
            char_size = len(char.encode("utf-8"))
            if chars and total + char_size > max_size:
                break
            chars.append(char)
            total += char_size
            if total >= max_size:
                break
        return "".join(chars)

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
    def _merge_image_parts(
        message_parts: list[str],
        image_urls: list[str],
        max_len: int,
        *,
        allow_standalone: bool = True,
    ) -> list[str]:
        parts = [part for part in message_parts if part.strip()]
        dropped = 0
        for url in image_urls[:4]:
            item = f"![image]({url})"
            if parts and OttoHubMessageEvent._content_size(parts[-1] + "\n" + item) <= max_len:
                parts[-1] = parts[-1] + "\n" + item
            elif allow_standalone and OttoHubMessageEvent._content_size(item) <= max_len:
                parts.append(item)
            else:
                dropped += 1
        if dropped:
            logger.info("[OttoHub] %d 张图片因超长无法合并，已丢弃", dropped)
        return parts

    # ------------------------------------------------------------------ decoration

    async def _decorate_single_text_part(self, part: str, max_len: int, *, detect_meme: bool = True) -> list[str]:
        if not detect_meme:
            split_parts = self._split_text(part, max_len)
            return self._merge_image_parts(split_parts, [], max_len, allow_standalone=False)
        detected_text = await self._detect_meme_for_text(part)
        decorated_text, decorated_images = await self._apply_meme_manager(detected_text)
        split_parts = self._split_text(decorated_text, max_len)
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
        is_llm_error = raw_text.startswith("LLM 响应错误") or raw_text.startswith("LLM response error")
        if is_llm_error:
            logger.warning("[OttoHub] 抑制原始 LLM 错误回复: %s", raw_text[:300])
            raw_text = "抱歉，这次图片或模型处理失败了，我先不乱回复。"
            image_urls = []

        max_len = SAFE_OTTOHUB_COMMENT_LEN if "comment" in self.reply_type else SAFE_OTTOHUB_MESSAGE_LEN
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

        if not is_llm_error and detect_each_part:
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
            if not is_llm_error:
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
                    )
                    if queued:
                        logger.warning("[OttoHub] 第 %d 段发送失败，剩余 %d 段已加入补发队列", idx, len(parts[idx:]))
                        break
                    if sent == 0:
                        raise RuntimeError(f"OttoHub send failed at segment {idx}")
                    break
                if idx < len(parts) - 1:
                    await asyncio.sleep(OTTOHUB_SEGMENT_DELAY_SECONDS)

        if sent > 0:
            self.set_extra("meme_manager_pending_images", [])
            await super().send(message)
