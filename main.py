import re

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.provider import ProviderRequest
from astrbot.api.star import Context, Star, register
from .adapter import PLUGIN_CONFIG_PATH

DEFAULT_OTTOHUB_REPLY_LIMIT_PROMPT = ""
IMAGE_PLACEHOLDER_RE = re.compile(r"(?<![!\[])[ \t]*\[(?:图片|image)\][ \t]*(?!\()", re.I)
TOOL_CALL_BLOCK_RE = re.compile(r"(?is)\b[a-zA-Z_][\w.:-]*\s*\n(?:\s*<arg_key>.*?</arg_key>\s*(?:\n\s*<arg_value>.*?</arg_value>\s*)?)+\s*(?:</tool_call>)?")
TOOL_CALL_TAG_RE = re.compile(r"(?is)</?tool_call>|<arg_key>.*?</arg_key>|<arg_value>.*?</arg_value>")


@register("Ottohub Adapter", "wcsndb", "OttoHub 适配器", "0.1.1")
class OttoHubPlugin(Star):
    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        self.config = config or {}
        from . import adapter
        adapter._SHARED_CONTEXT = context
        logger.debug("[OttoHub] 插件初始化完成，已共享 context")

    # ------------------------------------------------------------------ config helpers

    def _load_plugin_config(self) -> dict:
        try:
            with PLUGIN_CONFIG_PATH.open(encoding="utf-8-sig") as f:
                import json
                return json.load(f)
        except Exception:
            return {}

    def _config_text(self, key: str, default: str = "") -> str:
        plugin_config = self._load_plugin_config()
        value = plugin_config.get(key, self.config.get(key, default))
        return str(value).strip() if value is not None else ""

    # ------------------------------------------------------------------ text sanitization

    @staticmethod
    def _strip_image_placeholders(text: str) -> str:
        cleaned = IMAGE_PLACEHOLDER_RE.sub(" ", str(text or ""))
        return re.sub(r"[ \t]{2,}", " ", cleaned).strip()

    @classmethod
    def _sanitize_text(cls, text: str) -> str:
        cleaned = TOOL_CALL_BLOCK_RE.sub(" ", str(text or ""))
        cleaned = TOOL_CALL_TAG_RE.sub(" ", cleaned)
        cleaned = cls._strip_image_placeholders(cleaned)
        cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
        cleaned = re.sub(r"\n[ \t]+", "\n", cleaned)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

    def _sanitize_llm_value(self, value, *, remove_images: bool = False):
        if isinstance(value, str):
            if remove_images:
                value = re.sub(r"data:image/[^;]+;base64,[A-Za-z0-9+/=]+", "", value)
            return self._sanitize_text(value)
        if isinstance(value, list):
            cleaned = []
            for item in value:
                item = self._sanitize_llm_value(item, remove_images=remove_images)
                if item is None:
                    continue
                cleaned.append(item)
            return cleaned
        if isinstance(value, dict):
            item_type = str(value.get("type") or "")
            if remove_images and (item_type in {"image_url", "input_image"} or "image_url" in value):
                return None
            for key, item in list(value.items()):
                value[key] = self._sanitize_llm_value(item, remove_images=remove_images)
            return value
        for attr in ("text", "content"):
            if hasattr(value, attr):
                try:
                    item = getattr(value, attr)
                    if isinstance(item, str):
                        if remove_images:
                            item = re.sub(r"data:image/[^;]+;base64,[A-Za-z0-9+/=]+", "", item)
                        setattr(value, attr, self._sanitize_text(item))
                except Exception:
                    pass
        return value

    # ------------------------------------------------------------------ OCR garbage cleanup

    def _clean_external_ocr_garbage(self, req: ProviderRequest) -> None:
        if req.extra_user_content_parts:
            cleaned_parts = []
            for part in req.extra_user_content_parts:
                text_val = ""
                if hasattr(part, "text"):
                    text_val = getattr(part, "text") or ""
                elif isinstance(part, dict) and "text" in part:
                    text_val = part["text"] or ""
                if isinstance(text_val, str) and ("Image Attachment" in text_val):
                    continue
                cleaned_parts.append(part)
            req.extra_user_content_parts = cleaned_parts

        if req.contexts:
            cleaned_contexts = []
            for msg in req.contexts:
                if not isinstance(msg, dict):
                    cleaned_contexts.append(msg)
                    continue
                content = msg.get("content")
                if isinstance(content, list):
                    cleaned_content = []
                    for part in content:
                        if isinstance(part, dict):
                            text_val = part.get("text") or ""
                            if part.get("type") == "text" and isinstance(text_val, str) and "Image Attachment" in text_val:
                                continue
                        cleaned_content.append(part)
                    msg["content"] = cleaned_content
                elif isinstance(content, str):
                    msg["content"] = re.sub(r"\[Image Attachment.*?: path [^\]]+\]", "", content)
                cleaned_contexts.append(msg)
            req.contexts = cleaned_contexts

        req.image_urls = []

    def _sanitize_llm_request(self, req: ProviderRequest) -> None:
        req.prompt = self._sanitize_llm_value(req.prompt)
        req.system_prompt = self._sanitize_llm_value(req.system_prompt)
        req.contexts = self._sanitize_llm_value(req.contexts or [], remove_images=True)
        req.extra_user_content_parts = self._sanitize_llm_value(req.extra_user_content_parts or [], remove_images=False)

    # ------------------------------------------------------------------ LLM hook

    @filter.on_llm_request()
    async def apply_ottohub_llm_prompt(self, event: AstrMessageEvent, req: ProviderRequest):
        if event.__class__.__name__ == "OttoHubMessageEvent":
            handlers = event.get_extra("activated_handlers") or []
            handler_names = [
                f"{getattr(h, 'handler_module_path', '')}:{getattr(h, 'handler_name', '')}"
                for h in handlers
                if "astrbot_plugin_ottohub" not in str(getattr(h, "handler_module_path", ""))
            ]
            logger.info(
                "[OttoHub] LLM 阶段 text=%r activated_handlers=%s",
                event.get_message_str(),
                handler_names,
            )

        user_prompt = event.get_extra("_ottohub_user_prompt")
        if isinstance(user_prompt, str) and user_prompt.strip():
            req.prompt = user_prompt.strip()

        context_prompt = event.get_extra("_ottohub_context_prompt")
        if isinstance(context_prompt, str) and context_prompt.strip():
            self._append_system_prompt(req, context_prompt)

        legacy_prompt = event.get_extra("_ottohub_llm_prompt")
        if (
            isinstance(legacy_prompt, str)
            and legacy_prompt.strip()
            and legacy_prompt.strip() != str(user_prompt or "").strip()
            and legacy_prompt.strip() != str(context_prompt or "").strip()
        ):
            self._append_system_prompt(req, legacy_prompt)

        if event.__class__.__name__ == "OttoHubMessageEvent":
            self._append_system_prompt(req, self._config_text("system_prompt", ""))
            self._append_system_prompt(req, self._config_text("reply_limit_prompt", DEFAULT_OTTOHUB_REPLY_LIMIT_PROMPT))
            self._sanitize_llm_request(req)
            use_external_ocr = self._config_text("use_external_ocr", "false").lower() in ("true", "1", "yes", "on")
            if use_external_ocr:
                self._clean_external_ocr_garbage(req)

    # ------------------------------------------------------------------ system prompt helper

    @staticmethod
    def _append_system_prompt(req: ProviderRequest, extra_prompt: str):
        extra_prompt = str(extra_prompt or "").strip()
        if not extra_prompt:
            return
        current = str(getattr(req, "system_prompt", "") or "")
        if extra_prompt in current:
            return
        req.system_prompt = (current + "\n\n" + extra_prompt).strip()
