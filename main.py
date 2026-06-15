from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.provider import ProviderRequest
from astrbot.api.star import Context, Star, register

from .text_utils import sanitize_text, strip_data_images


@register("Ottohub Adapter", "wcsndb", "OttoHub 适配器", "0.1.1")
class OttoHubPlugin(Star):
    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        self.config = config or {}

        # 强制级联依赖：关闭上游开关时，同步关闭依赖它的高级选项并持久化到 WebUI。
        # 配置可能是分组嵌套结构（WebUI 分组），故用 _cfg_get/_cfg_set 兼容扁平与嵌套两种布局。
        changed = False
        if not self._cfg_get("resend_failed_messages", False):
            for key in ("resend_re_respond", "resend_delete_on_audit"):
                if self._cfg_get(key, False):
                    self._cfg_set(key, False)
                    changed = True

        if not self._cfg_get("reply_with_at", False) and self._cfg_get("use_effective_at", True):
            self._cfg_set("use_effective_at", False)
            changed = True

        if changed:
            try:
                from astrbot.core.star.star import star_map
                metadata = star_map.get(self.__class__.__module__)
                if metadata and metadata.config:
                    metadata.config.save_config(self.config)
                    logger.info("[OttoHub] 级联开关触发，已关闭重新响应/自动删除/有效@等依赖选项并同步 WebUI")
            except Exception as exc:
                logger.warning("[OttoHub] 自动联动保存配置失败: %s", exc)

        from . import adapter
        adapter._SHARED_CONTEXT = context
        logger.debug("[OttoHub] 插件初始化完成，已共享 context")

    # ------------------------------------------------------------------ config helpers

    def _cfg_get(self, key, default=None):
        """读取配置项，兼容扁平键与 WebUI 分组嵌套布局。"""
        if key in self.config:
            return self.config.get(key, default)
        for value in self.config.values():
            if isinstance(value, dict) and key in value:
                return value.get(key, default)
        return default

    def _cfg_set(self, key, new_value):
        """写入配置项，命中分组内的键时就地更新，否则写到顶层。"""
        if key in self.config:
            self.config[key] = new_value
            return
        for value in self.config.values():
            if isinstance(value, dict) and key in value:
                value[key] = new_value
                return
        self.config[key] = new_value

    # ------------------------------------------------------------------ LLM 请求清洗

    def _sanitize_llm_value(self, value, *, remove_images: bool = False):
        if isinstance(value, str):
            if remove_images:
                value = strip_data_images(value)
            return sanitize_text(value)
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
                            item = strip_data_images(item)
                        setattr(value, attr, sanitize_text(item))
                except Exception:
                    pass
        return value

    def _sanitize_llm_request(self, req: ProviderRequest) -> None:
        req.prompt = self._sanitize_llm_value(req.prompt)
        req.system_prompt = self._sanitize_llm_value(req.system_prompt)
        req.contexts = self._sanitize_llm_value(req.contexts or [], remove_images=True)
        req.extra_user_content_parts = self._sanitize_llm_value(req.extra_user_content_parts or [], remove_images=False)

    # ------------------------------------------------------------------ LLM hook

    @filter.on_llm_request()
    async def apply_ottohub_llm_prompt(self, event: AstrMessageEvent, req: ProviderRequest):
        if event.__class__.__name__ != "OttoHubMessageEvent":
            return

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

        self._sanitize_llm_request(req)

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
