import asyncio
import hashlib
import json
import logging
import os
import time
import urllib.parse
from typing import Any

import aiohttp

logger = logging.getLogger("astrbot")


class OttoHubClient:
    def __init__(
        self,
        cookies: dict[str, str],
        user_agent: str = "",
        image_upload_validate: bool = True,
        image_upload_attempts: int = 3,
        image_upload_check_delay: float = 5.0,
        image_upload_retry_delay: float = 5.0,
        resend_failed_messages: bool = False,
        resend_attempts: int = 3,
        resend_delay_seconds: float = 15.0,
        resend_dedupe_ttl_seconds: float = 3600.0,
    ):
        self.cookies = cookies
        self.user_agent = user_agent
        self.api_base = "https://api.ottohub.cn/"
        self.site_base = "https://www.ottohub.cn"
        raw_token = cookies.get("otto_token", "")
        self.token = urllib.parse.unquote(raw_token) if raw_token else None
        self.uid = cookies.get("otto_uid")
        self.image_upload_validate = bool(image_upload_validate)
        self.image_upload_attempts = self._clamp_int(image_upload_attempts, 3, 1, 10)
        self.image_upload_check_delay = self._clamp_float(image_upload_check_delay, 5.0, 0.0, 30.0)
        self.image_upload_retry_delay = self._clamp_float(image_upload_retry_delay, 5.0, 0.0, 30.0)
        self.resend_failed_messages = bool(resend_failed_messages)
        self.resend_attempts = self._clamp_int(resend_attempts, 3, 1, 10)
        self.resend_delay_seconds = self._clamp_float(resend_delay_seconds, 15.0, 1.0, 300.0)
        self.resend_dedupe_ttl_seconds = self._clamp_float(resend_dedupe_ttl_seconds, 3600.0, 60.0, 86400.0)
        self.session: aiohttp.ClientSession | None = None
        self._write_lock = asyncio.Lock()
        self._resend_keys: dict[str, float] = {}
        self._resend_tasks: set[asyncio.Task] = set()

    # ------------------------------------------------------------------ session

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            headers = {"User-Agent": self.user_agent} if self.user_agent else {}
            self.session = aiohttp.ClientSession(cookies=self.cookies, headers=headers)
        return self.session

    async def close(self) -> None:
        if self.session:
            await self.session.close()

    # ------------------------------------------------------------------ utilities

    @staticmethod
    def _safe_params(params: dict[str, Any]) -> dict[str, Any]:
        return {k: ("***" if k == "token" else v) for k, v in params.items()}

    @staticmethod
    def _limit_utf8(text: str, max_bytes: int = 380) -> str:
        total = 0
        chars = []
        for char in str(text or ""):
            char_size = len(char.encode("utf-8"))
            if chars and total + char_size > max_bytes:
                break
            chars.append(char)
            total += char_size
            if total >= max_bytes:
                break
        return "".join(chars)

    @staticmethod
    def _clamp_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            number = int(value)
        except Exception:
            number = default
        return min(max(number, minimum), maximum)

    @staticmethod
    def _clamp_float(value: Any, default: float, minimum: float, maximum: float) -> float:
        try:
            number = float(value)
        except Exception:
            number = default
        return min(max(number, minimum), maximum)

    # ------------------------------------------------------------------ HTTP

    async def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        session = await self._get_session()
        request_params = dict(params)
        request_params["offset"] = request_params.get("offset", 0)
        if self.token:
            request_params["token"] = self.token
        try:
            async with session.request(method, self.api_base, params=request_params) as resp:
                text = await resp.text()
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    logger.warning("[OttoHub API] 响应非 JSON，status=%s", resp.status)
                    return {"status": "error", "message": "bad_json"}
                if data.get("status") == "error":
                    logger.warning(
                        "[OttoHub API] 请求失败 message=%s params=%s",
                        data.get("message"),
                        self._safe_params(request_params),
                    )
                return data
        except Exception as exc:
            logger.warning("[OttoHub API] 请求异常: %s", exc)
            return {"status": "error", "message": "request_failed"}

    async def _site_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        session = await self._get_session()
        request_params = dict(params or {})
        if self.token and "token" not in request_params:
            request_params["token"] = self.token
        try:
            async with session.get(self.site_base + path, params=request_params) as resp:
                text = await resp.text()
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return {"status": "error", "message": "bad_json"}
        except Exception as exc:
            logger.warning("[OttoHub site API] 请求异常: %s", exc)
            return {"status": "error", "message": "request_failed"}

    @staticmethod
    def _write_ok(response: dict[str, Any], action: str) -> bool:
        status = str(response.get("status") or "").lower()
        message = str(response.get("message") or response.get("msg") or response.get("error") or "")
        payload = json.dumps(response, ensure_ascii=False)[:800]
        blocked_markers = (
            "敏感", "违禁", "违规", "审核", "拦截", "屏蔽", "禁止", "不允许",
            "content", "forbidden", "ban", "blocked",
        )
        if status != "success":
            logger.warning("[OttoHub API] %s 失败 status=%s message=%s response=%s", action, status, message, payload)
            return False
        if any(marker in message.lower() for marker in blocked_markers):
            logger.warning("[OttoHub API] %s 被拦截 message=%s response=%s", action, message, payload)
            return False
        return True

    # ------------------------------------------------------------------ messaging API

    async def verify_session(self) -> bool:
        response = await self._request("GET", {"module": "im", "action": "unread_message_list", "num": 1})
        return response.get("status") == "success"

    async def get_unread_messages(self, num: int = 10) -> list[dict[str, Any]]:
        response = await self._request(
            "GET", {"module": "im", "action": "unread_message_list", "num": min(int(num or 10), 12)}
        )
        return response.get("unread_message_list", []) or []

    async def mark_message_read(self, msg_id: int) -> bool:
        response = await self._request("GET", {"module": "im", "action": "read_message", "msg_id": msg_id})
        return response.get("status") == "success"

    async def send_message(self, receiver: int, content: str) -> bool:
        async with self._write_lock:
            response = await self._request(
                "POST", {"module": "im", "action": "send_message", "receiver": receiver, "message": self._limit_utf8(content)}
            )
            return self._write_ok(response, "send_message")

    async def comment_blog(self, bid: int, content: str, parent_bcid: int = 0) -> bool:
        async with self._write_lock:
            logger.info("[OttoHub] 评论动态 bid=%s parent_bcid=%s", bid, parent_bcid)
            response = await self._request(
                "POST",
                {
                    "module": "comment",
                    "action": "comment_blog",
                    "bid": bid,
                    "parent_bcid": int(parent_bcid or 0),
                    "content": self._limit_utf8(content),
                },
            )
            return self._write_ok(response, "comment_blog")

    async def comment_video(self, vid: int, content: str, parent_vcid: int = 0) -> bool:
        async with self._write_lock:
            logger.info("[OttoHub] 评论视频 vid=%s parent_vcid=%s", vid, parent_vcid)
            response = await self._request(
                "POST",
                {
                    "module": "comment",
                    "action": "comment_video",
                    "vid": vid,
                    "parent_vcid": int(parent_vcid or 0),
                    "content": self._limit_utf8(content),
                },
            )
            return self._write_ok(response, "comment_video")

    # ------------------------------------------------------------------ comment/post queries

    async def get_blog_detail(self, bid: int) -> dict[str, Any]:
        return await self._request("GET", {"module": "blog", "action": "get_blog_detail", "bid": bid})

    async def get_video_detail(self, vid: int) -> dict[str, Any]:
        return await self._request("GET", {"module": "video", "action": "get_video_detail", "vid": vid})

    async def get_blog_comments(
        self, bid: int, num: int = 12, offset: int = 0, parent_bcid: int = 0, cid_asc: int = 0
    ) -> list[dict[str, Any]]:
        response = await self._request(
            "GET",
            {
                "module": "comment",
                "action": "blog_comment_list",
                "bid": bid,
                "parent_bcid": int(parent_bcid or 0),
                "offset": max(0, int(offset or 0)),
                "num": min(max(1, int(num or 12)), 12),
                "cid_asc": int(cid_asc),
            },
        )
        return response.get("comment_list", []) or []

    async def get_video_comments(
        self, vid: int, num: int = 12, offset: int = 0, parent_vcid: int = 0, cid_asc: int = 0
    ) -> list[dict[str, Any]]:
        response = await self._request(
            "GET",
            {
                "module": "comment",
                "action": "video_comment_list",
                "vid": vid,
                "parent_vcid": int(parent_vcid or 0),
                "offset": max(0, int(offset or 0)),
                "num": min(max(1, int(num or 12)), 12),
                "cid_asc": int(cid_asc),
            },
        )
        return response.get("comment_list", []) or []

    async def get_bot_relationship(self, uid: str) -> dict[str, Any]:
        if not uid or not self.token:
            return {"is_fan": False, "follow_status": 0}
        response = await self._site_json(f"/api/following/status/{uid}")
        status = int(response.get("follow_status") or 0)
        return {
            "is_fan": status in {3, 4},
            "is_following": status in {2, 4},
            "is_mutual": status == 4,
            "follow_status": status,
        }

    # ------------------------------------------------------------------ resend

    def _resend_key(
        self,
        reply_type: str,
        context_id: int,
        parent_id: int,
        receiver_uid: int | None,
        parts: list[str],
    ) -> str:
        payload = json.dumps(
            {
                "reply_type": reply_type,
                "context_id": int(context_id or 0),
                "parent_id": int(parent_id or 0),
                "receiver_uid": int(receiver_uid or 0),
                "parts": parts,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _prune_resend_keys(self) -> None:
        now = time.time()
        for key, created_at in list(self._resend_keys.items()):
            if now - created_at > self.resend_dedupe_ttl_seconds:
                self._resend_keys.pop(key, None)

    def enqueue_resend(
        self,
        *,
        reply_type: str,
        context_id: int = 0,
        parent_id: int = 0,
        receiver_uid: int | None = None,
        parts: list[str],
    ) -> bool:
        if not self.resend_failed_messages:
            return False
        clean_parts = [str(part or "").strip() for part in parts if str(part or "").strip()]
        if not clean_parts:
            return False
        self._prune_resend_keys()
        key = self._resend_key(reply_type, context_id, parent_id, receiver_uid, clean_parts)
        if key in self._resend_keys:
            logger.info("[OttoHub] 跳过重复补发任务 key=%.12s", key)
            return True
        self._resend_keys[key] = time.time()
        task = asyncio.create_task(
            self._resend_worker(key, reply_type, int(context_id or 0), int(parent_id or 0), receiver_uid, clean_parts)
        )
        self._resend_tasks.add(task)
        task.add_done_callback(self._resend_tasks.discard)
        logger.warning(
            "[OttoHub] 已加入补发队列 key=%.12s type=%s 段数=%d",
            key, reply_type, len(clean_parts),
        )
        return True

    async def _send_resend_part(
        self,
        reply_type: str,
        context_id: int,
        parent_id: int,
        receiver_uid: int | None,
        content: str,
    ) -> bool:
        if reply_type == "blog_comment":
            return await self.comment_blog(context_id, content, parent_id)
        if reply_type == "video_comment":
            return await self.comment_video(context_id, content, parent_id)
        if receiver_uid:
            return await self.send_message(int(receiver_uid), content)
        return False

    async def _resend_worker(
        self,
        key: str,
        reply_type: str,
        context_id: int,
        parent_id: int,
        receiver_uid: int | None,
        parts: list[str],
    ) -> None:
        await asyncio.sleep(self.resend_delay_seconds)
        remaining_parts = list(parts)
        for attempt in range(1, self.resend_attempts + 1):
            failed_at = -1
            for idx, part in enumerate(remaining_parts):
                ok = await self._send_resend_part(reply_type, context_id, parent_id, receiver_uid, part)
                if not ok:
                    failed_at = idx
                    break
                if idx < len(remaining_parts) - 1:
                    await asyncio.sleep(5.0)
            if failed_at < 0:
                logger.info("[OttoHub] 补发成功 key=%.12s attempt=%d", key, attempt)
                return
            remaining_parts = remaining_parts[failed_at:]
            logger.warning(
                "[OttoHub] 补发失败 key=%.12s attempt=%d/%d failed_at=%d",
                key, attempt, self.resend_attempts, failed_at,
            )
            if attempt < self.resend_attempts:
                await asyncio.sleep(self.resend_delay_seconds)
        logger.error("[OttoHub] 补发已耗尽重试次数 key=%.12s attempts=%d", key, self.resend_attempts)

    # ------------------------------------------------------------------ image upload

    def _load_image_cache(self) -> dict[str, dict[str, str]]:
        cache_path = "data/config/astrbot_plugin_ottohub_image_cache.json"
        if not os.path.exists(cache_path):
            return {}
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except Exception as exc:
            logger.warning("[OttoHub] 加载图片缓存失败: %s", exc)
            return {}

    def _save_image_cache(self, cache: dict[str, dict[str, str]]) -> None:
        cache_path = "data/config/astrbot_plugin_ottohub_image_cache.json"
        try:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            logger.warning("[OttoHub] 保存图片缓存失败: %s", exc)

    def _image_as_jpeg_payload(self, path: str) -> tuple[bytes, str, str]:
        """将图片编码为 JPEG，返回 (bytes, filename, content_type)。"""
        import io
        from pathlib import Path

        from PIL import Image as PILImage, ImageOps

        file_path = Path(path)
        with PILImage.open(file_path) as img:
            img = ImageOps.exif_transpose(img)
            img.seek(0)

            aspect_ratio = max(img.size) / max(1, min(img.size))
            max_dim = 3000 if aspect_ratio >= 3 else 4096
            if max(img.size) > max_dim:
                ratio = max_dim / float(max(img.size))
                new_size = (max(1, int(img.size[0] * ratio)), max(1, int(img.size[1] * ratio)))
                img = img.resize(new_size, PILImage.Resampling.LANCZOS)

            if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
                rgba = img.convert("RGBA")
                background = PILImage.new("RGB", rgba.size, (255, 255, 255))
                background.paste(rgba, mask=rgba.split()[3])
                img = background
            else:
                img = img.convert("RGB")

            out_io = io.BytesIO()
            img.save(out_io, format="JPEG", quality=82, optimize=True, progressive=True)

        filename = f"{file_path.stem or 'image'}.jpg"
        return out_io.getvalue(), filename, "image/jpeg"

    def _image_upload_payload(self, path: str) -> tuple[bytes, str, str]:
        """构建上传 payload，GIF/SVG 等特殊格式保持原样，其余转为 JPEG。"""
        import mimetypes
        from pathlib import Path

        from PIL import Image as PILImage

        file_path = Path(path)
        suffix = file_path.suffix.lower()
        special_exts = {".gif", ".svg", ".svgz"}
        if suffix in special_exts:
            content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
            return file_path.read_bytes(), file_path.name or "image", content_type

        try:
            with PILImage.open(file_path) as img:
                if bool(getattr(img, "is_animated", False)) or int(getattr(img, "n_frames", 1) or 1) > 1:
                    content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
                    return file_path.read_bytes(), file_path.name or "image", content_type
        except Exception:
            raise

        return self._image_as_jpeg_payload(path)

    async def upload_image(self, file_path: str) -> str | None:
        path = str(file_path or "")
        if path.startswith("file://"):
            path = path[7:]
        if not path or path.lower().startswith(("http://", "https://")):
            if path and (not self.image_upload_validate or await self.validate_image_url(path)):
                return path
            logger.warning("[OttoHub] 远程图片不可达，已丢弃: %s", path)
            return None
        if not os.path.exists(path):
            logger.warning("[OttoHub] 图片文件不存在: %s", path)
            return None

        try:
            upload_payload = self._image_upload_payload(path)
            file_hash = hashlib.md5(upload_payload[0]).hexdigest()
        except Exception as exc:
            logger.warning("[OttoHub] 图片上传预处理失败，已丢弃: %s", exc)
            return None

        cache = {}
        if file_hash:
            cache = self._load_image_cache()
            cached_entry = cache.get(file_hash, {})
            oh_url = cached_entry.get("oh")
            if oh_url:
                if not self.image_upload_validate or await self.validate_image_url(oh_url):
                    logger.info("[OttoHub] 复用缓存图片 hash=%.10s url=%s", file_hash, oh_url)
                    return oh_url
                else:
                    logger.warning("[OttoHub] 缓存图片已失效: %s", oh_url)
                    cached_entry.pop("oh", None)
                    cache[file_hash] = cached_entry
                    self._save_image_cache(cache)

        for attempt in range(1, self.image_upload_attempts + 1):
            url = await self._upload_image_once(upload_payload)
            if not url:
                if attempt < self.image_upload_attempts and self.image_upload_retry_delay > 0:
                    await asyncio.sleep(self.image_upload_retry_delay)
                continue
            if not self.image_upload_validate:
                if file_hash:
                    cache = self._load_image_cache()
                    entry = cache.get(file_hash, {})
                    entry["oh"] = url
                    cache[file_hash] = entry
                    self._save_image_cache(cache)
                return url
            if self.image_upload_check_delay > 0:
                await asyncio.sleep(self.image_upload_check_delay)
            if await self.validate_image_url(url):
                if attempt > 1:
                    logger.info("[OttoHub] 图片上传在第 %d 次成功", attempt)
                if file_hash:
                    cache = self._load_image_cache()
                    entry = cache.get(file_hash, {})
                    entry["oh"] = url
                    cache[file_hash] = entry
                    self._save_image_cache(cache)
                return url
            logger.warning("[OttoHub] 上传图片第 %d 次后仍不可达: %s", attempt, url)
            if attempt < self.image_upload_attempts and self.image_upload_retry_delay > 0:
                await asyncio.sleep(self.image_upload_retry_delay)

        logger.warning("[OttoHub] 图片上传失败，已达最大重试次数 (%d): %s", self.image_upload_attempts, path)
        return None

    async def _upload_image_once(self, upload_payload: tuple[bytes, str, str]) -> str | None:
        try:
            upload_bytes, filename, content_type = upload_payload
            form = aiohttp.FormData()
            form.add_field("action", "submit_image")
            if self.token:
                form.add_field("token", self.token)
            form.add_field("file_img", upload_bytes, filename=filename, content_type=content_type)
            session = await self._get_session()
            async with session.post(
                self.api_base + "module/creator/submit_image.php",
                data=form,
                headers={"Origin": self.site_base, "Referer": self.site_base + "/creator/blog/new"},
            ) as resp:
                text = await resp.text()
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    logger.warning("[OttoHub] 图片上传响应非 JSON status=%s", resp.status)
                    return None
                if data.get("status") == "error":
                    logger.warning("[OttoHub] 图片上传失败: %s", data.get("message"))
                    return None
                return data.get("image_url") or data.get("url")
        except Exception as exc:
            logger.warning("[OttoHub] 图片上传异常: %s", exc)
            return None

    async def validate_image_url(self, url: str) -> bool:
        try:
            session = await self._get_session()
            async with session.head(url, timeout=8, allow_redirects=True) as resp:
                if resp.status == 405:
                    raise RuntimeError("HEAD not allowed")
                return resp.status < 400
        except Exception:
            try:
                session = await self._get_session()
                headers = {"Range": "bytes=0-0"}
                async with session.get(url, timeout=8, headers=headers, allow_redirects=True) as resp:
                    return resp.status < 400
            except Exception:
                return False
