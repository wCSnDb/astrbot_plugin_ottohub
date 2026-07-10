import asyncio
import hashlib
import json
import logging
import os
import time
import urllib.parse
from typing import Any

import aiohttp

from . import image_hosts
from .text_utils import truncate_chars

logger = logging.getLogger("astrbot")

# 网络超时与瞬时错误重试
DEFAULT_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
UPLOAD_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=120, connect=10)
TRANSIENT_REQUEST_ATTEMPTS = 3
TRANSIENT_RETRY_BACKOFF = 1.5

# OttoHub 单条消息/评论的平台字符硬上限（最后安全网，防止超限被服务端拒绝）
OTTOHUB_PLATFORM_MAX_CHARS = 400

# 两次写操作（评论/私信）之间的最小间隔的默认值：主回复和工具/插件的主动发送
# （如收款码、独立表情包）经常在几十到几百毫秒内先后触发，紧挨着连续调用 OttoHub
# 写接口会被服务端判定为 too_many_requests 而直接拒绝。可在插件配置里调整。
DEFAULT_MIN_WRITE_INTERVAL_SECONDS = 3.0


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
        resend_re_respond: bool = False,
        resend_delete_on_audit: bool = False,
        resend_max_attempts: int = 2,
        resend_delay_seconds: float = 15.0,
        comment_retrieval_limit: int = 24,
        use_third_party_image_host: bool = False,
        fallback_mode: bool = True,
        r2_account_id: str = "",
        r2_access_key_id: str = "",
        r2_secret_access_key: str = "",
        r2_bucket_name: str = "",
        r2_public_url: str = "",
        stardots_key: str = "",
        stardots_secret: str = "",
        stardots_space: str = "",
        beeimg_token: str = "",
        beeimg_strategy_id: str = "",
        fallback_max_kb: int = 800,
        min_write_interval_seconds: float = DEFAULT_MIN_WRITE_INTERVAL_SECONDS,
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
        self.resend_re_respond = bool(resend_re_respond) if self.resend_failed_messages else False
        self.resend_delete_on_audit = bool(resend_delete_on_audit) if self.resend_failed_messages else False
        self.resend_max_attempts = self._clamp_int(resend_max_attempts, 2, 1, 5)
        self.resend_delay_seconds = self._clamp_float(resend_delay_seconds, 15.0, 1.0, 300.0)
        self.comment_retrieval_limit = self._clamp_int(comment_retrieval_limit, 24, 12, 120)
        self.resend_dedupe_ttl_seconds = 3600.0
        self.min_write_interval_seconds = self._clamp_float(
            min_write_interval_seconds, DEFAULT_MIN_WRITE_INTERVAL_SECONDS, 0.0, 60.0
        )
        # “使用第三方图床”：开启后第三方图床取代 OttoHub 自带图床成为主图床。
        # “兜底模式”：仅在上面开启时才有意义——第三方图床全部失败后，是否回退到
        # OttoHub 自带图床再试一次。
        self.use_third_party_image_host = bool(use_third_party_image_host)
        self.fallback_mode = bool(fallback_mode)
        self._fallback_hosts = image_hosts.build_fallback_hosts(
            enabled=self.use_third_party_image_host,
            r2_account_id=r2_account_id,
            r2_access_key_id=r2_access_key_id,
            r2_secret_access_key=r2_secret_access_key,
            r2_bucket_name=r2_bucket_name,
            r2_public_url=r2_public_url,
            stardots_key=stardots_key,
            stardots_secret=stardots_secret,
            stardots_space=stardots_space,
            beeimg_token=beeimg_token,
            beeimg_strategy_id=beeimg_strategy_id,
        )
        self.fallback_max_bytes = self._clamp_int(fallback_max_kb, 800, 50, 3000) * 1024
        # OttoHubClient 本身只在平台适配器启动/重载时构造一次；下面这个签名用来在
        # 关键调用点重新读一遍磁盘上的部分设置（第三方图床、最短发送间隔等），这样
        # 在 WebUI 里改这些值不需要重启整个 AstrBot 就能生效（和适配器里其它按
        # _config_value 实时读取的设置行为保持一致），而不是被构造时传入的值永久锁死。
        self._dynamic_config_signature: tuple | None = None
        self.session: aiohttp.ClientSession | None = None
        self._write_lock = asyncio.Lock()
        self._last_write_ts: float = 0.0
        self._resend_keys: dict[str, float] = {}
        self._resend_tasks: set[asyncio.Task] = set()
        self._bg_tasks: set[asyncio.Task] = set()
        # 记录已发评论 ID，用于审核通知时的自动删除
        self._monitored_comments: dict[str, tuple[str, float]] = {}
        self.adapter = None  # 在 adapter 实例化后反向绑定

    def _spawn_bg_task(self, coro) -> None:
        """启动后台 fire-and-forget 任务并持有引用，防止被 GC 提前回收。"""
        task = asyncio.create_task(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    # ------------------------------------------------------------------ session

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            headers = {"User-Agent": self.user_agent} if self.user_agent else {}
            self.session = aiohttp.ClientSession(cookies=self.cookies, headers=headers)
        return self.session




    def mark_monitored_comment(self, kind: str, comment_id: str) -> None:
        if not self.resend_delete_on_audit:
            return
        self._prune_monitored_comments()
        self._monitored_comments[str(comment_id)] = (kind, time.time())

    def _prune_monitored_comments(self) -> None:
        now = time.time()
        for cid, (kind, created_at) in list(self._monitored_comments.items()):
            if now - created_at > 6 * 3600:
                self._monitored_comments.pop(cid, None)

    async def _fetch_and_mark_new_comment(self, kind: str, post_id: int, parent_id: int, content: str):
        if not self.resend_delete_on_audit:
            return
        # 等待 1.2 秒以确保 OttoHub 服务端数据库同步已完成
        await asyncio.sleep(1.2)
        try:
            bot_uid = str(self.uid or "")
            clean_content = content.strip()
            if kind == "blog":
                comments = await self.get_blog_comments(post_id, num=6, parent_bcid=parent_id)
            else:
                comments = await self.get_video_comments(post_id, num=6, parent_vcid=parent_id)
            
            for comment in comments:
                c_uid = str(comment.get("uid") or comment.get("author_uid") or "")
                c_text = str(comment.get("content") or comment.get("message") or comment.get("text") or "").strip()
                if c_uid == bot_uid and c_text == clean_content:
                    comment_id = comment.get("bcid") or comment.get("vcid") or comment.get("comment_id")
                    if comment_id:
                        self.mark_monitored_comment(kind, comment_id)
                        logger.info("[OttoHub] 评论发送已确认 kind=%s ID=%s", kind, comment_id)
                        break
        except Exception as e:
            logger.warning("[OttoHub] 异步获取并标记新评论 ID 失败: %s", e)

    async def close(self) -> None:
        if self.session:
            await self.session.close()

    # ------------------------------------------------------------------ utilities

    @staticmethod
    def _safe_params(params: dict[str, Any]) -> dict[str, Any]:
        return {k: ("***" if k == "token" else v) for k, v in params.items()}

    @staticmethod
    def _limit_chars(text: str, max_chars: int = OTTOHUB_PLATFORM_MAX_CHARS) -> str:
        return truncate_chars(text, max_chars)

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

    @staticmethod
    def _is_transient_error(exc: Exception) -> bool:
        """判断是否为可重试的瞬时网络错误（超时/连接问题/服务端 5xx）。"""
        return isinstance(
            exc,
            (
                asyncio.TimeoutError,
                aiohttp.ServerDisconnectedError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientPayloadError,
            ),
        )

    async def _fetch_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
        log_tag: str = "OttoHub API",
    ) -> dict[str, Any]:
        """统一的 JSON 请求：带超时、瞬时错误重试与非 JSON 容错。"""
        session = await self._get_session()
        last_exc: Exception | None = None
        for attempt in range(1, TRANSIENT_REQUEST_ATTEMPTS + 1):
            try:
                async with session.request(
                    method, url, params=params, timeout=timeout or DEFAULT_REQUEST_TIMEOUT
                ) as resp:
                    text = await resp.text()
                    try:
                        return json.loads(text)
                    except json.JSONDecodeError:
                        logger.warning("[%s] 响应非 JSON，status=%s", log_tag, resp.status)
                        return {"status": "error", "message": "bad_json"}
            except Exception as exc:
                last_exc = exc
                if self._is_transient_error(exc) and attempt < TRANSIENT_REQUEST_ATTEMPTS:
                    delay = TRANSIENT_RETRY_BACKOFF * attempt
                    logger.warning(
                        "[%s] 瞬时请求异常(第 %d/%d 次)，%.1fs 后重试: %s",
                        log_tag, attempt, TRANSIENT_REQUEST_ATTEMPTS, delay, exc,
                    )
                    await asyncio.sleep(delay)
                    continue
                break
        logger.warning("[%s] 请求异常: %s", log_tag, last_exc)
        return {"status": "error", "message": "request_failed"}

    async def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        request_params = dict(params)
        request_params["offset"] = request_params.get("offset", 0)
        if self.token:
            request_params["token"] = self.token
        data = await self._fetch_json(method, self.api_base, params=request_params)
        if data.get("status") == "error" and data.get("message") not in ("bad_json", "request_failed"):
            logger.warning(
                "[OttoHub API] 请求失败 message=%s params=%s",
                data.get("message"),
                self._safe_params(request_params),
            )
        return data

    async def _site_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        request_params = dict(params or {})
        if self.token and "token" not in request_params:
            request_params["token"] = self.token
        return await self._fetch_json(
            "GET", self.site_base + path, params=request_params, log_tag="OttoHub site API"
        )

    async def _api_path_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        request_params = dict(params or {})
        if self.token and "token" not in request_params:
            request_params["token"] = self.token
        return await self._fetch_json(
            "GET", "https://api.ottohub.cn" + path, params=request_params, log_tag="OttoHub REST API"
        )

    async def _throttle_write(self) -> None:
        """确保与上一次写操作（评论/私信）至少间隔 self.min_write_interval_seconds，
        避免紧挨着连续发起请求触发 OttoHub 的 too_many_requests 限流。"""
        self._refresh_dynamic_config()
        wait = self.min_write_interval_seconds - (time.time() - self._last_write_ts)
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_write_ts = time.time()

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
        try:
            msg_id = int(msg_id)
        except (TypeError, ValueError):
            logger.warning("[OttoHub] mark_message_read 收到非法 msg_id，已跳过: %r", msg_id)
            return False
        response = await self._request("GET", {"module": "im", "action": "read_message", "msg_id": msg_id})
        return response.get("status") == "success"

    async def send_message(self, receiver: int, content: str) -> bool:
        async with self._write_lock:
            await self._throttle_write()
            response = await self._request(
                "POST", {"module": "im", "action": "send_message", "receiver": receiver, "message": self._limit_chars(content)}
            )
            return self._write_ok(response, "send_message")

    async def comment_blog(self, bid: int, content: str, parent_bcid: int = 0) -> bool:
        async with self._write_lock:
            await self._throttle_write()
            logger.info("[OttoHub] 评论动态 bid=%s parent_bcid=%s", bid, parent_bcid)
            response = await self._request(
                "POST",
                {
                    "module": "comment",
                    "action": "comment_blog",
                    "bid": bid,
                    "parent_bcid": int(parent_bcid or 0),
                    "content": self._limit_chars(content),
                },
            )
            ok = self._write_ok(response, "comment_blog")
            if ok:
                self._spawn_bg_task(self._fetch_and_mark_new_comment("blog", bid, int(parent_bcid or 0), content))
            return ok

    async def delete_blog_comment(self, bcid: int) -> bool:
        async with self._write_lock:
            logger.info("[OttoHub] 删除动态评论 bcid=%s", bcid)
            response = await self._request(
                "POST",
                {
                    "module": "comment",
                    "action": "delete_blog_comment",
                    "bcid": int(bcid),
                },
            )
            return self._write_ok(response, "delete_blog_comment")

    async def comment_video(self, vid: int, content: str, parent_vcid: int = 0) -> bool:
        async with self._write_lock:
            await self._throttle_write()
            logger.info("[OttoHub] 评论视频 vid=%s parent_vcid=%s", vid, parent_vcid)
            response = await self._request(
                "POST",
                {
                    "module": "comment",
                    "action": "comment_video",
                    "vid": vid,
                    "parent_vcid": int(parent_vcid or 0),
                    "content": self._limit_chars(content),
                },
            )
            ok = self._write_ok(response, "comment_video")
            if ok:
                self._spawn_bg_task(self._fetch_and_mark_new_comment("video", vid, int(parent_vcid or 0), content))
            return ok

    async def delete_video_comment(self, vcid: int) -> bool:
        async with self._write_lock:
            logger.info("[OttoHub] 删除视频评论 vcid=%s", vcid)
            response = await self._request(
                "POST",
                {
                    "module": "comment",
                    "action": "delete_video_comment",
                    "vcid": int(vcid),
                },
            )
            return self._write_ok(response, "delete_video_comment")

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
        uid = str(uid or "").strip()
        if not uid or not self.token:
            return {"is_fan": False, "follow_status": 0, "api_error": True}
        return await self._relationship_from_user_page(uid)

    @staticmethod
    def _extract_follow_status(response: dict[str, Any]) -> int | None:
        if not isinstance(response, dict) or response.get("status") == "error":
            return None
        candidates = [
            response.get("follow_status"),
            response.get("relation"),
            response.get("status_code"),
        ]
        data = response.get("data")
        if isinstance(data, dict):
            candidates.extend([data.get("follow_status"), data.get("relation"), data.get("status")])
        for value in candidates:
            if value in (None, ""):
                continue
            try:
                status = int(value)
            except Exception:
                continue
            if status in {0, 1, 2, 3, 4}:
                return status
        return None

    @staticmethod
    def _relationship_from_status(status: int) -> dict[str, Any]:
        return {
            "is_fan": status in {3, 4},
            "is_following": status in {2, 4},
            "is_mutual": status == 4,
            "follow_status": status,
            "api_error": False,
        }

    async def _relationship_from_user_page(self, uid: str) -> dict[str, Any]:
        bot_uid = str(self.uid or "").strip()
        if not bot_uid:
            return {"is_fan": False, "follow_status": 0, "api_error": True}

        fan_result = await self._uid_exists_in_list(f"/api/following/list/{uid}", bot_uid)
        if fan_result is None:
            return {"is_fan": False, "follow_status": 0, "api_error": True}
        following_result = await self._uid_exists_in_list(f"/api/following/fans/{uid}", bot_uid)
        if following_result is None:
            return {"is_fan": False, "follow_status": 0, "api_error": True}

        if fan_result and following_result:
            return self._relationship_from_status(4)
        if following_result:
            return self._relationship_from_status(2)
        if fan_result:
            return self._relationship_from_status(3)
        return self._relationship_from_status(1)

    async def _uid_exists_in_list(self, path: str, uid: str) -> bool | None:
        page_size = 18
        for page in range(20):
            response = await self._api_path_json(path, {"offset": page * page_size, "num": page_size})
            if response.get("status") == "error":
                return None
            data = response.get("data") if isinstance(response, dict) else None
            users = data.get("user_list") if isinstance(data, dict) else None
            if not isinstance(users, list):
                return None
            if not users:
                return False
            for user in users:
                if isinstance(user, dict) and str(user.get("uid") or "").strip() == uid:
                    return True
            if len(users) < page_size:
                return False
        logger.warning("[OttoHub] 关系列表查询达到分页上限 path=%s uid=%s", path, uid)
        return False

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
        msg_id: str = None,
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
            self._resend_worker(key, reply_type, int(context_id or 0), int(parent_id or 0), receiver_uid, clean_parts, msg_id)
        )
        self._resend_tasks.add(task)
        task.add_done_callback(self._resend_tasks.discard)
        logger.warning(
            "[OttoHub] 已加入补发队列 key=%.12s type=%s 段数=%d msg_id=%s",
            key, reply_type, len(clean_parts), msg_id,
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
        # 补发前检测 OttoHub 页面上是否已经成功发表了这条评论
        try:
            bot_uid = str(self.uid or "")
            clean_content = content.strip()
            
            if reply_type == "blog_comment":
                comments = await self.get_blog_comments(context_id, num=12, parent_bcid=parent_id)
                for comment in comments:
                    c_uid = str(comment.get("uid") or comment.get("author_uid") or "")
                    c_text = str(comment.get("content") or comment.get("message") or comment.get("text") or "").strip()
                    if c_uid == bot_uid and c_text == clean_content:
                        logger.info("[OttoHub] 补发检测：评论已存在，无需重复补发")
                        return True
                        
            elif reply_type == "video_comment":
                comments = await self.get_video_comments(context_id, num=12, parent_vcid=parent_id)
                for comment in comments:
                    c_uid = str(comment.get("uid") or comment.get("author_uid") or "")
                    c_text = str(comment.get("content") or comment.get("message") or comment.get("text") or "").strip()
                    if c_uid == bot_uid and c_text == clean_content:
                        logger.info("[OttoHub] 补发检测：评论已存在，无需重复补发")
                        return True
        except Exception as exc:
            logger.warning("[OttoHub] 补发检测异常: %s", exc)

        # 若不存在，才进行真正发送
        if reply_type == "blog_comment":
            return await self.comment_blog(context_id, content, parent_id)
        if reply_type == "video_comment":
            return await self.comment_video(context_id, content, parent_id)
        if receiver_uid:
            return await self.send_message(int(receiver_uid), content)
        return False

    async def _is_parent_comment_exists(self, reply_type: str, context_id: int, parent_id: int) -> bool:
        if not parent_id:
            return True
        limit = getattr(self, "comment_retrieval_limit", 24)
        page_size = 12
        try:
            retrieved = 0
            while retrieved < limit:
                num_to_fetch = min(page_size, limit - retrieved)
                if reply_type == "blog_comment":
                    comments = await self.get_blog_comments(bid=context_id, num=num_to_fetch, parent_bcid=0, offset=retrieved)
                    for c in comments:
                        cid = int(c.get("bcid") or c.get("id") or 0)
                        if cid == parent_id:
                            return True
                    retrieved += len(comments)
                    if len(comments) < num_to_fetch:
                        break
                elif reply_type == "video_comment":
                    comments = await self.get_video_comments(vid=context_id, num=num_to_fetch, parent_vcid=0, offset=retrieved)
                    for c in comments:
                        cid = int(c.get("vcid") or c.get("id") or 0)
                        if cid == parent_id:
                            return True
                    retrieved += len(comments)
                    if len(comments) < num_to_fetch:
                        break
            return False
        except Exception as e:
            logger.warning("[OttoHub] 检查父评论是否存在异常，默认视为存在: %s", e)
            return True

    async def _resend_worker(
        self,
        key: str,
        reply_type: str,
        context_id: int,
        parent_id: int,
        receiver_uid: int | None,
        parts: list[str],
        msg_id: str = None,
    ) -> None:
        if self.resend_re_respond:
            await asyncio.sleep(self.resend_delay_seconds)
            if reply_type in ("blog_comment", "video_comment") and parent_id:
                if not await self._is_parent_comment_exists(reply_type, context_id, parent_id):
                    logger.warning("[OttoHub] 重新响应检测：父评论 %s 已不存在，直接放弃重新响应", parent_id)
                    return
            if reply_type in ("blog_comment", "video_comment") and msg_id and self.adapter:
                logger.warning("[OttoHub] 开启重新响应，将补发任务改为重新触发响应 msg_id=%s", msg_id)
                self.adapter.trigger_re_response(msg_id)
            return

        await asyncio.sleep(self.resend_delay_seconds)

        if reply_type in ("blog_comment", "video_comment") and parent_id:
            if not await self._is_parent_comment_exists(reply_type, context_id, parent_id):
                logger.warning("[OttoHub] 补发检测：父评论 %s 已不存在，放弃补发任务", parent_id)
                return

        remaining_parts = list(parts)
        resend_ok = False
        for attempt in range(1, self.resend_max_attempts + 1):
            if attempt > 1:
                if reply_type in ("blog_comment", "video_comment") and parent_id:
                    if not await self._is_parent_comment_exists(reply_type, context_id, parent_id):
                        logger.warning("[OttoHub] 重试补发检测：父评论 %s 已不存在，放弃后续重试", parent_id)
                        return
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
                resend_ok = True
                break
            remaining_parts = remaining_parts[failed_at:]
            logger.warning(
                "[OttoHub] 补发失败 key=%.12s attempt=%d/%d failed_at=%d",
                key, attempt, self.resend_max_attempts, failed_at,
            )
            if attempt < self.resend_max_attempts:
                await asyncio.sleep(self.resend_delay_seconds)

        if not resend_ok:
            logger.error("[OttoHub] 补发已耗尽重试次数 key=%.12s attempts=%d", key, self.resend_max_attempts)

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

    def _shrink_for_fallback(self, data: bytes, filename: str, content_type: str) -> tuple[bytes, str, str]:
        """兜底图床上传前的二次压缩：体积越小上传越快，也越不容易撞到第三方图床的
        单图大小限制（例如 StarDots 单图上限 3MB）。GIF/SVG 保持原样，不做有损转码。"""
        if content_type in ("image/gif", "image/svg+xml") or len(data) <= self.fallback_max_bytes:
            return data, filename, content_type
        try:
            import io

            from PIL import Image as PILImage

            img = PILImage.open(io.BytesIO(data))
            if img.mode not in ("RGB", "L"):
                img = img.convert("RGB")
            base_name = os.path.splitext(filename)[0] + ".jpg"

            best = data
            for quality in (70, 55, 40, 28, 18):
                out = io.BytesIO()
                img.save(out, format="JPEG", quality=quality, optimize=True)
                best = out.getvalue()
                if len(best) <= self.fallback_max_bytes:
                    return best, base_name, "image/jpeg"

            width, height = img.size
            for scale in (0.75, 0.5, 0.35):
                resized = img.resize(
                    (max(1, int(width * scale)), max(1, int(height * scale))),
                    PILImage.Resampling.LANCZOS,
                )
                out = io.BytesIO()
                resized.save(out, format="JPEG", quality=40, optimize=True)
                best = out.getvalue()
                if len(best) <= self.fallback_max_bytes:
                    return best, base_name, "image/jpeg"

            logger.warning(
                "[OttoHub] 兜底图床压缩后仍超过 %d KB，按压缩到最小的一次结果上传",
                self.fallback_max_bytes // 1024,
            )
            return best, base_name, "image/jpeg"
        except Exception as exc:
            logger.warning("[OttoHub] 兜底图床压缩失败，改用原始数据: %s", exc)
            return data, filename, content_type

    async def _upload_via_fallback_hosts(self, upload_payload: tuple[bytes, str, str], file_hash: str) -> str | None:
        data, filename, content_type = self._shrink_for_fallback(*upload_payload)
        for host in self._fallback_hosts:
            try:
                url = await host.upload(data, filename, content_type)
            except Exception as exc:
                logger.warning("[OttoHub][%s] 兜底上传异常: %s", host.name, exc)
                continue
            if not url:
                logger.warning("[OttoHub][%s] 兜底上传失败", host.name)
                continue
            logger.info("[OttoHub] 兜底图床 %s 上传成功: %s", host.name, url)
            if file_hash:
                cache = self._load_image_cache()
                entry = cache.get(file_hash, {})
                entry["oh"] = url
                cache[file_hash] = entry
                self._save_image_cache(cache)
            return url
        return None

    def _refresh_dynamic_config(self) -> None:
        """从磁盘重新读取第三方图床、最短发送间隔等设置，跳过没有变化的情况（避免
        每次调用都重建 host 列表、刷日志）。适配器不存在或读取失败时保留上一次的
        状态不动。"""
        adapter = getattr(self, "adapter", None)
        if not adapter or not hasattr(adapter, "_load_plugin_config"):
            return
        try:
            pc = adapter._load_plugin_config()
        except Exception:
            return

        signature = (
            bool(pc.get("use_third_party_image_host", False)),
            bool(pc.get("fallback_mode", True)),
            pc.get("fallback_max_kb", 800),
            pc.get("r2_account_id", ""),
            pc.get("r2_access_key_id", ""),
            pc.get("r2_secret_access_key", ""),
            pc.get("r2_bucket_name", ""),
            pc.get("r2_public_url", ""),
            pc.get("stardots_key", ""),
            pc.get("stardots_secret", ""),
            pc.get("stardots_space", ""),
            pc.get("beeimg_token", ""),
            pc.get("beeimg_strategy_id", ""),
            pc.get("min_write_interval_seconds", DEFAULT_MIN_WRITE_INTERVAL_SECONDS),
        )
        if signature == self._dynamic_config_signature:
            return

        self._dynamic_config_signature = signature
        self.use_third_party_image_host = signature[0]
        self.fallback_mode = signature[1]
        self.fallback_max_bytes = self._clamp_int(signature[2], 800, 50, 3000) * 1024
        self._fallback_hosts = image_hosts.build_fallback_hosts(
            enabled=self.use_third_party_image_host,
            r2_account_id=signature[3],
            r2_access_key_id=signature[4],
            r2_secret_access_key=signature[5],
            r2_bucket_name=signature[6],
            r2_public_url=signature[7],
            stardots_key=signature[8],
            stardots_secret=signature[9],
            stardots_space=signature[10],
            beeimg_token=signature[11],
            beeimg_strategy_id=signature[12],
        )
        self.min_write_interval_seconds = self._clamp_float(
            signature[13], DEFAULT_MIN_WRITE_INTERVAL_SECONDS, 0.0, 60.0
        )
        logger.info(
            "[OttoHub] 动态配置已刷新: 使用第三方图床=%s 兜底模式=%s hosts=%s 最短发送间隔=%.1fs",
            self.use_third_party_image_host, self.fallback_mode,
            [h.name for h in self._fallback_hosts], self.min_write_interval_seconds,
        )

    async def upload_image(self, file_path: str) -> str | None:
        self._refresh_dynamic_config()
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

        if file_hash:
            cache = self._load_image_cache()
            cached_entry = cache.get(file_hash, {})
            cached_url = cached_entry.get("oh")
            if cached_url:
                if not self.image_upload_validate or await self.validate_image_url(cached_url):
                    logger.info("[OttoHub] 复用缓存图片 hash=%.10s url=%s", file_hash, cached_url)
                    return cached_url
                logger.warning("[OttoHub] 缓存图片已失效: %s", cached_url)
                cached_entry.pop("oh", None)
                cache[file_hash] = cached_entry
                self._save_image_cache(cache)

        # “使用第三方图床”开启时，第三方图床取代 OttoHub 自带图床成为主图床；
        # “兜底模式”决定第三方图床全部失败后要不要回退到 OttoHub 自带图床。
        if self.use_third_party_image_host:
            if self._fallback_hosts:
                url = await self._upload_via_fallback_hosts(upload_payload, file_hash)
                if url:
                    return url
                logger.warning("[OttoHub] 第三方图床均上传失败: %s", path)
                if not self.fallback_mode:
                    logger.error("[OttoHub] 兜底模式已关闭，不再回退到 OttoHub 自带图床，本次放弃: %s", path)
                    return None
                logger.info("[OttoHub] 兜底模式已开启，回退尝试 OttoHub 自带图床")
            else:
                logger.warning("[OttoHub] 已开启“使用第三方图床”但未填写任何图床凭据，直接使用 OttoHub 自带图床")

        return await self._upload_via_ottohub_own(upload_payload, file_hash, path)

    async def _upload_via_ottohub_own(
        self, upload_payload: tuple[bytes, str, str], file_hash: str, path: str
    ) -> str | None:
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
                timeout=UPLOAD_REQUEST_TIMEOUT,
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
