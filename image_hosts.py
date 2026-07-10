"""图片上传兜底图床。

当 OttoHub 自带图床（submit_image.php）上传失败并耗尽重试后，按配置依次尝试
这里实现的第三方图床，直到有一个成功为止。每个 Host 类只需要实现一个方法：

    async def upload(self, data: bytes, filename: str, content_type: str) -> str | None

返回可直接拿去拼 Markdown 图片链接的公开 URL；失败返回 None（并自行记录日志），
调用方（client.py）负责依次尝试下一个。
"""

import asyncio
import hashlib
import logging
import os
import random
import string
import time

import aiohttp

logger = logging.getLogger("astrbot")


class CloudflareR2Host:
    """Cloudflare R2（S3 兼容 API）。用 boto3（同步）+ asyncio.to_thread 调用，
    避免在这个插件里再引入一套手写的 AWS SigV4 签名实现。"""

    name = "cloudflare_r2"

    def __init__(
        self,
        account_id: str,
        access_key_id: str,
        secret_access_key: str,
        bucket_name: str,
        public_url: str = "",
    ):
        self.account_id = account_id
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.bucket_name = bucket_name
        self.public_url = public_url.rstrip("/") if public_url else ""
        self._client = None

    def _get_client(self):
        if self._client is None:
            import boto3
            from botocore.config import Config

            self._client = boto3.client(
                "s3",
                endpoint_url=f"https://{self.account_id}.r2.cloudflarestorage.com",
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            )
        return self._client

    def _put_object_sync(self, key: str, data: bytes, content_type: str) -> None:
        client = self._get_client()
        client.put_object(Bucket=self.bucket_name, Key=key, Body=data, ContentType=content_type)

    def _public_url(self, key: str) -> str:
        if self.public_url:
            return f"{self.public_url}/{key}"
        return f"https://{self.bucket_name}.{self.account_id}.r2.dev/{key}"

    async def upload(self, data: bytes, filename: str, content_type: str) -> str | None:
        key = f"ottohub/{hashlib.md5(data).hexdigest()}_{filename}"
        try:
            await asyncio.to_thread(self._put_object_sync, key, data, content_type)
        except Exception as exc:
            logger.warning("[OttoHub][R2] 上传失败: %s", exc)
            return None
        return self._public_url(key)


class StarDotsHost:
    """StarDots (https://stardots.io) 图床。签名机制参考官方 OpenAPI 文档：
    x-stardots-timestamp / x-stardots-nonce / x-stardots-key / x-stardots-sign
    (sign = md5(f"{timestamp}|{secret}|{nonce}").hexdigest().upper())。
    单图上限 3MB，超限请在调用方压缩后再传入。"""

    name = "stardots"
    BASE_URL = "https://api.stardots.io"

    def __init__(self, key: str, secret: str, space: str):
        self.key = key
        self.secret = secret
        self.space = space
        self._time_offset: int | None = None

    async def _ensure_time_offset(self, session: aiohttp.ClientSession) -> None:
        if self._time_offset is not None:
            return
        try:
            async with session.get(
                f"{self.BASE_URL}/openapi/space/list", timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                result = await resp.json(content_type=None)
            server_ts = int(result.get("ts") or 0) // 1000
            self._time_offset = (server_ts - int(time.time())) if server_ts else 8 * 3600
        except Exception:
            self._time_offset = 8 * 3600

    def _headers(self) -> dict[str, str]:
        timestamp = str(int(time.time() + (self._time_offset or 0)))
        nonce = "".join(random.choices(string.ascii_letters + string.digits, k=10))
        sign_str = f"{timestamp}|{self.secret}|{nonce}"
        sign = hashlib.md5(sign_str.encode()).hexdigest().upper()
        return {
            "x-stardots-timestamp": timestamp,
            "x-stardots-nonce": nonce,
            "x-stardots-key": self.key,
            "x-stardots-sign": sign,
        }

    async def upload(self, data: bytes, filename: str, content_type: str) -> str | None:
        try:
            async with aiohttp.ClientSession() as session:
                await self._ensure_time_offset(session)
                form = aiohttp.FormData()
                form.add_field("file", data, filename=filename, content_type=content_type)
                form.add_field("space", self.space)
                async with session.put(
                    f"{self.BASE_URL}/openapi/file/upload",
                    headers=self._headers(),
                    data=form,
                    timeout=aiohttp.ClientTimeout(total=60),
                ) as resp:
                    result = await resp.json(content_type=None)
        except Exception as exc:
            logger.warning("[OttoHub][StarDots] 上传异常: %s", exc)
            return None
        if not result.get("success"):
            logger.warning("[OttoHub][StarDots] 上传失败: %s", result.get("message"))
            return None
        url = (result.get("data") or {}).get("url")
        if not url:
            logger.warning("[OttoHub][StarDots] 响应中未找到 url 字段: %s", result)
        return url


class BeeImgHost:
    """蜜蜂图床 (beeimg.cn)。

    其官方 API 文档页面（/pages/api-docs）是前端 SPA 动态渲染的，工具没能抓到
    实际接口内容；从官网自我介绍（RESTful API、多种链接格式）判断它大概率是基于
    兰空图床(Lsky Pro) V2 二次搭建的，这里按 Lsky Pro V2 的标准 API 实现：
        POST {base_url}/api/v1/upload
        Header: Authorization: Bearer <token>
        Body(multipart): file=<图片>, strategy_id=<可选>
        Resp: {"status": true, "data": {"links": {"url": "..."}}}
    如果实际接口有出入，请对照真实返回结果调整这里的字段名。
    """

    name = "beeimg"

    def __init__(self, token: str, strategy_id: str = "", base_url: str = "https://www.beeimg.cn"):
        self.token = token
        self.strategy_id = strategy_id
        self.base_url = base_url.rstrip("/")

    async def upload(self, data: bytes, filename: str, content_type: str) -> str | None:
        form = aiohttp.FormData()
        form.add_field("file", data, filename=filename, content_type=content_type)
        if self.strategy_id:
            form.add_field("strategy_id", str(self.strategy_id))
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/api/v1/upload",
                    headers=headers,
                    data=form,
                    timeout=aiohttp.ClientTimeout(total=60),
                ) as resp:
                    result = await resp.json(content_type=None)
        except Exception as exc:
            logger.warning("[OttoHub][BeeImg] 上传异常: %s", exc)
            return None
        if not result.get("status"):
            logger.warning("[OttoHub][BeeImg] 上传失败: %s", result.get("message"))
            return None
        url = ((result.get("data") or {}).get("links") or {}).get("url")
        if not url:
            logger.warning("[OttoHub][BeeImg] 响应中未找到 data.links.url 字段: %s", result)
        return url


def build_fallback_hosts(
    *,
    enabled: bool,
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
) -> list:
    """按固定优先级（R2 -> StarDots -> BeeImg）组装已经完整配置好凭据的兜底图床列表。"""
    if not enabled:
        return []
    hosts: list = []
    if r2_account_id and r2_access_key_id and r2_secret_access_key and r2_bucket_name:
        hosts.append(CloudflareR2Host(r2_account_id, r2_access_key_id, r2_secret_access_key, r2_bucket_name, r2_public_url))
    if stardots_key and stardots_secret and stardots_space:
        hosts.append(StarDotsHost(stardots_key, stardots_secret, stardots_space))
    if beeimg_token:
        hosts.append(BeeImgHost(beeimg_token, beeimg_strategy_id))
    if hosts:
        logger.info("[OttoHub] 已启用 %d 个兜底图床: %s", len(hosts), ", ".join(h.name for h in hosts))
    return hosts
