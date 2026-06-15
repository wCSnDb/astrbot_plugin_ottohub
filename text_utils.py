"""OttoHub 适配器共享文本工具。

集中所有跨模块复用的正则、文本清洗与 UTF-8 截断逻辑，避免在
main.py / adapter.py / event.py 中重复定义同一套实现。
"""

import re

# ---------------------------------------------------------------- 正则常量

# 匹配孤立的 [图片]/[image] 占位符（排除 Markdown 图片语法 ![..](..)）
IMAGE_PLACEHOLDER_RE = re.compile(r"(?<![!\[])[ \t]*\[(?:图片|image)\][ \t]*(?!\()", re.I)

# 匹配 LLM 误输出的工具调用块及其标签
TOOL_CALL_BLOCK_RE = re.compile(
    r"(?is)\b[a-zA-Z_][\w.:-]*\s*\n"
    r"(?:\s*<arg_key>.*?</arg_key>\s*(?:\n\s*<arg_value>.*?</arg_value>\s*)?)+"
    r"\s*(?:</tool_call>)?"
)
TOOL_CALL_TAG_RE = re.compile(
    r"(?is)</?tool_call>|<arg_key>.*?</arg_key>|<arg_value>.*?</arg_value>"
)

# 提取图片直链
IMAGE_URL_RE = re.compile(
    r"https?://[^\s\"'<>]+\.(?:jpg|jpeg|png|gif|webp|bmp|svg)", re.I
)

# base64 内联图片
DATA_IMAGE_RE = re.compile(r"data:image/[^;]+;base64,[A-Za-z0-9+/=]+")


# ---------------------------------------------------------------- 文本清洗

def strip_image_placeholders(text: str) -> str:
    """移除孤立的 [图片]/[image] 占位符并压缩多余空白。"""
    cleaned = IMAGE_PLACEHOLDER_RE.sub(" ", str(text or ""))
    return re.sub(r"[ \t]{2,}", " ", cleaned).strip()


def sanitize_text(text: str) -> str:
    """清洗文本：去除工具调用残留、图片占位符并规范空白。"""
    cleaned = TOOL_CALL_BLOCK_RE.sub(" ", str(text or ""))
    cleaned = TOOL_CALL_TAG_RE.sub(" ", cleaned)
    cleaned = strip_image_placeholders(cleaned)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = re.sub(r"\n[ \t]+", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def strip_data_images(text: str) -> str:
    """移除文本中的 base64 内联图片数据。"""
    return DATA_IMAGE_RE.sub("", str(text or ""))


# ---------------------------------------------------------------- 图片工具

def extract_images(text: str) -> list[str]:
    """从文本中提取所有图片直链。"""
    return IMAGE_URL_RE.findall(str(text or ""))


def dedupe(values: list[str]) -> list[str]:
    """保序去重，丢弃空值。"""
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


# ---------------------------------------------------------------- 长度截断

def truncate_chars(text: str, max_chars: int) -> str:
    """按字符数截断字符串（OttoHub 平台以字符计长，非字节）。"""
    text = str(text or "")
    if len(text) <= max_chars:
        return text
    return text[:max_chars]
