from __future__ import annotations

import codecs
import re
from dataclasses import dataclass
from html.parser import HTMLParser

import requests

from .fetch import FetchError


ALIASES_PAGE_URL = "https://manhua.zaimanhua.com/details/{comic_id}"
ALIASES_SCAN_LIMIT_BYTES = 256_000
ALIASES_SCAN_CHUNK_SIZE = 8_192

HTML_HEADERS: dict[str, str] = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1",
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    ),
}


@dataclass(frozen=True)
class AliasResult:
    aliases: list[str]


def fetch_aliases(*, session: requests.Session, comic_id: int, timeout_seconds: float) -> AliasResult:
    url = ALIASES_PAGE_URL.format(comic_id=comic_id)
    try:
        resp = session.get(url, headers=HTML_HEADERS, timeout=timeout_seconds, stream=True)
    except requests.RequestException as exc:
        raise FetchError(f"别名请求失败: {exc!r}") from exc

    with resp:
        if resp.status_code != 200:
            raise FetchError(
                f"别名 HTTP 状态码异常: {resp.status_code}",
                status_code=resp.status_code,
                response_text=_safe_text(resp),
            )

        keywords_content = _extract_keywords_content_streaming(resp)
        try:
            aliases = _parse_aliases_from_keywords(keywords_content)
        except FetchError as exc:
            if exc.response_text is not None:
                raise
            raise FetchError(str(exc), response_text=_truncate_text(keywords_content)) from exc
        return AliasResult(aliases=aliases)


class _KeywordsMetaParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.keywords_content: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "meta":
            return
        if self.keywords_content is not None:
            return
        attr_map: dict[str, str] = {}
        for key, value in attrs:
            if key is None or value is None:
                continue
            attr_map[key.lower()] = value
        if attr_map.get("name") == "keywords":
            content = attr_map.get("content")
            if content:
                self.keywords_content = content


def _extract_keywords_content(html: str) -> str:
    parser = _KeywordsMetaParser()
    try:
        parser.feed(html)
    except Exception as exc:
        raise FetchError(f"别名页面解析失败: {exc!r}") from exc
    if not parser.keywords_content:
        raise FetchError("别名页面缺少 meta[name=keywords] 或 content 为空")
    return parser.keywords_content


def _extract_keywords_content_streaming(resp: requests.Response) -> str:
    fallback_encoding = resp.encoding or "utf-8"
    bytes_read = 0
    buffered = bytearray()
    for chunk in resp.iter_content(chunk_size=ALIASES_SCAN_CHUNK_SIZE):
        if not chunk:
            continue
        bytes_read += len(chunk)
        buffered.extend(chunk)
        keywords_content = _extract_keywords_content_from_bytes(
            bytes(buffered),
            fallback_encoding=fallback_encoding,
        )
        if keywords_content:
            return keywords_content
        if bytes_read >= ALIASES_SCAN_LIMIT_BYTES:
            break

    keywords_content = _extract_keywords_content_from_bytes(
        bytes(buffered),
        fallback_encoding=fallback_encoding,
    )
    if keywords_content:
        return keywords_content
    raise FetchError(f"别名页面未在前 {ALIASES_SCAN_LIMIT_BYTES} 字节内找到 meta[name=keywords]")


_CHARSET_RE = re.compile(
    rb"""<meta\b[^>]*\bcharset\s*=\s*["']?\s*([A-Za-z0-9._:-]+)""",
    re.IGNORECASE,
)


def _extract_keywords_content_from_bytes(data: bytes, *, fallback_encoding: str) -> str | None:
    if not data:
        return None

    encoding = _detect_html_encoding(data, fallback_encoding=fallback_encoding)
    decoder = codecs.getincrementaldecoder(encoding)(errors="replace")
    html = decoder.decode(data, final=True)
    parser = _KeywordsMetaParser()
    try:
        parser.feed(html)
    except Exception as exc:
        raise FetchError(f"别名页面解析失败: {exc!r}") from exc
    return parser.keywords_content


def _detect_html_encoding(data: bytes, *, fallback_encoding: str) -> str:
    if data.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"

    match = _CHARSET_RE.search(data)
    if match:
        try:
            encoding = match.group(1).decode("ascii")
        except UnicodeDecodeError:
            pass
        else:
            return _require_supported_encoding(
                encoding,
                source="HTML charset",
                data=data,
            )

    return _require_supported_encoding(
        fallback_encoding or "utf-8",
        source="响应编码",
        data=data,
    )


def _require_supported_encoding(encoding: str, *, source: str, data: bytes) -> str:
    try:
        codecs.lookup(encoding)
    except LookupError as exc:
        response_text = data[:ALIASES_SCAN_LIMIT_BYTES].decode("ascii", errors="replace")
        raise FetchError(
            f"别名页面{source}无法识别: {encoding}",
            response_text=_truncate_text(response_text),
        ) from exc
    return encoding


def _parse_aliases_from_keywords(content: str) -> list[str]:
    content = content.strip()
    if not content:
        return []

    title = _extract_title_from_keywords(content)
    if not title:
        raise FetchError(f`keywords 中无法解析标题，{content}`)
    content = _drop_first_keywords_by_title(content, title=title, count=3)

    parts = _split_keywords(content)
    if not parts:
        return []

    try:
        marker_index = parts.index("再漫画漫画")
    except ValueError:
        candidates = parts
    else:
        author_index = marker_index - 1
        if author_index <= 0:
            candidates = []
        else:
            candidates = parts[:author_index]

    seen: set[str] = set()
    aliases: list[str] = []
    for item in candidates:
        if item in seen:
            continue
        seen.add(item)
        aliases.append(item)
    return aliases


_DELIMITERS = (",",)
_FIRST_KEYWORD_SUFFIX = "漫画"


def _extract_title_from_keywords(content: str) -> str | None:
    search_from = 0
    while True:
        idx = content.find(_FIRST_KEYWORD_SUFFIX, search_from)
        if idx < 0:
            return None
        suffix_end = idx + len(_FIRST_KEYWORD_SUFFIX)
        if suffix_end >= len(content):
            return content[:idx] or None
        if content[suffix_end] in _DELIMITERS:
            return content[:idx] or None
        search_from = suffix_end


def _drop_first_keywords_by_title(content: str, *, title: str, count: int) -> str:
    remaining = content
    for _ in range(count):
        remaining = remaining.lstrip()
        remaining = _strip_title_prefix(remaining, title)
        remaining = _drop_until_next_delimiter(remaining)
        if not remaining:
            return ""
    return remaining.lstrip()


def _strip_title_prefix(content: str, title: str) -> str:
    if content.startswith(title):
        return content[len(title) :]
    return content


def _drop_until_next_delimiter(content: str) -> str:
    indexes = [content.find(delim) for delim in _DELIMITERS]
    indexes = [idx for idx in indexes if idx >= 0]
    if not indexes:
        return ""
    next_idx = min(indexes)
    return content[next_idx + 1 :]


def _split_keywords(content: str) -> list[str]:
    if not content:
        return []
    parts = [item.strip() for item in content.split(",")]
    return [item for item in parts if item]


def _safe_text(resp: requests.Response, limit: int = 20_000) -> str:
    try:
        text = resp.text
    except Exception:
        return "<failed to decode response text>"
    if len(text) > limit:
        return text[:limit] + f"... <truncated {len(text) - limit} chars>"
    return text


def _truncate_text(text: str, limit: int = 20_000) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"... <truncated {len(text) - limit} chars>"
