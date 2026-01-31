from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import requests


DEFAULT_HEADERS: dict[str, str] = {
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Authorization": "Bearer undefined",
    "Cache-Control": "no-cache",
    "DNT": "1",
    "Platform": "h5",
    "Pragma": "no-cache",
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    ),
    "X-Client-ID": "undefined",
    "content-type": "application/json",
}


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    detail: dict[str, Any]


class FetchError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, response_text: str | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


def fetch_comic_detail(
    *,
    session: requests.Session,
    base_url: str,
    comic_id: int,
    api_version: str,
    timeout_seconds: float,
) -> FetchResult:
    base_url = base_url.rstrip("/")
    url = f"{base_url}/api/app/v1/comic/detail/{comic_id}?_v={api_version}"
    headers = dict(DEFAULT_HEADERS)
    headers["Referer"] = f"{base_url}/pages/comic/detail?id={comic_id}"

    try:
        resp = session.get(url, headers=headers, timeout=timeout_seconds)
    except requests.RequestException as exc:
        raise FetchError(f"HTTP 请求失败: {exc!r}") from exc

    if resp.status_code != 200:
        raise FetchError(
            f"HTTP 状态码异常: {resp.status_code}",
            status_code=resp.status_code,
            response_text=_safe_text(resp),
        )

    try:
        payload = resp.json()
    except (ValueError, json.JSONDecodeError) as exc:
        raise FetchError(
            f"响应不是合法 JSON: {exc}",
            status_code=resp.status_code,
            response_text=_safe_text(resp),
        ) from exc

    detail = _extract_detail(payload)
    return FetchResult(status_code=resp.status_code, detail=detail)


def _extract_detail(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise FetchError("响应 JSON 顶层不是对象")
    outer = payload.get("data")
    if not isinstance(outer, dict):
        raise FetchError("响应 JSON 缺少 data 对象")
    inner = outer.get("data")
    if not isinstance(inner, dict):
        raise FetchError("响应 JSON 缺少 data.data 对象")
    return inner


def _safe_text(resp: requests.Response, limit: int = 20_000) -> str:
    try:
        text = resp.text
    except Exception:
        return "<failed to decode response text>"
    if len(text) > limit:
        return text[:limit] + f"... <truncated {len(text) - limit} chars>"
    return text
