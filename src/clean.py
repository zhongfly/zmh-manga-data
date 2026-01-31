from __future__ import annotations

from typing import Any


CLEAN_VERSION = 2

DROP_KEYS: frozenset[str] = frozenset(
    {
        "chapters",
        "dh_url_links",
        "last_updatetime",
        "last_update_chapter_name",
        "hot_num",
        "hit_num",
        "last_update_chapter_id",
        "uid",
        "subscribe_num",
        "url_links",
        "corner_mark",
        "status",
        "comicNotice",
        "authorNotice",
    }
)


def clean_detail(detail: dict[str, Any]) -> dict[str, Any]:
    if not detail:
        return detail
    cleaned = dict(detail)
    for key in DROP_KEYS:
        cleaned.pop(key, None)
    return cleaned
