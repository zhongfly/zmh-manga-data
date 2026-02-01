from __future__ import annotations

import argparse
import json
import threading
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests

from .aliases import fetch_aliases
from .clean import CLEAN_VERSION, clean_detail
from .db import ErrorRecord, SqliteStore
from .fetch import FetchError, fetch_comic_detail


RANDOM_SLEEP_MIN_SECONDS = 0.1
RANDOM_SLEEP_MAX_SECONDS = 1.0
ERROR_BACKOFF_BASE_SECONDS = 0.5


@dataclass(frozen=True)
class SyncConfig:
    base_url: str
    api_version: str
    db_path: Path
    timeout_seconds: float
    delay_seconds: float
    max_errors: int
    max_zero_streak: int
    commit_every: int
    start_id: int | None
    clean_existing: bool
    update_history: bool
    aliases_workers: int
    detail_workers: int


def main(argv: list[str] | None = None) -> int:
    cfg = _parse_args(argv)
    store = SqliteStore(cfg.db_path)
    try:
        if cfg.start_id is not None and not cfg.update_history:
            store.set_next_id(cfg.start_id)
            store.commit()

        if cfg.clean_existing:
            _maybe_clean_existing_data(store)

        with ExitStack() as stack:
            alias_pool: _AliasPool | None = None
            if not cfg.update_history:
                alias_pool = stack.enter_context(
                    _AliasPool(cfg.aliases_workers, timeout_seconds=cfg.timeout_seconds)
                )
            detail_pool = stack.enter_context(
                _DetailPool(
                    cfg.detail_workers,
                    base_url=cfg.base_url,
                    api_version=cfg.api_version,
                    timeout_seconds=cfg.timeout_seconds,
                )
            )
            if cfg.update_history:
                return _update_history_data(store, detail_pool=detail_pool, cfg=cfg)

            assert alias_pool is not None
            checkpoint_next_id = store.get_next_id()
            next_request_id = checkpoint_next_id
            consecutive_error_count = 0
            zero_streak = 0
            since_commit = 0

            alias_futures: dict[int, Future[list[str]]] = {}
            next_alias_prefetch_id = next_request_id

            detail_futures: dict[int, Future[object]] = {}
            next_detail_prefetch_id = next_request_id

            def prefetch_aliases(current_id: int) -> None:
                nonlocal next_alias_prefetch_id
                if current_id not in alias_futures:
                    alias_futures[current_id] = alias_pool.submit(current_id)
                while len(alias_futures) < cfg.aliases_workers:
                    cid = next_alias_prefetch_id
                    if cid not in alias_futures:
                        alias_futures[cid] = alias_pool.submit(cid)
                    next_alias_prefetch_id += 1

            def prefetch_details(current_id: int) -> None:
                nonlocal next_detail_prefetch_id
                if current_id not in detail_futures:
                    detail_futures[current_id] = detail_pool.submit(current_id)
                while len(detail_futures) < cfg.detail_workers:
                    cid = next_detail_prefetch_id
                    if cid not in detail_futures:
                        detail_futures[cid] = detail_pool.submit(cid)
                    next_detail_prefetch_id += 1

            while True:
                comic_id = next_request_id
                prefetch_details(comic_id)
                prefetch_aliases(comic_id)

                detail_future = detail_futures.get(comic_id)
                if detail_future is None:
                    detail_future = detail_pool.submit(comic_id)
                    detail_futures[comic_id] = detail_future

                try:
                    result = detail_future.result()
                except FetchError as exc:
                    detail_futures.pop(comic_id, None)
                    alias_future = alias_futures.pop(comic_id, None)
                    if alias_future is not None:
                        alias_future.cancel()
                    consecutive_error_count += 1
                    zero_streak = 0
                    store.add_error(
                        ErrorRecord(
                            comic_id=comic_id,
                            occurred_at=_utc_now_iso(),
                            error=str(exc),
                            status_code=exc.status_code,
                            response_text=exc.response_text,
                        )
                    )
                    store.commit()
                    since_commit = 0
                    _print_line(
                        f"[ERROR] id={comic_id} {exc} ({consecutive_error_count}/{cfg.max_errors})"
                    )
                    if consecutive_error_count >= cfg.max_errors:
                        store.set_next_id(checkpoint_next_id)
                        store.commit()
                        _print_line("[STOP] 连续错误次数达到上限，已停止。")
                        return 2
                    _sleep_after_error(consecutive_error_count, cfg.delay_seconds)
                    continue

                detail_futures.pop(comic_id, None)
                detail_id = _as_int(result.detail.get("id"))
                if detail_id == 0:
                    consecutive_error_count = 0
                    alias_future = alias_futures.pop(comic_id, None)
                    if alias_future is not None:
                        alias_future.cancel()
                    zero_streak += 1
                    _print_line(
                        f"[ZERO] id={comic_id} data.id=0 ({zero_streak}/{cfg.max_zero_streak})"
                    )
                    if zero_streak >= cfg.max_zero_streak:
                        store.set_next_id(checkpoint_next_id)
                        store.commit()
                        _print_line(
                            f"[DONE] 连续 {cfg.max_zero_streak} 次 data.id=0，停止于 id={comic_id}。"
                        )
                        return 0
                    next_request_id = comic_id + 1
                    _sleep_between_requests(cfg.delay_seconds)
                    continue

                cleaned_detail = clean_detail(result.detail)
                title = cleaned_detail.get("title")
                if not isinstance(title, str) or not title.strip():
                    consecutive_error_count = 0
                    zero_streak = 0
                    alias_future = alias_futures.pop(comic_id, None)
                    if alias_future is not None:
                        alias_future.cancel()
                    store.add_error(
                        ErrorRecord(
                            comic_id=comic_id,
                            occurred_at=_utc_now_iso(),
                            error="数据 title 为空，已跳过保存",
                            status_code=None,
                            response_text=_truncate_text(
                                json.dumps(cleaned_detail, ensure_ascii=False, separators=(",", ":"))
                            ),
                        )
                    )
                    checkpoint_next_id = comic_id + 1
                    store.set_next_id(checkpoint_next_id)
                    store.commit()
                    since_commit = 0
                    next_request_id = checkpoint_next_id
                    _print_line(
                        f"[SKIP] id={comic_id} title 为空，已记录 errors 并跳过保存 -> next_id={checkpoint_next_id}"
                    )
                    _sleep_between_requests(cfg.delay_seconds)
                    continue
                while True:
                    alias_future = alias_futures.get(comic_id)
                    if alias_future is None:
                        alias_future = alias_pool.submit(comic_id)
                        alias_futures[comic_id] = alias_future
                    try:
                        aliases = alias_future.result()
                    except FetchError as exc:
                        alias_futures.pop(comic_id, None)
                        consecutive_error_count += 1
                        zero_streak = 0
                        store.add_error(
                            ErrorRecord(
                                comic_id=comic_id,
                                occurred_at=_utc_now_iso(),
                                error=str(exc),
                                status_code=exc.status_code,
                                response_text=exc.response_text,
                            )
                        )
                        store.commit()
                        since_commit = 0
                        _print_line(
                            f"[ERROR] id={comic_id} {exc} ({consecutive_error_count}/{cfg.max_errors})"
                        )
                        if consecutive_error_count >= cfg.max_errors:
                            store.set_next_id(checkpoint_next_id)
                            store.commit()
                            _print_line("[STOP] 连续错误次数达到上限，已停止。")
                            return 2
                        _sleep_after_error(consecutive_error_count, cfg.delay_seconds)
                        alias_futures[comic_id] = alias_pool.submit(comic_id)
                        continue

                    break

                consecutive_error_count = 0
                zero_streak = 0
                alias_futures.pop(comic_id, None)
                cleaned_detail["aliases"] = aliases

                store.upsert_comic(comic_id, cleaned_detail)
                checkpoint_next_id = comic_id + 1
                store.set_next_id(checkpoint_next_id)
                next_request_id = checkpoint_next_id
                since_commit += 1
                if since_commit >= cfg.commit_every:
                    store.commit()
                    since_commit = 0
                if since_commit > 0:
                    store.commit()
                    since_commit = 0

                _print_line(f"[OK] id={comic_id} -> next_id={checkpoint_next_id}")
                _sleep_between_requests(cfg.delay_seconds)
    except KeyboardInterrupt:
        store.commit()
        _print_line("\n[STOP] 收到中断信号，已退出。")
        return 130
    finally:
        store.close()


def _parse_args(argv: list[str] | None) -> SyncConfig:
    parser = argparse.ArgumentParser(description="顺序抓取再漫画 detail 数据并落地保存。")
    parser.add_argument("--db", default="data/zaimanhua.sqlite3", help="SQLite 数据库路径")
    parser.add_argument("--base-url", default="https://m.zaimanhua.com", help="站点 base URL")
    parser.add_argument("--api-version", default="15", help="查询参数 _v 的值")
    parser.add_argument("--timeout", type=float, default=20.0, help="请求超时（秒）")
    parser.add_argument("--delay", type=float, default=0.0, help="每次请求后的额外延迟（秒），会叠加随机休息")
    parser.add_argument("--max-errors", type=int, default=5, help="连续错误次数达到后停止")
    parser.add_argument("--max-zero-streak", type=int, default=20, help="连续命中 data.id=0 达到后停止")
    parser.add_argument("--commit-every", type=int, default=100, help="每 N 条成功记录提交一次")
    parser.add_argument(
        "--start-id",
        type=int,
        default=None,
        help="覆盖断点（抓取模式）/历史更新起点（--update-history），从指定 id 开始",
    )
    parser.add_argument(
        "--clean-existing",
        action="store_true",
        help="启动时清洗历史数据（默认禁用，仅在清洗版本变更时生效一次）",
    )
    parser.add_argument(
        "--update-history",
        action="store_true",
        help="更新 comics 表中已有历史数据（不抓取别名，保留原有 aliases 字段）",
    )
    parser.add_argument(
        "--aliases-workers",
        type=int,
        default=1,
        help="aliases 并发抓取线程数（默认 1）",
    )
    parser.add_argument(
        "--detail-workers",
        type=int,
        default=1,
        help="漫画详情接口并发抓取线程数（默认 1，独立于 --aliases-workers）",
    )
    args = parser.parse_args(argv)

    if args.max_errors < 1:
        parser.error("--max-errors 必须 >= 1")
    if args.max_zero_streak < 1:
        parser.error("--max-zero-streak 必须 >= 1")
    if args.commit_every < 1:
        parser.error("--commit-every 必须 >= 1")
    if args.timeout <= 0:
        parser.error("--timeout 必须 > 0")
    if args.delay < 0:
        parser.error("--delay 必须 >= 0")
    if args.start_id is not None and args.start_id < 1:
        parser.error("--start-id 必须 >= 1")
    if args.aliases_workers < 1:
        parser.error("--aliases-workers 必须 >= 1")
    if args.detail_workers < 1:
        parser.error("--detail-workers 必须 >= 1")

    return SyncConfig(
        base_url=args.base_url,
        api_version=args.api_version,
        db_path=Path(args.db),
        timeout_seconds=args.timeout,
        delay_seconds=args.delay,
        max_errors=args.max_errors,
        max_zero_streak=args.max_zero_streak,
        commit_every=args.commit_every,
        start_id=args.start_id,
        clean_existing=bool(args.clean_existing),
        update_history=bool(args.update_history),
        aliases_workers=args.aliases_workers,
        detail_workers=args.detail_workers,
    )


def _as_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return None
    if isinstance(value, str):
        value = value.strip()
        if value.isdigit():
            return int(value)
        return None
    return None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _print_line(message: str) -> None:
    print(message, flush=True)


def _sleep_between_requests(extra_delay_seconds: float) -> None:
    sleep_seconds = _base_sleep_seconds(extra_delay_seconds)
    time.sleep(sleep_seconds)


def _sleep_after_error(consecutive_errors: int, extra_delay_seconds: float) -> None:
    if consecutive_errors < 1:
        consecutive_errors = 1
    backoff_seconds = ERROR_BACKOFF_BASE_SECONDS * (2 ** (consecutive_errors - 1))
    sleep_seconds = _base_sleep_seconds(extra_delay_seconds) + backoff_seconds
    time.sleep(sleep_seconds)


def _base_sleep_seconds(extra_delay_seconds: float) -> float:
    if extra_delay_seconds < 0:
        extra_delay_seconds = 0.0
    return random.uniform(RANDOM_SLEEP_MIN_SECONDS, RANDOM_SLEEP_MAX_SECONDS) + extra_delay_seconds


def _maybe_clean_existing_data(store: SqliteStore) -> None:
    stored_version = store.get_meta("clean_version")
    try:
        current_version = int(stored_version) if stored_version is not None else 0
    except ValueError:
        current_version = 0

    if current_version >= CLEAN_VERSION:
        return

    total = 0
    updated = 0
    since_commit = 0
    for comic_id, json_text in store.iter_comics():
        total += 1
        try:
            detail = json.loads(json_text)
        except json.JSONDecodeError as exc:
            store.add_error(
                ErrorRecord(
                    comic_id=comic_id,
                    occurred_at=_utc_now_iso(),
                    error=f"本地数据 JSON 解析失败: {exc}",
                    status_code=None,
                    response_text=_truncate_text(json_text),
                )
            )
            continue
        if not isinstance(detail, dict):
            store.add_error(
                ErrorRecord(
                    comic_id=comic_id,
                    occurred_at=_utc_now_iso(),
                    error="本地数据 JSON 顶层不是对象",
                    status_code=None,
                    response_text=_truncate_text(json_text),
                )
            )
            continue

        cleaned = clean_detail(detail)
        if cleaned == detail:
            continue

        store.update_comic_json(
            comic_id,
            json.dumps(cleaned, ensure_ascii=False, separators=(",", ":")),
        )
        updated += 1
        since_commit += 1
        if since_commit >= 500:
            store.commit()
            since_commit = 0

    store.set_meta("clean_version", str(CLEAN_VERSION))
    store.commit()
    _print_line(f"[CLEAN] 完成：扫描 {total} 条，更新 {updated} 条。")


def _update_history_data(store: SqliteStore, *, detail_pool: "_DetailPool", cfg: SyncConfig) -> int:
    targets: list[tuple[int, bool, object]] = []
    for comic_id, json_text in store.iter_comics():
        if cfg.start_id is not None and comic_id < cfg.start_id:
            continue
        try:
            existing = json.loads(json_text)
        except json.JSONDecodeError as exc:
            store.add_error(
                ErrorRecord(
                    comic_id=comic_id,
                    occurred_at=_utc_now_iso(),
                    error=f"历史更新：本地数据 JSON 解析失败: {exc}",
                    status_code=None,
                    response_text=_truncate_text(json_text),
                )
            )
            store.commit()
            targets.append((comic_id, False, None))
            continue

        if isinstance(existing, dict) and "aliases" in existing:
            targets.append((comic_id, True, existing["aliases"]))
        else:
            targets.append((comic_id, False, None))

    if not targets:
        if cfg.start_id is not None:
            _print_line(f"[HIST] 未找到 id>={cfg.start_id} 的历史数据，无需更新。")
        else:
            _print_line("[HIST] comics 表为空，无需更新。")
        return 0

    total = len(targets)
    start_hint = f"，起始 id={cfg.start_id}" if cfg.start_id is not None else ""
    _print_line(f"[HIST] 开始更新历史数据：{total} 条（不更新 aliases{start_hint}）")

    consecutive_error_count = 0
    detail_futures: dict[int, Future[object]] = {}
    next_detail_prefetch_index = 0

    def prefetch_details() -> None:
        nonlocal next_detail_prefetch_index
        while len(detail_futures) < cfg.detail_workers and next_detail_prefetch_index < total:
            cid = targets[next_detail_prefetch_index][0]
            if cid not in detail_futures:
                detail_futures[cid] = detail_pool.submit(cid)
            next_detail_prefetch_index += 1

    for i, (comic_id, has_aliases, existing_aliases) in enumerate(targets, start=1):
        prefetch_details()

        detail_future = detail_futures.pop(comic_id, None)
        if detail_future is None:
            detail_future = detail_pool.submit(comic_id)

        try:
            result = detail_future.result()
        except FetchError as exc:
            consecutive_error_count += 1
            store.add_error(
                ErrorRecord(
                    comic_id=comic_id,
                    occurred_at=_utc_now_iso(),
                    error=str(exc),
                    status_code=exc.status_code,
                    response_text=exc.response_text,
                )
            )
            store.commit()
            _print_line(f"[HIST-ERROR] id={comic_id} {exc} ({consecutive_error_count}/{cfg.max_errors})")
            if consecutive_error_count >= cfg.max_errors:
                _print_line("[STOP] 连续错误次数达到上限，已停止。")
                return 2
            _sleep_after_error(consecutive_error_count, cfg.delay_seconds)
            continue

        detail_id = _as_int(result.detail.get("id"))
        if detail_id == 0:
            consecutive_error_count = 0
            store.add_error(
                ErrorRecord(
                    comic_id=comic_id,
                    occurred_at=_utc_now_iso(),
                    error="历史更新：data.id=0，已跳过更新",
                    status_code=None,
                    response_text=_truncate_text(
                        json.dumps(result.detail, ensure_ascii=False, separators=(",", ":"))
                    ),
                )
            )
            store.commit()
            _print_line(f"[HIST-SKIP] id={comic_id} data.id=0，已跳过更新 ({i}/{total})")
            _sleep_between_requests(cfg.delay_seconds)
            continue

        cleaned_detail = clean_detail(result.detail)
        title = cleaned_detail.get("title")
        if not isinstance(title, str) or not title.strip():
            consecutive_error_count = 0
            store.add_error(
                ErrorRecord(
                    comic_id=comic_id,
                    occurred_at=_utc_now_iso(),
                    error="历史更新：数据 title 为空，已跳过更新",
                    status_code=None,
                    response_text=_truncate_text(
                        json.dumps(cleaned_detail, ensure_ascii=False, separators=(",", ":"))
                    ),
                )
            )
            store.commit()
            _print_line(f"[HIST-SKIP] id={comic_id} title 为空，已跳过更新 ({i}/{total})")
            _sleep_between_requests(cfg.delay_seconds)
            continue

        consecutive_error_count = 0
        if has_aliases:
            cleaned_detail["aliases"] = existing_aliases
        else:
            cleaned_detail.pop("aliases", None)

        store.upsert_comic(comic_id, cleaned_detail)
        store.commit()

        _print_line(f"[HIST-OK] id={comic_id} ({i}/{total})")
        _sleep_between_requests(cfg.delay_seconds)

    _print_line(f"[HIST] 完成：更新 {total} 条。")
    return 0


def _truncate_text(text: str, limit: int = 20_000) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"... <truncated {len(text) - limit} chars>"


class _AliasPool:
    def __init__(self, workers: int, *, timeout_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="aliases")
        self._local = threading.local()
        self._sessions: list[requests.Session] = []
        self._sessions_lock = threading.Lock()

    def __enter__(self) -> "_AliasPool":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def submit(self, comic_id: int) -> Future[list[str]]:
        return self._executor.submit(self._task, comic_id)

    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=True)
        with self._sessions_lock:
            sessions = list(self._sessions)
            self._sessions.clear()
        for session in sessions:
            try:
                session.close()
            except Exception:
                pass

    def _get_session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            with self._sessions_lock:
                self._sessions.append(session)
            self._local.session = session
        return session

    def _task(self, comic_id: int) -> list[str]:
        session = self._get_session()
        result = fetch_aliases(
            session=session,
            comic_id=comic_id,
            timeout_seconds=self._timeout_seconds,
        )
        return result.aliases


class _DetailPool:
    def __init__(self, workers: int, *, base_url: str, api_version: str, timeout_seconds: float) -> None:
        self._base_url = base_url
        self._api_version = api_version
        self._timeout_seconds = timeout_seconds
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="details")
        self._local = threading.local()
        self._sessions: list[requests.Session] = []
        self._sessions_lock = threading.Lock()

    def __enter__(self) -> "_DetailPool":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def submit(self, comic_id: int) -> Future[object]:
        return self._executor.submit(self._task, comic_id)

    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=True)
        with self._sessions_lock:
            sessions = list(self._sessions)
            self._sessions.clear()
        for session in sessions:
            try:
                session.close()
            except Exception:
                pass

    def _get_session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            with self._sessions_lock:
                self._sessions.append(session)
            self._local.session = session
        return session

    def _task(self, comic_id: int) -> object:
        session = self._get_session()
        return fetch_comic_detail(
            session=session,
            base_url=self._base_url,
            comic_id=comic_id,
            api_version=self._api_version,
            timeout_seconds=self._timeout_seconds,
        )
