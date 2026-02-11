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

RUN_MODE_FETCH = "fetch"
RUN_MODE_UPDATE_DETAILS = "update_details"
RUN_MODE_UPDATE_ALL = "update_all"
RUN_MODE_UPDATE_ALIASES = "update_aliases"

DEFAULT_AUTO_HISTORY_BATCH_SIZE = 1000
ERROR_RETENTION_DAYS = 30
AUTO_HISTORY_STOP_SLEEP_SECONDS = 60

META_KEY_CLEAN_VERSION = "clean_version"
META_KEY_NEXT_ID = "next_id"
META_KEY_HISTORY_NEXT_ID = "history_next_id"


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
    db_maintenance: bool
    auto_update_history: bool
    history_batch_size: int
    run_mode: str
    aliases_workers: int
    detail_workers: int


def main(argv: list[str] | None = None) -> int:
    cfg = _parse_args(argv)
    store = SqliteStore(cfg.db_path)
    try:
        if cfg.start_id is not None and cfg.run_mode == RUN_MODE_FETCH:
            store.set_next_id(cfg.start_id)
            store.commit()

        if cfg.clean_existing:
            _maybe_clean_existing_data(store)

        if cfg.db_maintenance:
            _run_db_maintenance(store)

        with ExitStack() as stack:
            alias_pool: _AliasPool | None = None
            detail_pool: _DetailPool | None = None

            need_alias_pool = cfg.run_mode in (RUN_MODE_FETCH, RUN_MODE_UPDATE_ALL, RUN_MODE_UPDATE_ALIASES)
            if cfg.run_mode == RUN_MODE_FETCH and cfg.auto_update_history:
                need_alias_pool = True
            need_detail_pool = cfg.run_mode in (RUN_MODE_FETCH, RUN_MODE_UPDATE_DETAILS, RUN_MODE_UPDATE_ALL)
            if cfg.run_mode == RUN_MODE_FETCH and cfg.auto_update_history:
                need_detail_pool = True

            if need_alias_pool:
                alias_pool = stack.enter_context(
                    _AliasPool(cfg.aliases_workers, timeout_seconds=cfg.timeout_seconds)
                )
            if need_detail_pool:
                detail_pool = stack.enter_context(
                    _DetailPool(
                        cfg.detail_workers,
                        base_url=cfg.base_url,
                        api_version=cfg.api_version,
                        timeout_seconds=cfg.timeout_seconds,
                    )
                )

            if cfg.run_mode == RUN_MODE_UPDATE_DETAILS:
                assert detail_pool is not None
                return _update_details(store, detail_pool=detail_pool, cfg=cfg)
            if cfg.run_mode == RUN_MODE_UPDATE_ALL:
                assert detail_pool is not None
                assert alias_pool is not None
                return _update_all(store, detail_pool=detail_pool, alias_pool=alias_pool, cfg=cfg)
            if cfg.run_mode == RUN_MODE_UPDATE_ALIASES:
                assert alias_pool is not None
                return _update_aliases(store, alias_pool=alias_pool, cfg=cfg)

            assert cfg.run_mode == RUN_MODE_FETCH
            assert detail_pool is not None
            assert alias_pool is not None

            if cfg.auto_update_history:
                _auto_update_history_data(store, detail_pool=detail_pool, alias_pool=alias_pool, cfg=cfg)

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
        help="覆盖断点（抓取模式）/历史更新起点（--update-* 与自动历史更新），从指定 id 开始",
    )
    parser.add_argument(
        "--clean-existing",
        action="store_true",
        help="启动时清洗历史数据（默认禁用，仅在清洗版本变更时生效一次）",
    )

    update_group = parser.add_mutually_exclusive_group()
    update_group.add_argument(
        "--update-details",
        action="store_true",
        help="仅更新 comics 表中已有历史数据的详情（不更新 aliases）",
    )
    update_group.add_argument(
        "--update-all",
        action="store_true",
        help="更新 comics 表中已有历史数据的详情与 aliases",
    )
    update_group.add_argument(
        "--update-aliases",
        action="store_true",
        help="仅更新 comics 表中已有历史数据的 aliases（不更新详情）",
    )

    parser.add_argument(
        "--no-db-maintenance",
        action="store_true",
        help=f"启动时跳过数据库维护（清理 {ERROR_RETENTION_DAYS} 天前 errors + optimize/vacuum）",
    )
    parser.add_argument(
        "--no-auto-update-history",
        action="store_true",
        help="启动时跳过自动更新历史数据（默认启用）",
    )
    parser.add_argument(
        "--history-batch-size",
        type=int,
        default=DEFAULT_AUTO_HISTORY_BATCH_SIZE,
        help="自动历史更新每次处理条数（默认 1000；0 表示从起点更新到最后一个 id）",
    )
    parser.add_argument(
        "--aliases-workers",
        type=int,
        default=2,
        help="aliases 并发抓取线程数（默认 2）",
    )
    parser.add_argument(
        "--detail-workers",
        type=int,
        default=2,
        help="漫画详情接口并发抓取线程数（默认 2，独立于 --aliases-workers）",
    )
    args = parser.parse_args(argv)

    if args.history_batch_size < 0:
        parser.error("--history-batch-size 必须 >= 0")
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

    run_mode = RUN_MODE_FETCH
    if args.update_details:
        run_mode = RUN_MODE_UPDATE_DETAILS
    elif args.update_all:
        run_mode = RUN_MODE_UPDATE_ALL
    elif args.update_aliases:
        run_mode = RUN_MODE_UPDATE_ALIASES

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
        db_maintenance=not bool(args.no_db_maintenance),
        auto_update_history=not bool(args.no_auto_update_history),
        history_batch_size=args.history_batch_size,
        run_mode=run_mode,
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
    stored_version = store.get_meta(META_KEY_CLEAN_VERSION)
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

    store.set_meta(META_KEY_CLEAN_VERSION, str(CLEAN_VERSION))
    store.commit()
    _print_line(f"[CLEAN] 完成：扫描 {total} 条，更新 {updated} 条。")


def _run_db_maintenance(store: SqliteStore) -> None:
    deleted = store.purge_errors_older_than_days(ERROR_RETENTION_DAYS)
    store.commit()
    store.pragma_optimize_best_effort()
    if deleted > 0:
        store.vacuum_best_effort()
    _print_line(f"[DB] errors 清理完成：{deleted} 条（保留 {ERROR_RETENTION_DAYS} 天）")


def _read_meta_int(store: SqliteStore, key: str) -> int | None:
    value = store.get_meta(key)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _load_existing_aliases_state(
    store: SqliteStore, *, comic_id: int, json_text: str, stage: str
) -> tuple[bool, object]:
    try:
        existing = json.loads(json_text)
    except json.JSONDecodeError as exc:
        store.add_error(
            ErrorRecord(
                comic_id=comic_id,
                occurred_at=_utc_now_iso(),
                error=f"{stage}本地数据 JSON 解析失败: {exc}",
                status_code=None,
                response_text=_truncate_text(json_text),
            )
        )
        store.commit()
        return False, None

    if not isinstance(existing, dict):
        store.add_error(
            ErrorRecord(
                comic_id=comic_id,
                occurred_at=_utc_now_iso(),
                error=f"{stage}本地数据 JSON 顶层不是对象",
                status_code=None,
                response_text=_truncate_text(json_text),
            )
        )
        store.commit()
        return False, None

    if "aliases" in existing:
        return True, existing["aliases"]
    return False, None


def _load_existing_detail_dict(
    store: SqliteStore, *, comic_id: int, json_text: str, stage: str
) -> dict[str, object] | None:
    try:
        existing = json.loads(json_text)
    except json.JSONDecodeError as exc:
        store.add_error(
            ErrorRecord(
                comic_id=comic_id,
                occurred_at=_utc_now_iso(),
                error=f"{stage}本地数据 JSON 解析失败: {exc}",
                status_code=None,
                response_text=_truncate_text(json_text),
            )
        )
        store.commit()
        return None

    if not isinstance(existing, dict):
        store.add_error(
            ErrorRecord(
                comic_id=comic_id,
                occurred_at=_utc_now_iso(),
                error=f"{stage}本地数据 JSON 顶层不是对象",
                status_code=None,
                response_text=_truncate_text(json_text),
            )
        )
        store.commit()
        return None

    return existing


def _resolve_auto_history_start_id(store: SqliteStore, *, cfg: SyncConfig, min_id: int, max_id: int) -> int:
    if cfg.start_id is not None:
        start_id = cfg.start_id
    else:
        stored = _read_meta_int(store, META_KEY_HISTORY_NEXT_ID)
        start_id = stored if stored is not None else min_id

    if start_id < min_id or start_id > max_id:
        return min_id
    return start_id


def _select_auto_history_rows(
    store: SqliteStore, *, start_id: int, batch_size: int
) -> list[tuple[int, str]]:
    if batch_size == 0:
        return list(store.iter_comics_from_id(start_id))

    rows = list(store.iter_comics_from_id(start_id, limit=batch_size))
    if len(rows) < batch_size:
        remaining = batch_size - len(rows)
        rows.extend(list(store.iter_comics_before_id(start_id, limit=remaining)))
    return rows


def _compute_next_history_pointer(
    *,
    last_processed_id: int,
    min_id: int,
    max_id: int,
    batch_size: int,
    completed_all: bool,
) -> int:
    if batch_size == 0 and completed_all:
        return min_id
    next_id = last_processed_id + 1
    if next_id > max_id:
        return min_id
    return next_id


def _auto_update_history_data(
    store: SqliteStore, *, detail_pool: "_DetailPool", alias_pool: "_AliasPool", cfg: SyncConfig
) -> None:
    min_id = store.get_min_comic_id()
    max_id = store.get_max_comic_id()
    if min_id is None or max_id is None:
        _print_line("[AUTO-HIST] comics 表为空，跳过自动历史更新。")
        return

    start_id = _resolve_auto_history_start_id(store, cfg=cfg, min_id=min_id, max_id=max_id)
    targets = _select_auto_history_rows(store, start_id=start_id, batch_size=cfg.history_batch_size)
    if not targets:
        _print_line("[AUTO-HIST] 未找到可更新记录，跳过自动历史更新。")
        return

    batch_hint = "to-end" if cfg.history_batch_size == 0 else str(cfg.history_batch_size)
    total = len(targets)
    _print_line(f"[AUTO-HIST] 开始：{total} 条（start_id={start_id}, batch={batch_hint}，更新 aliases）")

    consecutive_error_count = 0
    last_processed_id: int | None = None
    processed_count = 0

    alias_futures: dict[int, Future[list[str]]] = {}
    next_alias_prefetch_index = 0

    detail_futures: dict[int, Future[object]] = {}
    next_detail_prefetch_index = 0

    def prefetch_aliases() -> None:
        nonlocal next_alias_prefetch_index
        while len(alias_futures) < cfg.aliases_workers and next_alias_prefetch_index < total:
            cid = targets[next_alias_prefetch_index][0]
            if cid not in alias_futures:
                alias_futures[cid] = alias_pool.submit(cid)
            next_alias_prefetch_index += 1

    def prefetch_details() -> None:
        nonlocal next_detail_prefetch_index
        while len(detail_futures) < cfg.detail_workers and next_detail_prefetch_index < total:
            cid = targets[next_detail_prefetch_index][0]
            if cid not in detail_futures:
                detail_futures[cid] = detail_pool.submit(cid)
            next_detail_prefetch_index += 1

    for i, (comic_id, json_text) in enumerate(targets, start=1):
        processed_count = i
        last_processed_id = comic_id
        prefetch_details()
        prefetch_aliases()

        has_aliases, existing_aliases = _load_existing_aliases_state(
            store,
            comic_id=comic_id,
            json_text=json_text,
            stage="自动历史更新：",
        )

        detail_future = detail_futures.pop(comic_id, None)
        if detail_future is None:
            detail_future = detail_pool.submit(comic_id)
        alias_future = alias_futures.pop(comic_id, None)
        if alias_future is None:
            alias_future = alias_pool.submit(comic_id)

        try:
            result = detail_future.result()
        except FetchError as exc:
            alias_future.cancel()
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
            _print_line(
                f"[AUTO-HIST-ERROR] id={comic_id} {exc} ({consecutive_error_count}/{cfg.max_errors})"
            )
            if consecutive_error_count >= cfg.max_errors:
                _print_line(
                    f"[AUTO-HIST-STOP] 连续错误达到上限，结束自动历史更新，休息 {AUTO_HISTORY_STOP_SLEEP_SECONDS}s。"
                )
                time.sleep(AUTO_HISTORY_STOP_SLEEP_SECONDS)
                break
            _sleep_after_error(consecutive_error_count, cfg.delay_seconds)
            continue

        detail_id = _as_int(result.detail.get("id"))
        if detail_id == 0:
            consecutive_error_count = 0
            alias_future.cancel()
            store.add_error(
                ErrorRecord(
                    comic_id=comic_id,
                    occurred_at=_utc_now_iso(),
                    error="自动历史更新：data.id=0，已跳过更新",
                    status_code=None,
                    response_text=_truncate_text(
                        json.dumps(result.detail, ensure_ascii=False, separators=(",", ":"))
                    ),
                )
            )
            store.commit()
            _print_line(f"[AUTO-HIST-SKIP] id={comic_id} data.id=0 ({i}/{total})")
            _sleep_between_requests(cfg.delay_seconds)
            continue

        cleaned_detail = clean_detail(result.detail)
        title = cleaned_detail.get("title")
        if not isinstance(title, str) or not title.strip():
            consecutive_error_count = 0
            alias_future.cancel()
            store.add_error(
                ErrorRecord(
                    comic_id=comic_id,
                    occurred_at=_utc_now_iso(),
                    error="自动历史更新：数据 title 为空，已跳过更新",
                    status_code=None,
                    response_text=_truncate_text(
                        json.dumps(cleaned_detail, ensure_ascii=False, separators=(",", ":"))
                    ),
                )
            )
            store.commit()
            _print_line(f"[AUTO-HIST-SKIP] id={comic_id} title 为空 ({i}/{total})")
            _sleep_between_requests(cfg.delay_seconds)
            continue

        aliases_failed = False
        write_aliases_key = True
        aliases_value: object = []
        try:
            aliases_value = alias_future.result()
        except FetchError as exc:
            aliases_failed = True
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
            _print_line(
                f"[AUTO-HIST-ALIAS-ERROR] id={comic_id} {exc} ({consecutive_error_count}/{cfg.max_errors})"
            )
            if has_aliases:
                write_aliases_key = True
                aliases_value = existing_aliases
            else:
                write_aliases_key = False
        else:
            consecutive_error_count = 0

        if write_aliases_key:
            cleaned_detail["aliases"] = aliases_value
        else:
            cleaned_detail.pop("aliases", None)

        store.upsert_comic(comic_id, cleaned_detail)
        store.commit()
        _print_line(f"[AUTO-HIST-OK] id={comic_id} ({i}/{total})")

        if aliases_failed:
            if consecutive_error_count >= cfg.max_errors:
                _print_line(
                    f"[AUTO-HIST-STOP] 连续错误达到上限，结束自动历史更新，休息 {AUTO_HISTORY_STOP_SLEEP_SECONDS}s。"
                )
                time.sleep(AUTO_HISTORY_STOP_SLEEP_SECONDS)
                break
            _sleep_after_error(consecutive_error_count, cfg.delay_seconds)
            continue

        _sleep_between_requests(cfg.delay_seconds)

    if last_processed_id is not None:
        completed_all = processed_count == total
        next_pointer = _compute_next_history_pointer(
            last_processed_id=last_processed_id,
            min_id=min_id,
            max_id=max_id,
            batch_size=cfg.history_batch_size,
            completed_all=completed_all,
        )
        store.set_meta(META_KEY_HISTORY_NEXT_ID, str(next_pointer))
        store.commit()
        _print_line(f"[AUTO-HIST] 指针已更新：next_id={next_pointer}")


def _build_update_targets(store: SqliteStore, *, start_id: int | None, stage: str) -> list[tuple[int, bool, object]]:
    targets: list[tuple[int, bool, object]] = []
    for comic_id, json_text in store.iter_comics():
        if start_id is not None and comic_id < start_id:
            continue
        has_aliases, existing_aliases = _load_existing_aliases_state(
            store,
            comic_id=comic_id,
            json_text=json_text,
            stage=stage,
        )
        targets.append((comic_id, has_aliases, existing_aliases))
    return targets


def _update_details(store: SqliteStore, *, detail_pool: "_DetailPool", cfg: SyncConfig) -> int:
    targets = _build_update_targets(store, start_id=cfg.start_id, stage="更新详情：")
    if not targets:
        if cfg.start_id is not None:
            _print_line(f"[UPD-DETAILS] 未找到 id>={cfg.start_id} 的历史数据，无需更新。")
        else:
            _print_line("[UPD-DETAILS] comics 表为空，无需更新。")
        return 0

    total = len(targets)
    start_hint = f"，起始 id={cfg.start_id}" if cfg.start_id is not None else ""
    _print_line(f"[UPD-DETAILS] 开始：{total} 条（不更新 aliases{start_hint}）")

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
            _print_line(f"[UPD-DETAILS-ERROR] id={comic_id} {exc} ({consecutive_error_count}/{cfg.max_errors})")
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
                    error="更新详情：data.id=0，已跳过更新",
                    status_code=None,
                    response_text=_truncate_text(
                        json.dumps(result.detail, ensure_ascii=False, separators=(",", ":"))
                    ),
                )
            )
            store.commit()
            _print_line(f"[UPD-DETAILS-SKIP] id={comic_id} data.id=0 ({i}/{total})")
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
                    error="更新详情：数据 title 为空，已跳过更新",
                    status_code=None,
                    response_text=_truncate_text(
                        json.dumps(cleaned_detail, ensure_ascii=False, separators=(",", ":"))
                    ),
                )
            )
            store.commit()
            _print_line(f"[UPD-DETAILS-SKIP] id={comic_id} title 为空 ({i}/{total})")
            _sleep_between_requests(cfg.delay_seconds)
            continue

        consecutive_error_count = 0
        if has_aliases:
            cleaned_detail["aliases"] = existing_aliases
        else:
            cleaned_detail.pop("aliases", None)

        store.upsert_comic(comic_id, cleaned_detail)
        store.commit()

        _print_line(f"[UPD-DETAILS-OK] id={comic_id} ({i}/{total})")
        _sleep_between_requests(cfg.delay_seconds)

    _print_line(f"[UPD-DETAILS] 完成：更新 {total} 条。")
    return 0


def _update_all(
    store: SqliteStore, *, detail_pool: "_DetailPool", alias_pool: "_AliasPool", cfg: SyncConfig
) -> int:
    targets = _build_update_targets(store, start_id=cfg.start_id, stage="更新全量：")
    if not targets:
        if cfg.start_id is not None:
            _print_line(f"[UPD-ALL] 未找到 id>={cfg.start_id} 的历史数据，无需更新。")
        else:
            _print_line("[UPD-ALL] comics 表为空，无需更新。")
        return 0

    total = len(targets)
    start_hint = f"，起始 id={cfg.start_id}" if cfg.start_id is not None else ""
    _print_line(f"[UPD-ALL] 开始：{total} 条（更新 aliases{start_hint}）")

    consecutive_error_count = 0
    alias_futures: dict[int, Future[list[str]]] = {}
    next_alias_prefetch_index = 0

    detail_futures: dict[int, Future[object]] = {}
    next_detail_prefetch_index = 0

    def prefetch_aliases() -> None:
        nonlocal next_alias_prefetch_index
        while len(alias_futures) < cfg.aliases_workers and next_alias_prefetch_index < total:
            cid = targets[next_alias_prefetch_index][0]
            if cid not in alias_futures:
                alias_futures[cid] = alias_pool.submit(cid)
            next_alias_prefetch_index += 1

    def prefetch_details() -> None:
        nonlocal next_detail_prefetch_index
        while len(detail_futures) < cfg.detail_workers and next_detail_prefetch_index < total:
            cid = targets[next_detail_prefetch_index][0]
            if cid not in detail_futures:
                detail_futures[cid] = detail_pool.submit(cid)
            next_detail_prefetch_index += 1

    for i, (comic_id, has_aliases, existing_aliases) in enumerate(targets, start=1):
        prefetch_details()
        prefetch_aliases()

        detail_future = detail_futures.pop(comic_id, None)
        if detail_future is None:
            detail_future = detail_pool.submit(comic_id)
        alias_future = alias_futures.pop(comic_id, None)
        if alias_future is None:
            alias_future = alias_pool.submit(comic_id)

        try:
            result = detail_future.result()
        except FetchError as exc:
            alias_future.cancel()
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
            _print_line(f"[UPD-ALL-ERROR] id={comic_id} {exc} ({consecutive_error_count}/{cfg.max_errors})")
            if consecutive_error_count >= cfg.max_errors:
                _print_line("[STOP] 连续错误次数达到上限，已停止。")
                return 2
            _sleep_after_error(consecutive_error_count, cfg.delay_seconds)
            continue

        detail_id = _as_int(result.detail.get("id"))
        if detail_id == 0:
            consecutive_error_count = 0
            alias_future.cancel()
            store.add_error(
                ErrorRecord(
                    comic_id=comic_id,
                    occurred_at=_utc_now_iso(),
                    error="更新全量：data.id=0，已跳过更新",
                    status_code=None,
                    response_text=_truncate_text(
                        json.dumps(result.detail, ensure_ascii=False, separators=(",", ":"))
                    ),
                )
            )
            store.commit()
            _print_line(f"[UPD-ALL-SKIP] id={comic_id} data.id=0 ({i}/{total})")
            _sleep_between_requests(cfg.delay_seconds)
            continue

        cleaned_detail = clean_detail(result.detail)
        title = cleaned_detail.get("title")
        if not isinstance(title, str) or not title.strip():
            consecutive_error_count = 0
            alias_future.cancel()
            store.add_error(
                ErrorRecord(
                    comic_id=comic_id,
                    occurred_at=_utc_now_iso(),
                    error="更新全量：数据 title 为空，已跳过更新",
                    status_code=None,
                    response_text=_truncate_text(
                        json.dumps(cleaned_detail, ensure_ascii=False, separators=(",", ":"))
                    ),
                )
            )
            store.commit()
            _print_line(f"[UPD-ALL-SKIP] id={comic_id} title 为空 ({i}/{total})")
            _sleep_between_requests(cfg.delay_seconds)
            continue

        aliases_failed = False
        write_aliases_key = True
        aliases_value: object = []
        try:
            aliases_value = alias_future.result()
        except FetchError as exc:
            aliases_failed = True
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
            _print_line(
                f"[UPD-ALL-ALIAS-ERROR] id={comic_id} {exc} ({consecutive_error_count}/{cfg.max_errors})"
            )
            if has_aliases:
                write_aliases_key = True
                aliases_value = existing_aliases
            else:
                write_aliases_key = False
        else:
            consecutive_error_count = 0

        if write_aliases_key:
            cleaned_detail["aliases"] = aliases_value
        else:
            cleaned_detail.pop("aliases", None)

        store.upsert_comic(comic_id, cleaned_detail)
        store.commit()

        _print_line(f"[UPD-ALL-OK] id={comic_id} ({i}/{total})")
        if aliases_failed:
            if consecutive_error_count >= cfg.max_errors:
                _print_line("[STOP] 连续错误次数达到上限，已停止。")
                return 2
            _sleep_after_error(consecutive_error_count, cfg.delay_seconds)
            continue

        _sleep_between_requests(cfg.delay_seconds)

    _print_line(f"[UPD-ALL] 完成：更新 {total} 条。")
    return 0


def _update_aliases(store: SqliteStore, *, alias_pool: "_AliasPool", cfg: SyncConfig) -> int:
    targets: list[tuple[int, str]] = []
    for comic_id, json_text in store.iter_comics():
        if cfg.start_id is not None and comic_id < cfg.start_id:
            continue
        targets.append((comic_id, json_text))

    if not targets:
        if cfg.start_id is not None:
            _print_line(f"[UPD-ALIASES] 未找到 id>={cfg.start_id} 的历史数据，无需更新。")
        else:
            _print_line("[UPD-ALIASES] comics 表为空，无需更新。")
        return 0

    total = len(targets)
    start_hint = f"，起始 id={cfg.start_id}" if cfg.start_id is not None else ""
    _print_line(f"[UPD-ALIASES] 开始：{total} 条（仅更新 aliases{start_hint}）")

    consecutive_error_count = 0
    alias_futures: dict[int, Future[list[str]]] = {}
    next_alias_prefetch_index = 0

    def prefetch_aliases() -> None:
        nonlocal next_alias_prefetch_index
        while len(alias_futures) < cfg.aliases_workers and next_alias_prefetch_index < total:
            cid = targets[next_alias_prefetch_index][0]
            if cid not in alias_futures:
                alias_futures[cid] = alias_pool.submit(cid)
            next_alias_prefetch_index += 1

    for i, (comic_id, json_text) in enumerate(targets, start=1):
        prefetch_aliases()

        detail = _load_existing_detail_dict(
            store,
            comic_id=comic_id,
            json_text=json_text,
            stage="更新别名：",
        )
        if detail is None:
            consecutive_error_count += 1
            if consecutive_error_count >= cfg.max_errors:
                _print_line("[STOP] 连续错误次数达到上限，已停止。")
                return 2
            _sleep_after_error(consecutive_error_count, cfg.delay_seconds)
            continue

        alias_future = alias_futures.pop(comic_id, None)
        if alias_future is None:
            alias_future = alias_pool.submit(comic_id)

        try:
            aliases = alias_future.result()
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
            _print_line(
                f"[UPD-ALIASES-ERROR] id={comic_id} {exc} ({consecutive_error_count}/{cfg.max_errors})"
            )
            if consecutive_error_count >= cfg.max_errors:
                _print_line("[STOP] 连续错误次数达到上限，已停止。")
                return 2
            _sleep_after_error(consecutive_error_count, cfg.delay_seconds)
            continue

        consecutive_error_count = 0
        detail["aliases"] = aliases
        store.update_comic_json(comic_id, json.dumps(detail, ensure_ascii=False, separators=(",", ":")))
        store.commit()

        _print_line(f"[UPD-ALIASES-OK] id={comic_id} ({i}/{total})")
        _sleep_between_requests(cfg.delay_seconds)

    _print_line(f"[UPD-ALIASES] 完成：更新 {total} 条。")
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
