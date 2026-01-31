from __future__ import annotations

import json
import sqlite3
import time
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


DB_BUSY_TIMEOUT_SECONDS = 60.0
DB_LOCK_RETRY_MAX_ATTEMPTS = 5
DB_LOCK_RETRY_BASE_SECONDS = 0.2


@dataclass(frozen=True)
class ErrorRecord:
    comic_id: int
    occurred_at: str
    error: str
    status_code: int | None = None
    response_text: str | None = None


class SqliteStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._db_path, timeout=DB_BUSY_TIMEOUT_SECONDS)
        self._conn.execute(f"PRAGMA busy_timeout={int(DB_BUSY_TIMEOUT_SECONDS * 1000)};")
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def close(self) -> None:
        self._conn.close()

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS comics (
                id INTEGER PRIMARY KEY,
                json TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                comic_id INTEGER NOT NULL,
                occurred_at TEXT NOT NULL,
                error TEXT NOT NULL,
                status_code INTEGER,
                response_text TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_errors_comic_id ON errors(comic_id);
            """
        )
        self._conn.commit()

    def get_meta(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        value = row[0]
        if isinstance(value, str):
            return value
        return None

    def set_meta(self, key: str, value: str) -> None:
        self._conn.execute(
            """
            INSERT INTO meta(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )

    def get_next_id(self, *, default: int = 4) -> int:
        value = self.get_meta("next_id")
        if value is None:
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def set_next_id(self, next_id: int) -> None:
        self.set_meta("next_id", str(next_id))

    def upsert_comic(self, comic_id: int, detail: dict[str, Any]) -> None:
        json_text = json.dumps(detail, ensure_ascii=False, separators=(",", ":"))
        self._conn.execute(
            """
            INSERT INTO comics(id, json, fetched_at) VALUES(?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET json = excluded.json, fetched_at = excluded.fetched_at
            """,
            (comic_id, json_text, _utc_now_iso()),
        )

    def add_error(self, record: ErrorRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO errors(comic_id, occurred_at, error, status_code, response_text)
            VALUES(?, ?, ?, ?, ?)
            """,
            (
                record.comic_id,
                record.occurred_at,
                record.error,
                record.status_code,
                record.response_text,
            ),
        )

    def iter_comics(self) -> Iterable[tuple[int, str]]:
        cursor = self._conn.execute("SELECT id, json FROM comics ORDER BY id")
        for row in cursor:
            yield int(row[0]), str(row[1])

    def update_comic_json(self, comic_id: int, json_text: str) -> None:
        self._conn.execute(
            "UPDATE comics SET json = ? WHERE id = ?",
            (json_text, comic_id),
        )

    def get_max_comic_id(self) -> int | None:
        row = self._conn.execute("SELECT MAX(id) FROM comics").fetchone()
        if row is None:
            return None
        value = row[0]
        if value is None:
            return None
        return int(value)

    def commit(self) -> None:
        for attempt in range(1, DB_LOCK_RETRY_MAX_ATTEMPTS + 1):
            try:
                self._conn.commit()
                return
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if "database is locked" not in message and "database is busy" not in message:
                    raise
                if attempt >= DB_LOCK_RETRY_MAX_ATTEMPTS:
                    raise
                sleep_seconds = DB_LOCK_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
                sleep_seconds += random.uniform(0.0, 0.05)
                time.sleep(sleep_seconds)
