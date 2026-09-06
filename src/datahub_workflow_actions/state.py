"""Idempotency store: remembers step runs by key so a redelivered event does
not grant access (or post to Slack) twice."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any, Dict, Optional


class InMemoryStateStore:
    def __init__(self) -> None:
        self._seen: Dict[str, Any] = {}

    def seen(self, key: str) -> bool:
        return key in self._seen

    def mark(self, key: str, output: Any = None) -> None:
        self._seen[key] = output

    def get(self, key: str) -> Optional[Any]:
        return self._seen.get(key)


class SqliteStateStore:
    def __init__(self, path: str) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS step_runs (key TEXT PRIMARY KEY, output TEXT, ran_at REAL NOT NULL)"
        )
        self._conn.commit()

    def seen(self, key: str) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT 1 FROM step_runs WHERE key = ?", (key,)).fetchone()
        return row is not None

    def mark(self, key: str, output: Any = None) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO step_runs (key, output, ran_at) VALUES (?, ?, ?)",
                (key, json.dumps(output, default=str), time.time()),
            )
            self._conn.commit()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            row = self._conn.execute("SELECT output FROM step_runs WHERE key = ?", (key,)).fetchone()
        return json.loads(row[0]) if row and row[0] is not None else None

    def close(self) -> None:
        self._conn.close()


def default_state_path() -> str:
    return os.environ.get(
        "WORKFLOW_ACTIONS_STATE_PATH", os.path.join(os.path.expanduser("~"), ".datahub", "workflow-actions-state.db")
    )
