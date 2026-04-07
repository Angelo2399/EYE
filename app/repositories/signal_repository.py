from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from app.core.config import get_settings
from app.schemas.signal import SignalRequest, SignalResponse


class SignalRepository:
    def __init__(self, db_path: Path | None = None) -> None:
        settings = get_settings()
        self.db_path = Path(db_path) if db_path is not None else settings.sqlite_db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_database()

    def save_signal(self, request: SignalRequest, response: SignalResponse) -> int:
        response_data = response.model_dump(mode="json")
        created_at_utc = datetime.now(timezone.utc).isoformat()

        with sqlite3.connect(self.db_path) as connection:
            cursor = connection.execute(
                """
                INSERT INTO signals (
                    created_at_utc,
                    symbol,
                    timeframe,
                    asset,
                    action,
                    entry_min,
                    entry_max,
                    entry_window,
                    expected_holding,
                    hard_exit_time,
                    close_by_session_end,
                    stop_loss,
                    take_profit_1,
                    take_profit_2,
                    risk_reward,
                    favorable_move_pct,
                    tp1_hit_pct,
                    stop_hit_first_pct,
                    model_confidence_pct,
                    confidence_label,
                    day_context_label,
                    day_context_bias,
                    day_context_confidence_pct,
                    explanation
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at_utc,
                    request.symbol.value,
                    request.timeframe.value,
                    response_data["asset"],
                    response_data["action"],
                    response_data["entry_min"],
                    response_data["entry_max"],
                    response_data["entry_window"],
                    response_data["expected_holding"],
                    response_data["hard_exit_time"],
                    response_data["close_by_session_end"],
                    response_data["stop_loss"],
                    response_data["take_profit_1"],
                    response_data["take_profit_2"],
                    response_data["risk_reward"],
                    response_data["favorable_move_pct"],
                    response_data["tp1_hit_pct"],
                    response_data["stop_hit_first_pct"],
                    response_data["model_confidence_pct"],
                    response_data["confidence_label"],
                    response_data["day_context_label"],
                    response_data["day_context_bias"],
                    response_data["day_context_confidence_pct"],
                    response_data["explanation"],
                ),
            )
            connection.commit()
            return int(cursor.lastrowid)

    def list_recent_signals(self, limit: int = 50) -> list[dict[str, object]]:
        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        with sqlite3.connect(self.db_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT
                    id,
                    created_at_utc,
                    symbol,
                    timeframe,
                    asset,
                    action,
                    entry_min,
                    entry_max,
                    entry_window,
                    expected_holding,
                    hard_exit_time,
                    close_by_session_end,
                    stop_loss,
                    take_profit_1,
                    take_profit_2,
                    risk_reward,
                    favorable_move_pct,
                    tp1_hit_pct,
                    stop_hit_first_pct,
                    model_confidence_pct,
                    confidence_label,
                    day_context_label,
                    day_context_bias,
                    day_context_confidence_pct,
                    explanation
                FROM signals
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        results: list[dict[str, object]] = [dict(row) for row in rows]
        for row in results:
            row["close_by_session_end"] = bool(row["close_by_session_end"])
        return results

    def _initialize_database(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at_utc TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    action TEXT NOT NULL,
                    entry_min REAL,
                    entry_max REAL,
                    entry_window TEXT,
                    expected_holding TEXT,
                    hard_exit_time TEXT,
                    close_by_session_end INTEGER NOT NULL DEFAULT 1,
                    stop_loss REAL,
                    take_profit_1 REAL,
                    take_profit_2 REAL,
                    risk_reward REAL,
                    favorable_move_pct REAL,
                    tp1_hit_pct REAL,
                    stop_hit_first_pct REAL,
                    model_confidence_pct REAL,
                    confidence_label TEXT,
                    day_context_label TEXT,
                    day_context_bias TEXT,
                    day_context_confidence_pct REAL,
                    explanation TEXT NOT NULL
                )
                """
            )
            self._migrate_schema_if_needed(connection)
            connection.commit()

    def _migrate_schema_if_needed(self, connection: sqlite3.Connection) -> None:
        existing_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(signals)").fetchall()
        }
        required_columns = {
            "entry_window": "TEXT",
            "expected_holding": "TEXT",
            "hard_exit_time": "TEXT",
            "close_by_session_end": "INTEGER NOT NULL DEFAULT 1",
            "day_context_label": "TEXT",
            "day_context_bias": "TEXT",
            "day_context_confidence_pct": "REAL",
        }

        for column_name, sql_type in required_columns.items():
            if column_name not in existing_columns:
                connection.execute(
                    f"ALTER TABLE signals ADD COLUMN {column_name} {sql_type}"
                )
