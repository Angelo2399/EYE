from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.core.config import get_settings
from app.schemas.market_intelligence import (
    EventOutcomeFeedback,
    StructuredMarketEvent,
)


class MarketEventRepository:
    def __init__(self, db_path: Path | None = None) -> None:
        settings = get_settings()
        self.db_path = Path(db_path) if db_path is not None else settings.sqlite_db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_database()

    def save_event(self, event: StructuredMarketEvent) -> int:
        event_data = event.model_dump(mode="json")
        created_at_utc = datetime.now(timezone.utc).isoformat()

        with sqlite3.connect(self.db_path) as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO market_events (
                    created_at_utc,
                    event_id,
                    asset,
                    symbol,
                    timeframe,
                    event_type,
                    source_type,
                    source_name,
                    source_url,
                    direction,
                    urgency,
                    confidence_pct,
                    decay_minutes,
                    scenario_change,
                    title,
                    summary,
                    raw_text,
                    occurred_at_utc,
                    detected_at_utc,
                    tags_json,
                    market_context_json,
                    event_score
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at_utc,
                    event_data["event_id"],
                    event_data["asset"],
                    event_data["symbol"],
                    event_data["timeframe"],
                    event_data["event_type"],
                    event_data["source_type"],
                    event_data["source_name"],
                    event_data["source_url"],
                    event_data["direction"],
                    event_data["urgency"],
                    event_data["confidence_pct"],
                    event_data["decay_minutes"],
                    int(bool(event_data["scenario_change"])),
                    event_data["title"],
                    event_data["summary"],
                    event_data["raw_text"],
                    event_data["occurred_at_utc"],
                    event_data["detected_at_utc"],
                    json.dumps(event_data["tags"]),
                    json.dumps(event_data["market_context"]),
                    event_data["event_score"],
                ),
            )
            connection.commit()

            if cursor.rowcount == 1 and cursor.lastrowid is not None:
                return int(cursor.lastrowid)

            existing_row = connection.execute(
                "SELECT id FROM market_events WHERE event_id = ?",
                (event_data["event_id"],),
            ).fetchone()
            if existing_row is None:
                raise ValueError("Failed to persist or retrieve market event.")
            return int(existing_row[0])

    def save_feedback(self, feedback: EventOutcomeFeedback) -> int:
        feedback_data = feedback.model_dump(mode="json")
        created_at_utc = datetime.now(timezone.utc).isoformat()

        with sqlite3.connect(self.db_path) as connection:
            cursor = connection.execute(
                """
                INSERT INTO event_outcome_feedback (
                    created_at_utc,
                    event_id,
                    asset,
                    symbol,
                    observed_after_5m,
                    observed_after_30m,
                    observed_after_2h,
                    session_close_outcome,
                    event_score,
                    notes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at_utc,
                    feedback_data["event_id"],
                    feedback_data["asset"],
                    feedback_data["symbol"],
                    feedback_data["observed_after_5m"],
                    feedback_data["observed_after_30m"],
                    feedback_data["observed_after_2h"],
                    feedback_data["session_close_outcome"],
                    feedback_data["event_score"],
                    feedback_data["notes"],
                ),
            )
            connection.commit()
            return int(cursor.lastrowid)

    def list_recent_events(self, limit: int = 50) -> list[dict[str, object]]:
        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        with sqlite3.connect(self.db_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT
                    id,
                    created_at_utc,
                    event_id,
                    asset,
                    symbol,
                    timeframe,
                    event_type,
                    source_type,
                    source_name,
                    source_url,
                    direction,
                    urgency,
                    confidence_pct,
                    decay_minutes,
                    scenario_change,
                    title,
                    summary,
                    raw_text,
                    occurred_at_utc,
                    detected_at_utc,
                    tags_json,
                    market_context_json,
                    event_score
                FROM market_events
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        results: list[dict[str, object]] = [dict(row) for row in rows]
        for row in results:
            row["scenario_change"] = bool(row["scenario_change"])
            row["tags"] = json.loads(row.pop("tags_json") or "[]")
            row["market_context"] = json.loads(row.pop("market_context_json") or "{}")
        return results

    def list_recent_feedback(self, limit: int = 50) -> list[dict[str, object]]:
        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        with sqlite3.connect(self.db_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT
                    id,
                    created_at_utc,
                    event_id,
                    asset,
                    symbol,
                    observed_after_5m,
                    observed_after_30m,
                    observed_after_2h,
                    session_close_outcome,
                    event_score,
                    notes
                FROM event_outcome_feedback
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [dict(row) for row in rows]

    def list_events_without_feedback(self, limit: int = 50) -> list[dict[str, object]]:
        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        with sqlite3.connect(self.db_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT
                    e.id,
                    e.created_at_utc,
                    e.event_id,
                    e.asset,
                    e.symbol,
                    e.timeframe,
                    e.event_type,
                    e.source_type,
                    e.source_name,
                    e.source_url,
                    e.direction,
                    e.urgency,
                    e.confidence_pct,
                    e.decay_minutes,
                    e.scenario_change,
                    e.title,
                    e.summary,
                    e.raw_text,
                    e.occurred_at_utc,
                    e.detected_at_utc,
                    e.tags_json,
                    e.market_context_json,
                    e.event_score
                FROM market_events e
                LEFT JOIN event_outcome_feedback f
                    ON f.event_id = e.event_id
                WHERE f.event_id IS NULL
                ORDER BY e.id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        results: list[dict[str, object]] = [dict(row) for row in rows]
        for row in results:
            row["scenario_change"] = bool(row["scenario_change"])
            row["tags"] = json.loads(row.pop("tags_json") or "[]")
            row["market_context"] = json.loads(row.pop("market_context_json") or "{}")
        return results

    def delete_events_older_than(self, days: int = 90) -> int:
        if days <= 0:
            raise ValueError("days must be greater than 0")

        cutoff_utc = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        with sqlite3.connect(self.db_path) as connection:
            cursor = connection.execute(
                """
                DELETE FROM market_events
                WHERE datetime(created_at_utc) < datetime(?)
                """,
                (cutoff_utc,),
            )
            connection.commit()
            return int(cursor.rowcount if cursor.rowcount != -1 else 0)

    def delete_feedback_older_than(self, days: int = 180) -> int:
        if days <= 0:
            raise ValueError("days must be greater than 0")

        cutoff_utc = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        with sqlite3.connect(self.db_path) as connection:
            cursor = connection.execute(
                """
                DELETE FROM event_outcome_feedback
                WHERE datetime(created_at_utc) < datetime(?)
                """,
                (cutoff_utc,),
            )
            connection.commit()
            return int(cursor.rowcount if cursor.rowcount != -1 else 0)

    def _initialize_database(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS market_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at_utc TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT,
                    event_type TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    source_url TEXT,
                    direction TEXT NOT NULL,
                    urgency TEXT NOT NULL,
                    confidence_pct REAL NOT NULL,
                    decay_minutes INTEGER,
                    scenario_change INTEGER NOT NULL DEFAULT 0,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    raw_text TEXT,
                    occurred_at_utc TEXT,
                    detected_at_utc TEXT,
                    tags_json TEXT NOT NULL,
                    market_context_json TEXT NOT NULL,
                    event_score REAL NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS event_outcome_feedback (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at_utc TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    observed_after_5m REAL,
                    observed_after_30m REAL,
                    observed_after_2h REAL,
                    session_close_outcome REAL,
                    event_score REAL NOT NULL,
                    notes TEXT NOT NULL DEFAULT ''
                )
                """
            )
            self._migrate_schema_if_needed(connection)
            connection.commit()

    def _migrate_schema_if_needed(self, connection: sqlite3.Connection) -> None:
        # Keep the earliest copy of each legacy event before enforcing uniqueness.
        connection.execute(
            """
            DELETE FROM market_events
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM market_events
                GROUP BY event_id
            )
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_market_events_event_id_unique
            ON market_events(event_id)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_events_symbol_created_at
            ON market_events(symbol, created_at_utc)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_event_feedback_event_id
            ON event_outcome_feedback(event_id)
            """
        )
