from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
from uuid import uuid4

import pytest

from app.repositories.market_event_repository import MarketEventRepository
from app.schemas.market_intelligence import (
    EventOutcomeFeedback,
    IntelligenceDirection,
    IntelligenceImportance,
    IntelligenceSourceType,
    MarketEventType,
    StructuredMarketEvent,
)


def _build_test_db_path() -> Path:
    artifacts_dir = Path(__file__).resolve().parent / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    return artifacts_dir / f"test_eye_market_events_{uuid4().hex}.db"


def _cleanup_db_file(db_path: Path) -> None:
    with suppress(PermissionError, FileNotFoundError):
        db_path.unlink()


def _build_event(*, event_id: str = "dedup-test-001") -> StructuredMarketEvent:
    return StructuredMarketEvent(
        event_id=event_id,
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        event_type=MarketEventType.headline,
        source_type=IntelligenceSourceType.news,
        source_name="Dedup Test",
        direction=IntelligenceDirection.bullish,
        urgency=IntelligenceImportance.high,
        confidence_pct=80.0,
        decay_minutes=120,
        scenario_change=True,
        title="Same event twice",
        summary="This should not duplicate.",
        raw_text="Repeated market event.",
        occurred_at_utc="2026-04-25T10:00:00+00:00",
        detected_at_utc="2026-04-25T10:01:00+00:00",
        tags=["macro", "headline"],
        market_context={"session": "open", "volatility_regime": "elevated"},
        event_score=81.0,
    )


def test_save_event_returns_existing_id_for_duplicate_event() -> None:
    db_path = _build_test_db_path()

    try:
        repository = MarketEventRepository(db_path=db_path)
        event = _build_event()

        first_id = repository.save_event(event)
        second_id = repository.save_event(event)
        rows = [
            row
            for row in repository.list_recent_events(limit=20)
            if row["event_id"] == event.event_id
        ]

        assert first_id == 1
        assert second_id == first_id
        assert len(rows) == 1
        assert rows[0]["title"] == "Same event twice"
        assert rows[0]["scenario_change"] is True
        assert rows[0]["tags"] == ["macro", "headline"]
        assert rows[0]["market_context"] == {
            "session": "open",
            "volatility_regime": "elevated",
        }
    finally:
        _cleanup_db_file(db_path)


def test_repository_migrates_legacy_duplicate_rows_before_unique_index() -> None:
    db_path = _build_test_db_path()

    try:
        with sqlite3.connect(db_path) as connection:
            connection.execute(
                """
                CREATE TABLE market_events (
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
            connection.executemany(
                """
                INSERT INTO market_events (
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
                [
                    (
                        "2026-04-25T10:00:00+00:00",
                        "legacy-dup-001",
                        "Nasdaq 100",
                        "NDX",
                        "1h",
                        "headline",
                        "news",
                        "Legacy Feed",
                        None,
                        "bullish",
                        "high",
                        80.0,
                        120,
                        1,
                        "Original copy",
                        "Keep the first row.",
                        "original",
                        "2026-04-25T09:59:00+00:00",
                        "2026-04-25T10:00:00+00:00",
                        "[\"macro\"]",
                        "{\"session\": \"open\"}",
                        81.0,
                    ),
                    (
                        "2026-04-25T10:05:00+00:00",
                        "legacy-dup-001",
                        "Nasdaq 100",
                        "NDX",
                        "1h",
                        "headline",
                        "news",
                        "Legacy Feed",
                        None,
                        "bullish",
                        "high",
                        79.0,
                        120,
                        1,
                        "Duplicate copy",
                        "This row should be removed.",
                        "duplicate",
                        "2026-04-25T10:04:00+00:00",
                        "2026-04-25T10:05:00+00:00",
                        "[\"macro\"]",
                        "{\"session\": \"open\"}",
                        80.0,
                    ),
                ],
            )
            connection.commit()

        repository = MarketEventRepository(db_path=db_path)
        rows = [
            row
            for row in repository.list_recent_events(limit=20)
            if row["event_id"] == "legacy-dup-001"
        ]

        with sqlite3.connect(db_path) as connection:
            index_rows = connection.execute("PRAGMA index_list(market_events)").fetchall()

        assert len(rows) == 1
        assert rows[0]["title"] == "Original copy"
        assert any(
            row[1] == "idx_market_events_event_id_unique" and row[2] == 1
            for row in index_rows
        )

        duplicate_id = repository.save_event(_build_event(event_id="legacy-dup-001"))

        assert duplicate_id == rows[0]["id"]
    finally:
        _cleanup_db_file(db_path)


def test_save_feedback_and_list_recent_feedback() -> None:
    db_path = _build_test_db_path()

    try:
        repository = MarketEventRepository(db_path=db_path)
        event = _build_event(event_id="feedback-test-001")
        repository.save_event(event)

        saved_id = repository.save_feedback(
            EventOutcomeFeedback(
                event_id=event.event_id,
                asset=event.asset,
                symbol=event.symbol,
                observed_after_5m=0.2,
                observed_after_30m=0.8,
                observed_after_2h=1.4,
                session_close_outcome=2.1,
                event_score=event.event_score,
                notes="Follow-through remained constructive.",
            )
        )
        rows = repository.list_recent_feedback(limit=10)

        assert saved_id == 1
        assert len(rows) == 1
        assert rows[0]["event_id"] == event.event_id
        assert rows[0]["notes"] == "Follow-through remained constructive."
    finally:
        _cleanup_db_file(db_path)


def test_delete_old_events_and_feedback_respects_retention_windows() -> None:
    db_path = _build_test_db_path()

    try:
        repository = MarketEventRepository(db_path=db_path)

        old_event = _build_event(event_id="old-event-001")
        recent_event = _build_event(event_id="recent-event-001")
        repository.save_event(old_event)
        repository.save_event(recent_event)

        repository.save_feedback(
            EventOutcomeFeedback(
                event_id=old_event.event_id,
                asset=old_event.asset,
                symbol=old_event.symbol,
                observed_after_5m=0.5,
                observed_after_30m=1.0,
                observed_after_2h=1.5,
                session_close_outcome=2.0,
                event_score=70.0,
                notes="Old feedback.",
            )
        )
        repository.save_feedback(
            EventOutcomeFeedback(
                event_id=recent_event.event_id,
                asset=recent_event.asset,
                symbol=recent_event.symbol,
                observed_after_5m=0.6,
                observed_after_30m=1.2,
                observed_after_2h=1.8,
                session_close_outcome=2.4,
                event_score=72.0,
                notes="Recent feedback.",
            )
        )

        old_event_created_at = (
            datetime.now(timezone.utc) - timedelta(days=120)
        ).isoformat()
        old_feedback_created_at = (
            datetime.now(timezone.utc) - timedelta(days=200)
        ).isoformat()

        with sqlite3.connect(db_path) as connection:
            connection.execute(
                """
                UPDATE market_events
                SET created_at_utc = ?
                WHERE event_id = ?
                """,
                (old_event_created_at, old_event.event_id),
            )
            connection.execute(
                """
                UPDATE event_outcome_feedback
                SET created_at_utc = ?
                WHERE event_id = ?
                """,
                (old_feedback_created_at, old_event.event_id),
            )
            connection.commit()

        deleted_events = repository.delete_events_older_than(days=90)
        deleted_feedback = repository.delete_feedback_older_than(days=180)
        remaining_events = repository.list_recent_events(limit=10)
        remaining_feedback = repository.list_recent_feedback(limit=10)

        assert deleted_events == 1
        assert deleted_feedback == 1
        assert [row["event_id"] for row in remaining_events] == ["recent-event-001"]
        assert [row["event_id"] for row in remaining_feedback] == ["recent-event-001"]
    finally:
        _cleanup_db_file(db_path)


def test_delete_old_rows_rejects_invalid_days() -> None:
    db_path = _build_test_db_path()

    try:
        repository = MarketEventRepository(db_path=db_path)

        with pytest.raises(ValueError, match="days must be greater than 0"):
            repository.delete_events_older_than(days=0)

        with pytest.raises(ValueError, match="days must be greater than 0"):
            repository.delete_feedback_older_than(days=0)
    finally:
        _cleanup_db_file(db_path)


def test_list_recent_events_rejects_invalid_limit() -> None:
    db_path = _build_test_db_path()

    try:
        repository = MarketEventRepository(db_path=db_path)

        with pytest.raises(ValueError, match="limit must be greater than 0"):
            repository.list_recent_events(limit=0)
    finally:
        _cleanup_db_file(db_path)
