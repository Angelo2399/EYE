from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from app.schemas.market import MarketSymbol
from app.services.market_calendar_service import MarketCalendarService


NASDAQ_SAMPLE_HTML = """
<html>
  <body>
    <table>
      <thead>
        <tr>
          <th>Holiday</th>
          <th>Date</th>
          <th>Market Status</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>New Year's Day</td>
          <td>January 1</td>
          <td>Closed</td>
        </tr>
        <tr>
          <td>Early Close</td>
          <td>November 26</td>
          <td>1:00 p.m.</td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
"""

CME_WTI_SAMPLE_HTML = """
<html>
  <body>
    <table>
      <thead>
        <tr>
          <th>Holiday</th>
          <th>Date</th>
          <th>Market Status</th>
          <th>Close Time</th>
          <th>Next Open</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>Independence Day Observed</td>
          <td>July 3</td>
          <td>Closed</td>
          <td></td>
          <td>July 5 18:00</td>
        </tr>
        <tr>
          <td>Thanksgiving</td>
          <td>November 26</td>
          <td>Early Close</td>
          <td>13:30</td>
          <td>November 26 18:00</td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
"""


def _utc(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def test_ndx_returns_closed_holiday_on_good_friday_2026() -> None:
    service = MarketCalendarService()

    result = service.get_market_status(
        asset=MarketSymbol.ndx,
        current_dt=_utc(2026, 4, 3, 15, 0),
        timezone_name="Europe/Madrid",
    )

    assert result["asset"] == "NDX"
    assert result["venue"] == "us_equities_cash"
    assert result["market_status"] == "closed_holiday"
    assert result["is_open_now"] is False
    assert result["is_tradable_now"] is False
    assert result["holiday_name"] == "Good Friday"
    assert "Good Friday" in str(result["official_reason"])
    assert result["next_open_local"] is not None


def test_spx_returns_early_close_status_on_day_after_thanksgiving_2026() -> None:
    service = MarketCalendarService()

    result = service.get_market_status(
        asset=MarketSymbol.spx,
        current_dt=_utc(2026, 11, 27, 16, 30),
        timezone_name="Europe/Madrid",
    )

    assert result["asset"] == "SPX"
    assert result["market_status"] == "early_close"
    assert result["is_open_now"] is True
    assert result["is_tradable_now"] is True
    assert result["holiday_name"] == "Day after Thanksgiving"
    assert "early close" in str(result["official_reason"]).lower()


def test_ndx_returns_closed_weekend_on_saturday() -> None:
    service = MarketCalendarService()

    result = service.get_market_status(
        asset=MarketSymbol.ndx,
        current_dt=_utc(2026, 3, 28, 18, 0),
        timezone_name="Europe/Madrid",
    )

    assert result["market_status"] == "closed_weekend"
    assert result["is_open_now"] is False
    assert result["is_tradable_now"] is False


def test_wti_returns_open_during_regular_session() -> None:
    service = MarketCalendarService()

    result = service.get_market_status(
        asset=MarketSymbol.wti,
        current_dt=_utc(2026, 3, 30, 18, 30),
        timezone_name="Europe/Madrid",
    )

    assert result["asset"] == "WTI"
    assert result["venue"] == "nymex_wti"
    assert result["market_status"] == "open"
    assert result["is_open_now"] is True
    assert result["is_tradable_now"] is True


def test_wti_returns_reduced_hours_during_daily_maintenance_break() -> None:
    service = MarketCalendarService()

    result = service.get_market_status(
        asset=MarketSymbol.wti,
        current_dt=_utc(2026, 3, 30, 21, 30),
        timezone_name="Europe/Madrid",
    )

    assert result["market_status"] == "reduced_hours"
    assert result["is_open_now"] is False
    assert result["is_tradable_now"] is False
    assert "maintenance break" in str(result["official_reason"]).lower()


def test_market_calendar_service_rejects_unsupported_asset() -> None:
    service = MarketCalendarService()

    with pytest.raises(ValueError, match="Unsupported asset"):
        service.get_market_status(
            asset="BTC",
            current_dt=_utc(2026, 3, 30, 18, 30),
            timezone_name="Europe/Madrid",
        )


def test_market_calendar_service_refreshes_us_equities_schedule_from_official_html() -> None:
    cache_dir = Path("data") / f"market-calendar-test-{uuid4().hex}"
    service = MarketCalendarService(
        cache_dir=cache_dir,
        fetcher=lambda url: NASDAQ_SAMPLE_HTML,
        now_provider=lambda: _utc(2027, 1, 1, 12, 0),
    )

    result = service.get_market_status(
        asset=MarketSymbol.ndx,
        current_dt=_utc(2027, 1, 1, 15, 0),
        timezone_name="Europe/Madrid",
    )

    assert result["market_status"] == "closed_holiday"
    assert result["holiday_name"] == "New Year's Day"
    assert "Nasdaq official holiday schedule" in str(result["source"])


def test_market_calendar_service_uses_cached_schedule_when_refresh_fails() -> None:
    cache_dir = Path("data") / f"market-calendar-test-{uuid4().hex}"
    writer_service = MarketCalendarService(
        cache_dir=cache_dir,
        fetcher=lambda url: NASDAQ_SAMPLE_HTML,
        now_provider=lambda: _utc(2027, 1, 1, 12, 0),
    )
    writer_service.warm_reference_calendars(years=[2027])

    reader_service = MarketCalendarService(
        cache_dir=cache_dir,
        fetcher=lambda url: (_ for _ in ()).throw(RuntimeError("network failed")),
        now_provider=lambda: _utc(2027, 1, 1, 13, 0),
    )

    result = reader_service.get_market_status(
        asset=MarketSymbol.spx,
        current_dt=_utc(2027, 1, 1, 15, 0),
        timezone_name="Europe/Madrid",
    )

    assert result["market_status"] == "closed_holiday"
    assert result["holiday_name"] == "New Year's Day"


def test_market_calendar_service_refreshes_wti_schedule_from_official_html() -> None:
    cache_dir = Path("data") / f"market-calendar-test-{uuid4().hex}"
    service = MarketCalendarService(
        cache_dir=cache_dir,
        fetcher=lambda url: CME_WTI_SAMPLE_HTML,
        now_provider=lambda: _utc(2027, 7, 3, 12, 0),
    )

    result = service.get_market_status(
        asset=MarketSymbol.wti,
        current_dt=_utc(2027, 7, 3, 15, 0),
        timezone_name="Europe/Madrid",
    )

    assert result["market_status"] == "closed_holiday"
    assert result["holiday_name"] == "Independence Day Observed"
    assert result["next_open_local"] is not None
    assert "CME/NYMEX official holiday hours" in str(result["source"])


def test_market_calendar_service_uses_cached_wti_schedule_when_refresh_fails() -> None:
    cache_dir = Path("data") / f"market-calendar-test-{uuid4().hex}"
    writer_service = MarketCalendarService(
        cache_dir=cache_dir,
        fetcher=lambda url: CME_WTI_SAMPLE_HTML,
        now_provider=lambda: _utc(2027, 7, 3, 12, 0),
    )
    writer_service.get_market_status(
        asset=MarketSymbol.wti,
        current_dt=_utc(2027, 7, 3, 15, 0),
        timezone_name="Europe/Madrid",
    )

    reader_service = MarketCalendarService(
        cache_dir=cache_dir,
        fetcher=lambda url: (_ for _ in ()).throw(RuntimeError("network failed")),
        now_provider=lambda: _utc(2027, 7, 3, 13, 0),
    )

    result = reader_service.get_market_status(
        asset=MarketSymbol.wti,
        current_dt=_utc(2027, 7, 3, 15, 0),
        timezone_name="Europe/Madrid",
    )

    assert result["market_status"] == "closed_holiday"
    assert result["holiday_name"] == "Independence Day Observed"
