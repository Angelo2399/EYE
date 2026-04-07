from __future__ import annotations

from app.schemas.market import MarketSymbol, MarketTimeframe
from app.services.asset_update_runner_service import AssetUpdateRunnerService


class FakeSignalService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def generate_signal(self, payload, timezone_name=None):
        self.calls.append(
            {
                "symbol": payload.symbol.value,
                "timeframe": payload.timeframe.value,
                "timezone_name": timezone_name,
            }
        )

        if payload.symbol == MarketSymbol.wti:
            raise RuntimeError("WTI feed unavailable.")

        return {
            "asset": payload.symbol.value,
            "action": "ok",
        }


class FakeMarketCalendarService:
    def __init__(self, tradable_assets: set[str] | None = None) -> None:
        self.tradable_assets = set(tradable_assets or set())
        self.calls: list[dict[str, object]] = []

    def get_market_status(self, *, asset, current_dt=None, timezone_name="Europe/Madrid"):
        asset_value = asset.value if hasattr(asset, "value") else str(asset)
        self.calls.append(
            {
                "asset": asset_value,
                "timezone_name": timezone_name,
            }
        )

        if asset_value in self.tradable_assets:
            return {
                "asset": asset_value,
                "market_status": "open",
                "is_tradable_now": True,
                "official_reason": "Regular session open.",
                "next_open_local": None,
            }

        return {
            "asset": asset_value,
            "market_status": "closed_holiday",
            "is_tradable_now": False,
            "official_reason": f"{asset_value} market closed for holiday.",
            "next_open_local": "2026-04-06T09:30:00+02:00",
        }


def test_run_asset_updates_collects_results_for_tradable_assets() -> None:
    signal_service = FakeSignalService()
    calendar_service = FakeMarketCalendarService(
        tradable_assets={"NDX", "SPX", "BTC"},
    )
    service = AssetUpdateRunnerService(
        signal_service=signal_service,
        market_calendar_service=calendar_service,
    )

    result = service.run_asset_updates(
        assets=["NDX", "SPX", "BTC"],
        timeframe="1h",
        timezone_name="Europe/London",
    )

    assert signal_service.calls == [
        {
            "symbol": "NDX",
            "timeframe": "1h",
            "timezone_name": "Europe/London",
        },
        {
            "symbol": "SPX",
            "timeframe": "1h",
            "timezone_name": "Europe/London",
        },
        {
            "symbol": "BTC",
            "timeframe": "1h",
            "timezone_name": "Europe/London",
        },
    ]
    assert calendar_service.calls == [
        {"asset": "NDX", "timezone_name": "Europe/London"},
        {"asset": "SPX", "timezone_name": "Europe/London"},
        {"asset": "BTC", "timezone_name": "Europe/London"},
    ]
    assert result["errors"] == []
    assert result["results"] == [
        {
            "asset": "NDX",
            "timeframe": "1h",
            "response": {
                "asset": "NDX",
                "action": "ok",
            },
        },
        {
            "asset": "SPX",
            "timeframe": "1h",
            "response": {
                "asset": "SPX",
                "action": "ok",
            },
        },
        {
            "asset": "BTC",
            "timeframe": "1h",
            "response": {
                "asset": "BTC",
                "action": "ok",
            },
        },
    ]


def test_run_asset_updates_separates_errors_from_successful_results() -> None:
    signal_service = FakeSignalService()
    calendar_service = FakeMarketCalendarService(
        tradable_assets={"NDX", "WTI"},
    )
    service = AssetUpdateRunnerService(
        signal_service=signal_service,
        market_calendar_service=calendar_service,
    )

    result = service.run_asset_updates(
        assets=[MarketSymbol.ndx, MarketSymbol.wti],
        timeframe=MarketTimeframe.h1,
        timezone_name="Europe/London",
    )

    assert result["results"] == [
        {
            "asset": "NDX",
            "timeframe": "1h",
            "response": {
                "asset": "NDX",
                "action": "ok",
            },
        }
    ]
    assert result["errors"] == [
        {
            "asset": "WTI",
            "timeframe": "1h",
            "error": "WTI feed unavailable.",
        }
    ]


def test_run_asset_updates_uses_default_asset_cycle() -> None:
    signal_service = FakeSignalService()
    calendar_service = FakeMarketCalendarService(
        tradable_assets={"NDX", "SPX", "WTI"},
    )
    service = AssetUpdateRunnerService(
        signal_service=signal_service,
        market_calendar_service=calendar_service,
    )

    service.run_asset_updates(
        timeframe="1h",
        timezone_name="Europe/London",
    )

    assert [call["symbol"] for call in signal_service.calls] == [
        "NDX",
        "SPX",
        "WTI",
    ]


def test_run_asset_updates_skips_closed_assets_with_calendar_reason() -> None:
    signal_service = FakeSignalService()
    calendar_service = FakeMarketCalendarService(
        tradable_assets={"SPX"},
    )
    service = AssetUpdateRunnerService(
        signal_service=signal_service,
        market_calendar_service=calendar_service,
    )

    result = service.run_updates(
        assets=["NDX", "SPX"],
        timeframe="1h",
        timezone_name="Europe/Madrid",
    )

    assert signal_service.calls == [
        {
            "symbol": "SPX",
            "timeframe": "1h",
            "timezone_name": "Europe/Madrid",
        }
    ]
    assert result["errors"] == []
    assert result["results"] == [
        {
            "asset": "NDX",
            "status": "skipped_market_closed",
            "market_status": "closed_holiday",
            "official_reason": "NDX market closed for holiday.",
            "next_open_local": "2026-04-06T09:30:00+02:00",
        },
        {
            "asset": "SPX",
            "timeframe": "1h",
            "response": {
                "asset": "SPX",
                "action": "ok",
            },
        },
    ]


def test_run_asset_updates_skips_wti_when_calendar_marks_reduced_hours() -> None:
    signal_service = FakeSignalService()

    class ReducedHoursCalendarService(FakeMarketCalendarService):
        def get_market_status(self, *, asset, current_dt=None, timezone_name="Europe/Madrid"):
            asset_value = asset.value if hasattr(asset, "value") else str(asset)
            return {
                "asset": asset_value,
                "market_status": "reduced_hours",
                "is_tradable_now": False,
                "official_reason": "NYMEX WTI daily maintenance break 17:00-18:00 ET.",
                "next_open_local": "2026-03-30T23:00:00+02:00",
            }

    service = AssetUpdateRunnerService(
        signal_service=signal_service,
        market_calendar_service=ReducedHoursCalendarService(),
    )

    result = service.run_updates(
        assets=[MarketSymbol.wti],
        timeframe=MarketTimeframe.h1,
        timezone_name="Europe/Madrid",
    )

    assert signal_service.calls == []
    assert result["errors"] == []
    assert result["results"] == [
        {
            "asset": "WTI",
            "status": "skipped_market_closed",
            "market_status": "reduced_hours",
            "official_reason": "NYMEX WTI daily maintenance break 17:00-18:00 ET.",
            "next_open_local": "2026-03-30T23:00:00+02:00",
        }
    ]


def test_run_updates_skips_all_assets_when_market_is_closed() -> None:
    signal_service = FakeSignalService()

    class ClosedCalendarService(FakeMarketCalendarService):
        def get_market_status(self, *, asset, current_dt=None, timezone_name="Europe/Madrid"):
            asset_value = asset.value if hasattr(asset, "value") else str(asset)
            return {
                "asset": asset_value,
                "market_status": "closed_weekend",
                "is_tradable_now": False,
                "official_reason": "Market closed for weekend.",
                "next_open_local": "2026-04-06T09:30:00+02:00",
            }

    service = AssetUpdateRunnerService(
        signal_service=signal_service,
        market_calendar_service=ClosedCalendarService(),
    )

    result = service.run_updates(
        assets=[MarketSymbol.ndx, MarketSymbol.spx],
        timeframe=MarketTimeframe.h1,
        timezone_name="Europe/Madrid",
    )

    assert result["errors"] == []
    assert result["results"] == [
        {
            "asset": "NDX",
            "status": "skipped_market_closed",
            "market_status": "closed_weekend",
            "official_reason": "Market closed for weekend.",
            "next_open_local": "2026-04-06T09:30:00+02:00",
        },
        {
            "asset": "SPX",
            "status": "skipped_market_closed",
            "market_status": "closed_weekend",
            "official_reason": "Market closed for weekend.",
            "next_open_local": "2026-04-06T09:30:00+02:00",
        },
    ]
    assert signal_service.calls == []
