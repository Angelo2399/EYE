from __future__ import annotations

from app.schemas.market import MarketSymbol, MarketTimeframe
from app.schemas.signal import SignalRequest
from app.services.market_calendar_service import MarketCalendarService
from app.services.signal_service import SignalService

DEFAULT_ASSET_UPDATE_SYMBOLS = (
    MarketSymbol.ndx,
    MarketSymbol.spx,
    MarketSymbol.wti,
)


class AssetUpdateRunnerService:
    def __init__(
        self,
        *,
        signal_service: SignalService | None = None,
        market_calendar_service: MarketCalendarService | None = None,
    ) -> None:
        self.signal_service = signal_service or SignalService()
        self.market_calendar_service = market_calendar_service or MarketCalendarService()
        self.default_assets = list(DEFAULT_ASSET_UPDATE_SYMBOLS)

    def run_updates(
        self,
        *,
        assets: list[str | MarketSymbol] | None = None,
        timeframe: str | MarketTimeframe = MarketTimeframe.h1,
        timezone_name: str | None = None,
    ) -> dict[str, list[dict[str, object]]]:
        resolved_assets = self._resolve_assets(assets or self.default_assets)
        resolved_timeframe = self._resolve_timeframe(timeframe)

        results: list[dict[str, object]] = []
        errors: list[dict[str, object]] = []

        for asset_symbol in resolved_assets:
            market_status = self.market_calendar_service.get_market_status(
                asset=asset_symbol,
                timezone_name=str(timezone_name or "Europe/Madrid"),
            )
            if not bool(market_status.get("is_tradable_now")):
                results.append(
                    {
                        "asset": asset_symbol.value,
                        "status": "skipped_market_closed",
                        "market_status": market_status.get("market_status"),
                        "official_reason": market_status.get("official_reason"),
                        "next_open_local": market_status.get("next_open_local"),
                    }
                )
                continue

            request = SignalRequest(
                symbol=asset_symbol,
                timeframe=resolved_timeframe,
            )

            try:
                response = self.signal_service.generate_signal(
                    request,
                    timezone_name=timezone_name,
                )
                results.append(
                    {
                        "asset": asset_symbol.value,
                        "timeframe": resolved_timeframe.value,
                        "response": response,
                    }
                )
            except Exception as exc:
                errors.append(
                    {
                        "asset": asset_symbol.value,
                        "timeframe": resolved_timeframe.value,
                        "error": str(exc),
                    }
                )

        return {
            "results": results,
            "errors": errors,
        }

    def run_asset_updates(
        self,
        *,
        assets: list[str | MarketSymbol] | None = None,
        timeframe: str | MarketTimeframe = MarketTimeframe.h1,
        timezone_name: str | None = None,
    ) -> dict[str, list[dict[str, object]]]:
        return self.run_updates(
            assets=assets,
            timeframe=timeframe,
            timezone_name=timezone_name,
        )

    def _resolve_assets(
        self,
        assets: list[str | MarketSymbol] | None,
    ) -> list[MarketSymbol]:
        return [
            asset if isinstance(asset, MarketSymbol) else MarketSymbol(str(asset))
            for asset in assets
        ]

    def _resolve_timeframe(
        self,
        timeframe: str | MarketTimeframe,
    ) -> MarketTimeframe:
        if isinstance(timeframe, MarketTimeframe):
            return timeframe

        return MarketTimeframe(str(timeframe))

__all__ = [
    "AssetUpdateRunnerService",
    "DEFAULT_ASSET_UPDATE_SYMBOLS",
]
