from __future__ import annotations

from app.schemas.day_context import DayContext
from app.schemas.market_intelligence import MarketIntelligenceSnapshot


class BriefingBuilderService:
    def build_briefing(
        self,
        *,
        asset: str,
        symbol: str,
        timeframe: str,
        briefing_context: dict[str, object],
        session_context=None,
        day_context: DayContext | None = None,
        intelligence_snapshot: MarketIntelligenceSnapshot | None = None,
    ) -> dict[str, object]:
        timezone_name = str(briefing_context.get("timezone_name") or "UTC")
        local_date = str(briefing_context.get("local_date") or "")
        local_time = str(briefing_context.get("local_time") or "")
        local_weekday = int(briefing_context.get("local_weekday") or 0)
        is_local_morning_window = bool(
            briefing_context.get("is_local_morning_window") or False
        )
        us_cash_open_local_time = str(
            briefing_context.get("us_cash_open_local_time") or ""
        )
        minutes_to_us_cash_open = int(
            briefing_context.get("minutes_to_us_cash_open") or 0
        )
        raw_market_closures = list(briefing_context.get("market_closures") or [])

        session_phase = str(getattr(session_context, "phase", "unknown"))
        day_context_label = (
            day_context.label.value if day_context is not None else "unclear"
        )
        day_context_bias = (
            day_context.bias.value if day_context is not None else "neutral"
        )

        market_bias = "neutral"
        bias_confidence_pct = 0.0
        risk_flags: list[str] = []
        dominant_drivers: list[str] = []

        if intelligence_snapshot is not None:
            market_bias = intelligence_snapshot.market_bias.value
            bias_confidence_pct = float(intelligence_snapshot.bias_confidence_pct)
            risk_flags = list(intelligence_snapshot.risk_flags or [])
            dominant_drivers = list(intelligence_snapshot.dominant_drivers or [])

        market_closures = [
            {
                "asset": str(item.get("asset") or ""),
                "market_status": str(item.get("market_status") or ""),
                "official_reason": str(item.get("official_reason") or ""),
                "next_open_local": (
                    str(item.get("next_open_local"))
                    if item.get("next_open_local") is not None
                    else None
                ),
            }
            for item in raw_market_closures
            if isinstance(item, dict)
        ]

        summary = (
            f"{asset} briefing | "
            f"local {local_date} {local_time} ({timezone_name}) | "
            f"session={session_phase} | "
            f"day_context={day_context_label}/{day_context_bias} | "
            f"market_bias={market_bias} ({bias_confidence_pct:.1f}%) | "
            f"US open local={us_cash_open_local_time} | "
            f"T-{minutes_to_us_cash_open}m"
        )

        if market_closures:
            closure_lines = [
                (
                    f"- {closure['asset']}: {closure['market_status']} | "
                    f"{closure['official_reason']} | "
                    f"Next open: {closure['next_open_local'] or 'n/a'}"
                )
                for closure in market_closures
            ]
            summary = "\n".join(
                [
                    summary,
                    "",
                    "Closed / limited markets today",
                    *closure_lines,
                ]
            )

        return {
            "title": f"{asset} intraday briefing",
            "asset": asset,
            "symbol": symbol,
            "timeframe": timeframe,
            "timezone_name": timezone_name,
            "local_date": local_date,
            "local_time": local_time,
            "local_weekday": local_weekday,
            "is_local_morning_window": is_local_morning_window,
            "session_phase": session_phase,
            "day_context_label": day_context_label,
            "day_context_bias": day_context_bias,
            "market_bias": market_bias,
            "bias_confidence_pct": round(bias_confidence_pct, 1),
            "us_cash_open_local_time": us_cash_open_local_time,
            "minutes_to_us_cash_open": minutes_to_us_cash_open,
            "dominant_drivers": dominant_drivers,
            "risk_flags": risk_flags,
            "market_closures": market_closures,
            "summary": summary,
        }
