from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from hashlib import sha1

from app.schemas.market_intelligence import (
    IntelligenceDirection,
    IntelligenceImportance,
    IntelligenceIngestionResult,
    MarketBias,
    MarketIntelligenceItem,
    MarketIntelligenceSnapshot,
    StructuredMarketEvent,
)


class MarketIntelligenceService:
    def build_snapshot(
        self,
        *,
        asset: str,
        symbol: str,
        timeframe: str,
        items: list[MarketIntelligenceItem],
        session_phase: str | None = None,
        regime: str | None = None,
        volatility_20: float | None = None,
        rsi_14: float | None = None,
        distance_sma20: float | None = None,
        distance_sma50: float | None = None,
        key_levels: list[float] | None = None,
        generated_at_utc: str | None = None,
    ) -> MarketIntelligenceSnapshot:
        accepted_items = self._filter_items(items)
        ingestion = self.summarize_ingestion(accepted_items)
        market_bias, bias_confidence_pct = self._derive_market_bias(
            items=accepted_items,
            regime=regime,
            volatility_20=volatility_20,
        )
        dominant_drivers = self._derive_dominant_drivers(accepted_items)
        top_headlines = self._derive_top_headlines(accepted_items)
        risk_flags = self._derive_risk_flags(
            items=accepted_items,
            session_phase=session_phase,
            regime=regime,
            volatility_20=volatility_20,
        )
        synthesis = self._build_synthesis(
            asset=asset,
            market_bias=market_bias,
            bias_confidence_pct=bias_confidence_pct,
            session_phase=session_phase,
            regime=regime,
            ingestion=ingestion,
            dominant_drivers=dominant_drivers,
            risk_flags=risk_flags,
            top_headlines=top_headlines,
        )

        return MarketIntelligenceSnapshot(
            asset=asset,
            symbol=symbol,
            timeframe=timeframe,
            generated_at_utc=generated_at_utc or self._utc_now_iso(),
            market_bias=market_bias,
            bias_confidence_pct=bias_confidence_pct,
            session_phase=session_phase,
            regime=regime,
            volatility_20=volatility_20,
            rsi_14=rsi_14,
            distance_sma20=distance_sma20,
            distance_sma50=distance_sma50,
            key_levels=key_levels or [],
            dominant_drivers=dominant_drivers,
            risk_flags=risk_flags,
            items=accepted_items,
            synthesis=synthesis,
        )

    def build_structured_events(
        self,
        *,
        asset: str,
        symbol: str,
        timeframe: str,
        items: list[MarketIntelligenceItem],
        session_phase: str | None = None,
        regime: str | None = None,
        volatility_20: float | None = None,
    ) -> list[StructuredMarketEvent]:
        accepted_items = self._filter_items(items)
        structured_events: list[StructuredMarketEvent] = []

        for item in accepted_items:
            title = str(item.title or item.summary or item.source_name or item.event_type.value).strip()
            summary = str(item.summary or item.title or "").strip()
            market_context = {
                "session_phase": session_phase,
                "regime": regime,
                "volatility_20": volatility_20,
                "relevance_score": round(float(item.relevance_score), 1),
                "source": item.source.value,
            }

            structured_events.append(
                StructuredMarketEvent(
                    event_id=self._build_event_id(
                        asset=asset,
                        symbol=symbol,
                        timeframe=timeframe,
                        item=item,
                    ),
                    asset=asset,
                    symbol=symbol,
                    timeframe=timeframe,
                    event_type=item.event_type,
                    source_type=item.source,
                    source_name=item.source_name or "unknown",
                    source_url=item.source_url,
                    direction=item.direction,
                    urgency=item.importance,
                    confidence_pct=item.confidence_pct,
                    decay_minutes=item.impact_horizon_minutes,
                    scenario_change=item.direction != IntelligenceDirection.neutral,
                    title=title,
                    summary=summary,
                    raw_text=item.raw_text,
                    occurred_at_utc=item.occurred_at_utc,
                    detected_at_utc=item.detected_at_utc,
                    tags=list(item.tags),
                    market_context=market_context,
                    event_score=self._estimate_event_score(item),
                )
            )

        return structured_events

    def summarize_ingestion(
        self,
        items: list[MarketIntelligenceItem],
    ) -> IntelligenceIngestionResult:
        accepted_items = self._filter_items(items)
        discarded_items = max(len(items) - len(accepted_items), 0)
        critical_items = sum(
            1
            for item in accepted_items
            if item.importance == IntelligenceImportance.critical
        )

        if not accepted_items:
            return IntelligenceIngestionResult(
                accepted_items=0,
                discarded_items=discarded_items,
                critical_items=0,
                latest_direction=IntelligenceDirection.neutral,
                average_confidence_pct=0.0,
                summary="No usable intelligence items were accepted.",
            )

        average_confidence_pct = round(
            sum(item.confidence_pct for item in accepted_items) / len(accepted_items),
            1,
        )
        latest_direction = accepted_items[-1].direction

        summary = (
            f"Accepted {len(accepted_items)} items, "
            f"discarded {discarded_items}, "
            f"critical {critical_items}, "
            f"latest direction={latest_direction.value}, "
            f"avg confidence={average_confidence_pct:.1f}%."
        )

        return IntelligenceIngestionResult(
            accepted_items=len(accepted_items),
            discarded_items=discarded_items,
            critical_items=critical_items,
            latest_direction=latest_direction,
            average_confidence_pct=average_confidence_pct,
            summary=summary,
        )

    def _filter_items(
        self,
        items: list[MarketIntelligenceItem],
    ) -> list[MarketIntelligenceItem]:
        accepted_items: list[MarketIntelligenceItem] = []

        for item in items:
            if not item.title and not item.summary and not item.raw_text:
                continue

            if (
                item.relevance_score < 20.0
                and item.importance == IntelligenceImportance.low
            ):
                continue

            accepted_items.append(item)

        return accepted_items

    def _derive_market_bias(
        self,
        *,
        items: list[MarketIntelligenceItem],
        regime: str | None,
        volatility_20: float | None,
    ) -> tuple[MarketBias, float]:
        if regime == "sideways":
            return MarketBias.no_trade, 72.0

        if (
            regime is not None
            and regime.endswith("high_vol")
            and (volatility_20 or 0.0) >= 0.02
        ):
            return MarketBias.no_trade, 78.0

        if not items:
            return MarketBias.neutral, 45.0

        bullish_score = 0.0
        bearish_score = 0.0
        neutral_score = 0.0

        for item in items:
            weight = self._direction_weight(item)

            if item.direction == IntelligenceDirection.bullish:
                bullish_score += weight
            elif item.direction == IntelligenceDirection.bearish:
                bearish_score += weight
            elif item.direction == IntelligenceDirection.neutral:
                neutral_score += weight
            else:
                neutral_score += weight * 0.8

        total_score = bullish_score + bearish_score + neutral_score
        if total_score <= 0:
            return MarketBias.neutral, 45.0

        if bullish_score > bearish_score and bullish_score >= neutral_score:
            confidence_pct = round((bullish_score / total_score) * 100.0, 1)
            return MarketBias.long, max(confidence_pct, 45.0)

        if bearish_score > bullish_score and bearish_score >= neutral_score:
            confidence_pct = round((bearish_score / total_score) * 100.0, 1)
            return MarketBias.short, max(confidence_pct, 45.0)

        confidence_pct = round((neutral_score / total_score) * 100.0, 1)
        return MarketBias.neutral, max(confidence_pct, 45.0)

    def _direction_weight(self, item: MarketIntelligenceItem) -> float:
        importance_multiplier = {
            IntelligenceImportance.low: 0.8,
            IntelligenceImportance.medium: 1.0,
            IntelligenceImportance.high: 1.35,
            IntelligenceImportance.critical: 1.7,
        }[item.importance]

        relevance_component = max(item.relevance_score, 1.0) / 100.0
        confidence_component = max(item.confidence_pct, 1.0) / 100.0

        return importance_multiplier * relevance_component * confidence_component * 100.0

    def _estimate_event_score(self, item: MarketIntelligenceItem) -> float:
        importance_bonus = {
            IntelligenceImportance.low: -5.0,
            IntelligenceImportance.medium: 0.0,
            IntelligenceImportance.high: 5.0,
            IntelligenceImportance.critical: 10.0,
        }[item.importance]
        directional_bonus = (
            3.0 if item.direction != IntelligenceDirection.neutral else 0.0
        )
        raw_score = (
            (float(item.relevance_score) + float(item.confidence_pct)) / 2.0
            + importance_bonus
            + directional_bonus
        )
        return round(max(0.0, min(raw_score, 100.0)), 1)

    def _build_event_id(
        self,
        *,
        asset: str,
        symbol: str,
        timeframe: str,
        item: MarketIntelligenceItem,
    ) -> str:
        payload = "|".join(
            [
                asset,
                symbol,
                timeframe,
                item.event_type.value,
                item.source.value,
                item.source_name or "",
                item.title or "",
                item.summary or "",
                item.occurred_at_utc or "",
                item.detected_at_utc or "",
            ]
        )
        digest = sha1(payload.encode("utf-8")).hexdigest()[:16]
        return f"{str(symbol).strip().lower()}-{item.event_type.value}-{digest}"

    def _derive_dominant_drivers(
        self,
        items: list[MarketIntelligenceItem],
    ) -> list[str]:
        driver_counter: Counter[str] = Counter()

        for item in items:
            for tag in item.tags:
                normalized_tag = str(tag).strip().lower()
                if normalized_tag:
                    driver_counter[normalized_tag] += 1

            if item.source_name:
                driver_counter[item.source_name.strip().lower()] += 1

            if item.event_type.value:
                driver_counter[item.event_type.value] += 1

        most_common = [name for name, _ in driver_counter.most_common(5)]
        return most_common

    def _derive_top_headlines(
        self,
        items: list[MarketIntelligenceItem],
    ) -> list[str]:
        headlines: list[str] = []

        for item in items:
            title = str(item.title or "").strip()
            if not title:
                continue
            if title in headlines:
                continue
            headlines.append(title)
            if len(headlines) >= 2:
                break

        return headlines

    def _derive_risk_flags(
        self,
        *,
        items: list[MarketIntelligenceItem],
        session_phase: str | None,
        regime: str | None,
        volatility_20: float | None,
    ) -> list[str]:
        risk_flags: list[str] = []

        if session_phase in {"pre_open", "closed"}:
            risk_flags.append("session_not_tradeable")

        if session_phase == "power_hour":
            risk_flags.append("late_session")

        if regime == "sideways":
            risk_flags.append("sideways_regime")

        if regime is not None and regime.endswith("high_vol"):
            risk_flags.append("high_vol_regime")

        if (volatility_20 or 0.0) >= 0.02:
            risk_flags.append("elevated_volatility")

        critical_items = [
            item for item in items if item.importance == IntelligenceImportance.critical
        ]
        if critical_items:
            risk_flags.append("critical_event_flow")

        mixed_count = sum(
            1 for item in items if item.direction == IntelligenceDirection.mixed
        )
        if mixed_count >= 2:
            risk_flags.append("conflicting_signals")

        return sorted(set(risk_flags))

    def _build_synthesis(
        self,
        *,
        asset: str,
        market_bias: MarketBias,
        bias_confidence_pct: float,
        session_phase: str | None,
        regime: str | None,
        ingestion: IntelligenceIngestionResult,
        dominant_drivers: list[str],
        risk_flags: list[str],
        top_headlines: list[str],
    ) -> str:
        drivers_text = ", ".join(dominant_drivers[:3]) if dominant_drivers else "none"
        risk_text = ", ".join(risk_flags[:3]) if risk_flags else "none"
        phase_text = session_phase or "n/a"
        regime_text = regime or "n/a"
        headlines_text = " | ".join(top_headlines[:2]) if top_headlines else "none"

        return (
            f"{asset}: intelligence bias={market_bias.value}, "
            f"confidence={bias_confidence_pct:.1f}%, "
            f"session={phase_text}, regime={regime_text}. "
            f"Accepted items={ingestion.accepted_items}, critical items={ingestion.critical_items}. "
            f"Top headlines={headlines_text}. "
            f"Dominant drivers={drivers_text}. "
            f"Risk flags={risk_text}."
        )

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()
