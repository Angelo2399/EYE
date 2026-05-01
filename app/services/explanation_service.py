from __future__ import annotations

from app.schemas.day_context import DayContext
from app.schemas.market_intelligence import MarketIntelligenceSnapshot
from app.schemas.probability import ProbabilityEstimate
from app.schemas.risk import RiskPlan
from app.schemas.scoring import SetupScore
from app.services.technical_confluence_service import TechnicalConfluence


class ExplanationService:
    def build_explanation(
        self,
        asset: str,
        regime: str,
        setup_score: SetupScore,
        risk_plan: RiskPlan,
        probability_estimate: ProbabilityEstimate,
        day_context: DayContext | None = None,
        intelligence_snapshot: MarketIntelligenceSnapshot | None = None,
        technical_confluence: TechnicalConfluence | None = None,
    ) -> str:
        action = setup_score.action.value.upper()
        regime_label = regime.replace("_", " ")
        holding_text = (
            risk_plan.expected_holding.value
            if risk_plan.expected_holding is not None
            else "n/a"
        )
        exit_text = risk_plan.hard_exit_time or "n/a"
        day_context_text = self._format_day_context(day_context)
        technical_text = self._format_technical_confluence(technical_confluence)
        intelligence_text = self._format_external_intelligence(intelligence_snapshot)

        if setup_score.action.value in {"wait", "no_trade"}:
            return (
                f"{asset}: {action}. "
                f"Regime {regime_label}. "
                f"Setup intraday non abbastanza pulito. "
                f"{day_context_text}"
                f"{technical_text}"
                f"{intelligence_text}"
                f"Score={setup_score.score:.1f}, "
                f"fav move={probability_estimate.favorable_move_pct:.1f}%, "
                f"confidence={probability_estimate.confidence_label.value}. "
                f"Nessuna posizione overnight."
            )

        entry_text = self._format_entry(risk_plan)
        stop_text = self._format_price(risk_plan.stop_loss)
        tp1_text = self._format_price(risk_plan.take_profit_1)
        tp2_text = self._format_price(risk_plan.take_profit_2)
        rr_text = self._format_ratio(risk_plan.risk_reward)
        entry_window_text = risk_plan.entry_window or "n/a"
        session_text = "yes" if risk_plan.close_by_session_end else "no"

        return (
            f"{asset}: {action} intraday. "
            f"Regime {regime_label}. "
            f"{day_context_text}"
            f"{technical_text}"
            f"{intelligence_text}"
            f"Entry window {entry_window_text}, "
            f"holding {holding_text}, "
            f"hard exit {exit_text}, "
            f"close by session end={session_text}. "
            f"Entry {entry_text}, stop {stop_text}, tp1 {tp1_text}, tp2 {tp2_text}, "
            f"R/R {rr_text}. "
            f"Score={setup_score.score:.1f}, "
            f"fav move={probability_estimate.favorable_move_pct:.1f}%, "
            f"tp1={probability_estimate.tp1_hit_pct:.1f}%, "
            f"stop first={probability_estimate.stop_hit_first_pct:.1f}%, "
            f"confidence={probability_estimate.confidence_label.value}."
        )

    def _format_day_context(self, day_context: DayContext | None) -> str:
        if day_context is None:
            return ""

        return (
            f"Day context {day_context.label.value}, "
            f"bias={day_context.bias.value}, "
            f"ctx_conf={day_context.confidence_pct:.1f}%. "
        )

    def _format_technical_confluence(
        self,
        technical_confluence: TechnicalConfluence | None,
    ) -> str:
        if technical_confluence is None:
            return ""

        return (
            f"Technical confluence score={technical_confluence.confluence_score:.1f}, "
            f"trend={technical_confluence.trend_bias}, "
            f"momentum={technical_confluence.momentum_state}, "
            f"volatility={technical_confluence.volatility_state}, "
            f"structure={technical_confluence.structure_state}, "
            f"summary={technical_confluence.summary} "
        )

    def _format_external_intelligence(
        self,
        intelligence_snapshot: MarketIntelligenceSnapshot | None,
    ) -> str:
        if intelligence_snapshot is None:
            return ""

        drivers_text = self._build_asset_specific_driver_text(intelligence_snapshot)
        synthesis = str(intelligence_snapshot.synthesis or "").strip()

        if synthesis:
            return (
                f"External intelligence bias={intelligence_snapshot.market_bias.value}, "
                f"ext_conf={intelligence_snapshot.bias_confidence_pct:.1f}%, "
                f"drivers={drivers_text}, "
                f"synthesis={synthesis}. "
            )

        return (
            f"External intelligence bias={intelligence_snapshot.market_bias.value}, "
            f"ext_conf={intelligence_snapshot.bias_confidence_pct:.1f}%, "
            f"drivers={drivers_text}. "
        )

    def _build_asset_specific_driver_text(
        self,
        intelligence_snapshot: MarketIntelligenceSnapshot,
    ) -> str:
        items = list(getattr(intelligence_snapshot, "items", []) or [])
        ranked_items = sorted(
            items,
            key=self._rank_intelligence_item,
            reverse=True,
        )

        for item in ranked_items:
            title = " ".join(str(getattr(item, "title", "")).split()).strip()
            source_name = " ".join(str(getattr(item, "source_name", "")).split()).strip()

            if title and source_name:
                return f"{source_name}: {title}"

            if title:
                return title

        drivers = [
            str(driver).strip()
            for driver in intelligence_snapshot.dominant_drivers[:3]
            if str(driver).strip()
        ]
        return ", ".join(drivers) if drivers else "n/a"

    def _rank_intelligence_item(self, item) -> float:
        importance_value = str(
            getattr(getattr(item, "importance", None), "value", getattr(item, "importance", ""))
        ).strip().lower()

        importance_rank = {
            "critical": 4.0,
            "high": 3.0,
            "medium": 2.0,
            "low": 1.0,
        }.get(importance_value, 0.0)

        confidence_pct = float(getattr(item, "confidence_pct", 0.0) or 0.0)
        relevance_score = float(getattr(item, "relevance_score", 0.0) or 0.0)

        return (importance_rank * 1000.0) + (confidence_pct * 10.0) + relevance_score

    def _format_entry(self, risk_plan: RiskPlan) -> str:
        if risk_plan.entry_min is None or risk_plan.entry_max is None:
            return "n/a"

        return f"{risk_plan.entry_min:.2f}-{risk_plan.entry_max:.2f}"

    def _format_price(self, value: float | None) -> str:
        if value is None:
            return "n/a"

        return f"{value:.2f}"

    def _format_ratio(self, value: float | None) -> str:
        if value is None:
            return "n/a"

        return f"{value:.2f}"
