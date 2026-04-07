from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from fastapi import HTTPException, status

from app.core.config import get_settings
from app.core.constants import ASSET_DISPLAY_NAMES
from app.repositories.signal_repository import SignalRepository
from app.schemas.day_context import DayBias, DayContext, DayContextLabel
from app.schemas.market import MarketSymbol, MarketTimeframe
from app.schemas.market_intelligence import MarketBias, MarketIntelligenceSnapshot
from app.schemas.scoring import SetupScore
from app.schemas.signal import SignalAction, SignalRequest, SignalResponse
from app.services.day_context_service import DayContextService
from app.services.briefing_builder_service import BriefingBuilderService
from app.services.explanation_service import ExplanationService
from app.services.feature_service import FeatureService
from app.services.intelligence_snapshot_service import IntelligenceSnapshotService
from app.services.market_calendar_service import MarketCalendarService
from app.services.market_data_service import MarketDataService
from app.services.probability_service import ProbabilityService
from app.services.regime_service import RegimeService
from app.services.risk_service import RiskService
from app.services.scoring_service import ScoringService
from app.services.session_service import SessionService
from app.services.telegram_alert_service import TelegramAlertService


class SignalService:
    def __init__(self) -> None:
        settings = get_settings()

        self.market_data_service = MarketDataService()
        self.feature_service = FeatureService()
        self.regime_service = RegimeService()
        self.scoring_service = ScoringService()
        self.risk_service = RiskService()
        self.probability_service = ProbabilityService()
        self.explanation_service = ExplanationService()
        self.briefing_builder_service = BriefingBuilderService()
        self.day_context_service = DayContextService()
        self.session_service = SessionService()
        self.intelligence_snapshot_service = IntelligenceSnapshotService()
        self.market_calendar_service = MarketCalendarService()
        self.telegram_alert_service = TelegramAlertService()
        self.signal_repository = SignalRepository()

        self.external_intelligence_enabled = settings.external_intelligence_enabled

    def generate_signal(
        self,
        payload: SignalRequest,
        timezone_name: str | None = None,
    ) -> SignalResponse:
        symbol: MarketSymbol = payload.symbol
        timeframe: MarketTimeframe = payload.timeframe
        asset = ASSET_DISPLAY_NAMES.get(symbol, symbol.value)
        resolved_timezone_name = self._resolve_timezone_name(timezone_name)
        self._last_timezone_name = resolved_timezone_name
        runtime_context = self._build_runtime_context(resolved_timezone_name)
        self._last_runtime_context = runtime_context
        briefing_context = self._build_briefing_context(resolved_timezone_name)
        briefing_context["market_closures"] = self._build_briefing_market_closures(
            timezone_name=resolved_timezone_name,
        )
        self._last_briefing_context = briefing_context

        try:
            try:
                market_data = self.market_data_service.get_ohlcv(symbol, timeframe)
            except ValueError as exc:
                requested_timeframe = str(getattr(timeframe, "value", timeframe)).lower()
                normalized_symbol = str(getattr(symbol, "value", symbol)).lower()

                if (
                    requested_timeframe == "1h"
                    and normalized_symbol in {"ndx", "spx"}
                    and "No OHLCV data returned" in str(exc)
                ):
                    market_data = self.market_data_service.get_ohlcv(
                        symbol,
                        MarketTimeframe.d1,
                    )
                else:
                    raise
            features = self.feature_service.build_features(market_data)
            features_with_regime = self.regime_service.classify_regime(features)

            latest_row = features_with_regime.iloc[-1]
            latest_regime = str(latest_row["regime"])
            session_context = self._get_session_context()
            day_context = self._get_day_context(latest_row)
            intelligence_snapshot = self._get_intelligence_snapshot(
                asset=asset,
                symbol=symbol,
                timeframe=timeframe,
                latest_row=latest_row,
                regime=latest_regime,
                session_context=session_context,
            )

            self.briefing_builder_service = getattr(
                self,
                "briefing_builder_service",
                BriefingBuilderService(),
            )
            self._last_briefing_payload = self.briefing_builder_service.build_briefing(
                asset=asset,
                symbol=symbol.value,
                timeframe=timeframe.value,
                briefing_context=briefing_context,
                session_context=session_context,
                day_context=day_context,
                intelligence_snapshot=intelligence_snapshot,
            )

            setup_score = self.scoring_service.score_setup(features_with_regime)
            setup_score = self._apply_session_guard(setup_score, session_context)
            setup_score = self._apply_final_trade_filter(setup_score, latest_row)
            setup_score = self._apply_day_context_filter(setup_score, day_context)
            setup_score = self._apply_intelligence_guard(
                setup_score,
                intelligence_snapshot,
            )

            risk_plan = self.risk_service.build_risk_plan(
                features_with_regime,
                setup_score.action,
            )
            probability_estimate = self.probability_service.estimate_probabilities(
                features_with_regime,
                setup_score,
                risk_plan,
            )
            explanation = self.explanation_service.build_explanation(
                asset=asset,
                regime=latest_regime,
                setup_score=setup_score,
                risk_plan=risk_plan,
                probability_estimate=probability_estimate,
                day_context=day_context,
                intelligence_snapshot=intelligence_snapshot,
            )

            response = SignalResponse(
                asset=asset,
                action=setup_score.action,
                entry_min=risk_plan.entry_min,
                entry_max=risk_plan.entry_max,
                entry_window=risk_plan.entry_window,
                expected_holding=risk_plan.expected_holding,
                hard_exit_time=risk_plan.hard_exit_time,
                close_by_session_end=risk_plan.close_by_session_end,
                stop_loss=risk_plan.stop_loss,
                take_profit_1=risk_plan.take_profit_1,
                take_profit_2=risk_plan.take_profit_2,
                risk_reward=risk_plan.risk_reward,
                favorable_move_pct=probability_estimate.favorable_move_pct,
                tp1_hit_pct=probability_estimate.tp1_hit_pct,
                stop_hit_first_pct=probability_estimate.stop_hit_first_pct,
                model_confidence_pct=probability_estimate.model_confidence_pct,
                confidence_label=probability_estimate.confidence_label,
                day_context_label=day_context.label,
                day_context_bias=day_context.bias,
                day_context_confidence_pct=day_context.confidence_pct,
                explanation=explanation,
            )

            self.signal_repository.save_signal(payload, response)
            self._maybe_publish_asset_main_update(
                symbol=symbol,
                response=response,
                day_context=day_context,
                intelligence_snapshot=intelligence_snapshot,
                timezone_name=resolved_timezone_name,
                risk_plan=risk_plan,
                probability_estimate=probability_estimate,
            )
            return response

        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(exc),
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(exc),
            ) from exc

    def _resolve_timezone_name(self, timezone_name: str | None) -> str:
        candidate = str(timezone_name or "").strip()

        if not candidate:
            return "UTC"

        try:
            ZoneInfo(candidate)
            return candidate
        except Exception:
            return "UTC"

    def _build_runtime_context(self, timezone_name: str) -> dict[str, object]:
        local_now = datetime.now(ZoneInfo(timezone_name))
        utc_now = datetime.now(timezone.utc)

        return {
            "timezone_name": timezone_name,
            "local_now_iso": local_now.isoformat(),
            "utc_now_iso": utc_now.isoformat(),
            "local_date": local_now.date().isoformat(),
            "local_time": local_now.strftime("%H:%M:%S"),
            "local_weekday": local_now.weekday(),
            "local_hour": local_now.hour,
            "local_minute": local_now.minute,
        }

    def _build_briefing_context(self, timezone_name: str) -> dict[str, object]:
        local_now = datetime.now(ZoneInfo(timezone_name))
        new_york_now = local_now.astimezone(ZoneInfo("America/New_York"))

        us_cash_open_ny = new_york_now.replace(
            hour=9,
            minute=30,
            second=0,
            microsecond=0,
        )
        us_cash_open_local = us_cash_open_ny.astimezone(ZoneInfo(timezone_name))

        countdown_minutes = int(
            (us_cash_open_local - local_now).total_seconds() // 60
        )

        return {
            "timezone_name": timezone_name,
            "local_now_iso": local_now.isoformat(),
            "local_date": local_now.date().isoformat(),
            "local_time": local_now.strftime("%H:%M:%S"),
            "local_weekday": local_now.weekday(),
            "is_local_morning_window": 6 <= local_now.hour < 12,
            "us_cash_open_local_time": us_cash_open_local.strftime("%H:%M"),
            "minutes_to_us_cash_open": countdown_minutes,
        }

    def _build_briefing_market_closures(
        self,
        *,
        timezone_name: str,
    ) -> list[dict[str, object]]:
        market_calendar_service = getattr(self, "market_calendar_service", None)
        if market_calendar_service is None:
            market_calendar_service = MarketCalendarService()
            self.market_calendar_service = market_calendar_service

        current_dt_utc = datetime.now(timezone.utc)
        market_closures: list[dict[str, object]] = []

        for asset_symbol in (
            MarketSymbol.ndx,
            MarketSymbol.spx,
            MarketSymbol.wti,
        ):
            try:
                market_status = market_calendar_service.get_market_status(
                    asset=asset_symbol,
                    current_dt=current_dt_utc,
                    timezone_name=timezone_name,
                )
            except Exception:
                continue

            resolved_market_status = str(market_status.get("market_status") or "")
            if resolved_market_status == "open":
                continue

            market_closures.append(
                {
                    "asset": str(
                        market_status.get("asset")
                        or getattr(asset_symbol, "value", asset_symbol)
                    ),
                    "market_status": resolved_market_status,
                    "official_reason": str(
                        market_status.get("official_reason") or ""
                    ),
                    "next_open_local": (
                        str(market_status.get("next_open_local"))
                        if market_status.get("next_open_local") is not None
                        else None
                    ),
                }
            )

        return market_closures

    def get_runtime_session_context(
        self,
        timezone_name: str | None = None,
    ):
        resolved_timezone_name = self._resolve_timezone_name(timezone_name)
        self._last_timezone_name = resolved_timezone_name
        self._last_runtime_context = self._build_runtime_context(
            resolved_timezone_name
        )
        return self._get_session_context()

    def get_last_briefing_payload(self) -> dict[str, object] | None:
        payload = getattr(self, "_last_briefing_payload", None)

        if not isinstance(payload, dict):
            return None

        return dict(payload)

    def send_last_briefing_alert(
        self,
        *,
        chat_id: str | None = None,
    ) -> dict:
        briefing_payload = self.get_last_briefing_payload()
        if not isinstance(briefing_payload, dict):
            raise ValueError("No briefing payload available.")

        telegram_service = getattr(self, "telegram_alert_service", None)
        if telegram_service is None:
            raise ValueError("Telegram alert service unavailable.")

        if hasattr(telegram_service, "send_briefing_payload"):
            return telegram_service.send_briefing_payload(
                briefing_payload=briefing_payload,
                chat_id=chat_id,
            )

        return telegram_service.send_briefing_alert(
            title=str(briefing_payload.get("title") or "").strip(),
            summary=str(briefing_payload.get("summary") or "").strip(),
            timezone_name=(
                str(briefing_payload.get("timezone_name")).strip()
                if briefing_payload.get("timezone_name") is not None
                else None
            ),
            local_time=(
                str(briefing_payload.get("local_time")).strip()
                if briefing_payload.get("local_time") is not None
                else None
            ),
            chat_id=chat_id,
        )

    def _maybe_publish_asset_topic_signal_alert(
        self,
        *,
        symbol: MarketSymbol,
        timeframe: MarketTimeframe,
        response: SignalResponse,
        day_context: DayContext | None = None,
        risk_plan=None,
        probability_estimate=None,
    ) -> None:
        telegram_service = getattr(self, "telegram_alert_service", None)
        if telegram_service is None:
            return

        if not hasattr(telegram_service, "should_send_signal_alert"):
            return

        action_value = (
            response.action.value
            if hasattr(response.action, "value")
            else str(response.action)
        )

        if not telegram_service.should_send_signal_alert(action=action_value):
            return

        asset_topic = str(symbol.value).strip().lower()
        if asset_topic not in {"ndx", "spx", "wti"}:
            return

        local_time = None
        runtime_context = getattr(self, "_last_runtime_context", None)
        if isinstance(runtime_context, dict):
            local_time = runtime_context.get("local_time")

        context = {
            "local_time": str(local_time or "--:--:--"),
            "scenario": str(
                day_context.label.value
                if day_context is not None
                else "signal_update"
            ),
            "bias": action_value,
            "plausible_action": action_value,
            "timeframe": str(
                timeframe.value if hasattr(timeframe, "value") else timeframe
            ),
            "model_confidence_pct": (
                float(
                    getattr(probability_estimate, "model_confidence_pct", 0.0)
                    or 0.0
                )
            ),
            "confidence_label": str(
                getattr(
                    getattr(probability_estimate, "confidence_label", None),
                    "value",
                    getattr(probability_estimate, "confidence_label", "unknown"),
                )
            ),
            "entry_min": getattr(risk_plan, "entry_min", None),
            "entry_max": getattr(risk_plan, "entry_max", None),
            "stop_loss": getattr(risk_plan, "stop_loss", None),
            "take_profit_1": getattr(risk_plan, "take_profit_1", None),
            "take_profit_2": getattr(risk_plan, "take_profit_2", None),
            "risk_reward": getattr(risk_plan, "risk_reward", None),
            "destinations": {
                asset_topic: {
                    "asset": str(response.asset),
                }
            },
        }

        try:
            telegram_service.publish_asset_topic_alert(
                asset_topic=asset_topic,
                conditions=["scenario_change"],
                context=context,
                allowed_destinations=[asset_topic],
            )
        except Exception:
            pass

    def _maybe_publish_asset_main_update(
        self,
        *,
        symbol: MarketSymbol,
        response: SignalResponse,
        day_context: DayContext | None = None,
        intelligence_snapshot: MarketIntelligenceSnapshot | None = None,
        timezone_name: str | None = None,
        risk_plan=None,
        probability_estimate=None,
    ) -> None:
        telegram_service = getattr(self, "telegram_alert_service", None)
        if telegram_service is None:
            return

        if not hasattr(telegram_service, "publish_asset_update"):
            return

        runtime_context = getattr(self, "_last_runtime_context", {}) or {}
        resolved_timezone = str(
            timezone_name or runtime_context.get("timezone_name") or "Europe/Madrid"
        )

        try:
            market_status = self.market_calendar_service.get_market_status(
                asset=symbol,
                timezone_name=resolved_timezone,
            )
            if not bool(market_status.get("is_tradable_now")):
                return
        except Exception:
            return

        asset_value = str(
            symbol.value if hasattr(symbol, "value") else symbol
        ).strip().upper()
        action_value = str(
            response.action.value
            if hasattr(response.action, "value")
            else response.action
        )
        scenario_value = str(
            day_context.label.value
            if day_context is not None
            else "signal_update"
        )
        confidence_value = str(
            getattr(
                getattr(probability_estimate, "confidence_label", None),
                "value",
                getattr(probability_estimate, "confidence_label", "unknown"),
            )
        )
        market_explanation = str(
            getattr(intelligence_snapshot, "synthesis", "")
            or response.explanation
            or ""
        ).strip()

        try:
            telegram_service.publish_asset_update(
                asset=asset_value,
                asset_full_name=str(response.asset),
                action=action_value,
                scenario=scenario_value,
                confidence_label=confidence_value,
                market_explanation=market_explanation or None,
                entry_min=getattr(risk_plan, "entry_min", None),
                entry_max=getattr(risk_plan, "entry_max", None),
                stop_loss=getattr(risk_plan, "stop_loss", None),
                take_profit_1=getattr(risk_plan, "take_profit_1", None),
                take_profit_2=getattr(risk_plan, "take_profit_2", None),
                risk_reward=getattr(risk_plan, "risk_reward", None),
                timezone_name=timezone_name,
            )
        except Exception:
            return

    def _get_session_context(self):
        session_service = getattr(self, "session_service", None)
        if session_service is None or not hasattr(session_service, "get_session_context"):
            return self._build_default_session_context()

        runtime_context = getattr(self, "_last_runtime_context", None)
        current_dt = None

        if isinstance(runtime_context, dict):
            local_now_iso = runtime_context.get("local_now_iso")
            if local_now_iso:
                try:
                    current_dt = datetime.fromisoformat(str(local_now_iso))
                except Exception:
                    current_dt = None

        try:
            if current_dt is not None:
                try:
                    return session_service.get_session_context(current_dt)
                except TypeError:
                    return session_service.get_session_context()

            return session_service.get_session_context()
        except Exception:
            return self._build_default_session_context()

    def _build_default_session_context(self):
        return SimpleNamespace(
            phase="open",
            is_session_open=True,
            allow_new_trades=True,
            minutes_to_cutoff=None,
        )

    def _get_day_context(self, latest_row) -> DayContext:
        day_context_service = getattr(self, "day_context_service", None)
        if day_context_service is None or not hasattr(
            day_context_service,
            "classify_day_context",
        ):
            return self._build_default_day_context()

        try:
            return day_context_service.classify_day_context(latest_row)
        except Exception:
            return self._build_default_day_context()

    def _build_default_day_context(self) -> DayContext:
        return DayContext(
            label=DayContextLabel.unclear,
            bias=DayBias.neutral,
            confidence_pct=45.0,
            prefer_breakout=False,
            avoid_mean_reversion=False,
            explanation="Day context unavailable.",
        )

    def _get_intelligence_snapshot(
        self,
        *,
        asset: str,
        symbol: MarketSymbol,
        timeframe: MarketTimeframe,
        latest_row,
        regime: str,
        session_context,
    ) -> MarketIntelligenceSnapshot:
        if not bool(getattr(self, "external_intelligence_enabled", False)):
            return self._build_default_intelligence_snapshot(
                asset=asset,
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                session_context=session_context,
                latest_row=latest_row,
                explanation="External intelligence disabled.",
            )

        intelligence_service = getattr(self, "intelligence_snapshot_service", None)
        if intelligence_service is None or not hasattr(
            intelligence_service,
            "build_snapshot_for_symbol",
        ):
            return self._build_default_intelligence_snapshot(
                asset=asset,
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                session_context=session_context,
                latest_row=latest_row,
                explanation="External intelligence unavailable.",
            )

        try:
            snapshot, _connector_results = intelligence_service.build_snapshot_for_symbol(
                asset=asset,
                symbol=symbol,
                timeframe=timeframe.value,
                max_items_per_source=5,
                session_phase=self._normalize_session_phase(session_context),
                regime=regime,
                volatility_20=self._safe_float(latest_row.get("volatility_20"), 0.0),
                rsi_14=self._safe_float(latest_row.get("rsi_14"), 50.0),
                distance_sma20=self._safe_float(latest_row.get("distance_sma20"), 0.0),
                distance_sma50=self._safe_float(latest_row.get("distance_sma50"), 0.0),
                key_levels=[],
            )
            return snapshot
        except Exception:
            return self._build_default_intelligence_snapshot(
                asset=asset,
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                session_context=session_context,
                latest_row=latest_row,
                explanation="External intelligence fetch failed.",
            )

    def _build_default_intelligence_snapshot(
        self,
        *,
        asset: str,
        symbol: MarketSymbol,
        timeframe: MarketTimeframe,
        regime: str,
        session_context,
        latest_row,
        explanation: str,
    ) -> MarketIntelligenceSnapshot:
        return MarketIntelligenceSnapshot(
            asset=asset,
            symbol=symbol.value,
            timeframe=timeframe.value,
            generated_at_utc=self._utc_now_iso(),
            market_bias=MarketBias.neutral,
            bias_confidence_pct=45.0,
            session_phase=self._normalize_session_phase(session_context),
            regime=regime,
            volatility_20=self._safe_float(latest_row.get("volatility_20"), 0.0),
            rsi_14=self._safe_float(latest_row.get("rsi_14"), 50.0),
            distance_sma20=self._safe_float(latest_row.get("distance_sma20"), 0.0),
            distance_sma50=self._safe_float(latest_row.get("distance_sma50"), 0.0),
            key_levels=[],
            dominant_drivers=[],
            risk_flags=[],
            items=[],
            synthesis=explanation,
        )

    def _apply_session_guard(self, setup_score: SetupScore, session_context) -> SetupScore:
        if bool(getattr(session_context, "allow_new_trades", True)):
            return setup_score

        phase = getattr(session_context, "phase", "unknown")
        phase_label = phase.value if hasattr(phase, "value") else str(phase)
        minutes_to_cutoff = getattr(session_context, "minutes_to_cutoff", None)
        cutoff_text = (
            f"{int(minutes_to_cutoff)}m to cutoff"
            if isinstance(minutes_to_cutoff, int)
            else "cutoff n/a"
        )

        return self._replace_action(
            setup_score,
            action=SignalAction.no_trade,
            direction="neutral",
            explanation=(
                "No-trade bias for now: session constraints active. "
                f"phase={phase_label}, {cutoff_text}. "
                f"Long score={setup_score.long_score:.1f}, short score={setup_score.short_score:.1f}."
            ),
        )

    def _apply_final_trade_filter(self, setup_score: SetupScore, latest_row) -> SetupScore:
        regime = str(latest_row["regime"])
        volatility_20 = (
            float(latest_row["volatility_20"])
            if latest_row["volatility_20"] == latest_row["volatility_20"]
            else 0.0
        )
        rsi_14 = (
            float(latest_row["rsi_14"])
            if latest_row["rsi_14"] == latest_row["rsi_14"]
            else 50.0
        )

        if regime == "sideways":
            return self._replace_action(
                setup_score,
                action=SignalAction.no_trade,
                direction="neutral",
                explanation=(
                    "No-trade bias for now: sideways intraday regime. "
                    f"Long score={setup_score.long_score:.1f}, short score={setup_score.short_score:.1f}, "
                    f"regime={regime}, RSI={rsi_14:.1f}."
                ),
            )

        if regime.endswith("high_vol") and volatility_20 >= 0.02:
            return self._replace_action(
                setup_score,
                action=SignalAction.no_trade,
                direction="neutral",
                explanation=(
                    "No-trade bias for now: intraday volatility too unstable. "
                    f"Long score={setup_score.long_score:.1f}, short score={setup_score.short_score:.1f}, "
                    f"regime={regime}, RSI={rsi_14:.1f}."
                ),
            )

        if setup_score.action == SignalAction.wait and setup_score.score < 45.0:
            return self._replace_action(
                setup_score,
                action=SignalAction.no_trade,
                direction="neutral",
                explanation=(
                    "No-trade bias for now: setup quality too weak for an intraday CFD trade. "
                    f"Long score={setup_score.long_score:.1f}, short score={setup_score.short_score:.1f}, "
                    f"regime={regime}, RSI={rsi_14:.1f}."
                ),
            )

        return setup_score

    def _apply_day_context_filter(
        self,
        setup_score: SetupScore,
        day_context: DayContext,
    ) -> SetupScore:
        if setup_score.action in {SignalAction.wait, SignalAction.no_trade}:
            return setup_score

        if day_context.label == DayContextLabel.volatile_day:
            return self._replace_action(
                setup_score,
                action=SignalAction.no_trade,
                direction="neutral",
                explanation=(
                    "No-trade bias for now: day context flags unstable volatility. "
                    f"day_context={day_context.label.value}, confidence={day_context.confidence_pct:.1f}%."
                ),
            )

        if (
            setup_score.action == SignalAction.long
            and day_context.bias == DayBias.short
            and day_context.confidence_pct >= 70.0
        ):
            return self._replace_action(
                setup_score,
                action=SignalAction.wait,
                direction="neutral",
                explanation=(
                    "Wait bias: day context is strongly short-biased against a long setup. "
                    f"day_context={day_context.label.value}, confidence={day_context.confidence_pct:.1f}%."
                ),
            )

        if (
            setup_score.action == SignalAction.short
            and day_context.bias == DayBias.long
            and day_context.confidence_pct >= 70.0
        ):
            return self._replace_action(
                setup_score,
                action=SignalAction.wait,
                direction="neutral",
                explanation=(
                    "Wait bias: day context is strongly long-biased against a short setup. "
                    f"day_context={day_context.label.value}, confidence={day_context.confidence_pct:.1f}%."
                ),
            )

        return setup_score

    def _apply_intelligence_guard(
        self,
        setup_score: SetupScore,
        intelligence_snapshot: MarketIntelligenceSnapshot,
    ) -> SetupScore:
        if setup_score.action in {SignalAction.wait, SignalAction.no_trade}:
            return setup_score

        intelligence_bias = intelligence_snapshot.market_bias
        intelligence_confidence = intelligence_snapshot.bias_confidence_pct
        drivers = ", ".join(intelligence_snapshot.dominant_drivers[:3]) or "none"

        if (
            intelligence_bias == MarketBias.no_trade
            and intelligence_confidence >= 70.0
        ):
            return self._replace_action(
                setup_score,
                action=SignalAction.no_trade,
                direction="neutral",
                explanation=(
                    "No-trade bias for now: external intelligence flags unstable context. "
                    f"intelligence_bias={intelligence_bias.value}, "
                    f"confidence={intelligence_confidence:.1f}%, "
                    f"drivers={drivers}."
                ),
            )

        if (
            setup_score.action == SignalAction.long
            and intelligence_bias == MarketBias.short
            and intelligence_confidence >= 65.0
        ):
            return self._replace_action(
                setup_score,
                action=SignalAction.wait,
                direction="neutral",
                explanation=(
                    "Wait bias: external intelligence is strongly short-biased against a long setup. "
                    f"intelligence_bias={intelligence_bias.value}, "
                    f"confidence={intelligence_confidence:.1f}%, "
                    f"drivers={drivers}."
                ),
            )

        if (
            setup_score.action == SignalAction.short
            and intelligence_bias == MarketBias.long
            and intelligence_confidence >= 65.0
        ):
            return self._replace_action(
                setup_score,
                action=SignalAction.wait,
                direction="neutral",
                explanation=(
                    "Wait bias: external intelligence is strongly long-biased against a short setup. "
                    f"intelligence_bias={intelligence_bias.value}, "
                    f"confidence={intelligence_confidence:.1f}%, "
                    f"drivers={drivers}."
                ),
            )

        return setup_score

    def _replace_action(
        self,
        setup_score: SetupScore,
        action: SignalAction,
        direction: str,
        explanation: str,
    ) -> SetupScore:
        return SetupScore(
            action=action,
            direction=direction,
            score=setup_score.score,
            long_score=setup_score.long_score,
            short_score=setup_score.short_score,
            trend_score=setup_score.trend_score,
            moving_average_score=setup_score.moving_average_score,
            rsi_score=setup_score.rsi_score,
            price_position_score=setup_score.price_position_score,
            regime_score=setup_score.regime_score,
            explanation=explanation,
        )

    def _normalize_session_phase(self, session_context) -> str:
        phase = getattr(session_context, "phase", "unknown")

        if hasattr(phase, "value"):
            return str(phase.value)

        if hasattr(phase, "name"):
            return str(phase.name).lower()

        return str(phase)

    def _safe_float(self, value, default: float) -> float:
        if value != value:
            return default

        try:
            return float(value)
        except Exception:
            return default

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()


_signal_service = SignalService()


def generate_signal(
    payload: SignalRequest,
    timezone_name: str | None = None,
) -> SignalResponse:
    return _signal_service.generate_signal(
        payload,
        timezone_name=timezone_name,
    )
