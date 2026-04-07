from __future__ import annotations

import importlib


class _DummyDependency:
    pass


def _build_service_with_flag(enabled: bool):
    module = importlib.import_module("app.services.signal_service")

    module.get_settings = lambda: type(
        "FakeSettings",
        (),
        {"external_intelligence_enabled": enabled},
    )()

    module.MarketDataService = _DummyDependency
    module.FeatureService = _DummyDependency
    module.RegimeService = _DummyDependency
    module.ScoringService = _DummyDependency
    module.RiskService = _DummyDependency
    module.ProbabilityService = _DummyDependency
    module.ExplanationService = _DummyDependency
    module.DayContextService = _DummyDependency
    module.SessionService = _DummyDependency
    module.IntelligenceSnapshotService = _DummyDependency
    module.SignalRepository = _DummyDependency

    return module.SignalService()


def test_signal_service_reads_external_intelligence_flag_as_false_from_settings() -> None:
    service = _build_service_with_flag(enabled=False)

    assert service.external_intelligence_enabled is False


def test_signal_service_reads_external_intelligence_flag_as_true_from_settings() -> None:
    service = _build_service_with_flag(enabled=True)

    assert service.external_intelligence_enabled is True
