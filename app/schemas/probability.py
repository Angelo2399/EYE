from __future__ import annotations

from dataclasses import dataclass

from app.schemas.signal import ModelConfidence


@dataclass(frozen=True)
class ProbabilityEstimate:
    favorable_move_pct: float
    tp1_hit_pct: float
    stop_hit_first_pct: float
    model_confidence_pct: float
    confidence_label: ModelConfidence
