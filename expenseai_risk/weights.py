"""Helpers for loading and validating risk contributor weights."""
from __future__ import annotations

from typing import Dict, Tuple

from flask import Flask

EXPECTED_KEYS = {
    "market_outlier",
    "arithmetic",
    "hsn_rate",
    "gst_vendor",
    "gst_company",
    "duplicate",
}


def resolve_weights(app: Flask) -> Tuple[Dict[str, float], str]:
    """Return (weights, policy_version) blending config defaults with adaptive policy."""
    policy_version = "seed"
    weights = _load_from_config(app)

    if app.config.get("BANDIT_ENABLED", True):
        try:
            from expenseai_bandit.policy import get_active_policy  # local import to avoid circular

            policy = get_active_policy()
        except Exception:  # pragma: no cover - bandit optional during tests
            policy = None
        if policy is not None:
            policy_weights = policy.weights()
            if policy_weights:
                weights.update({k: float(policy_weights.get(k, weights.get(k, 0.0))) for k in EXPECTED_KEYS})
                policy_version = policy.version

    weights = _normalise(weights)
    return weights, policy_version


def load_weights(app: Flask) -> Dict[str, float]:
    """Backwards-compatible helper returning only the weight mapping."""
    weights, _ = resolve_weights(app)
    return weights


def _load_from_config(app: Flask) -> Dict[str, float]:
    raw = app.config.get("RISK_WEIGHTS", {})
    base: Dict[str, float] = {key: float(raw.get(key, 0.0)) for key in EXPECTED_KEYS}
    return _normalise(base)


def _normalise(weights: Dict[str, float]) -> Dict[str, float]:
    total = sum(max(value, 0.0) for value in weights.values())
    if total > 0 and total != 1.0:
        weights = {key: (max(value, 0.0) / total) for key, value in weights.items()}
    return weights


__all__ = ["load_weights", "resolve_weights", "EXPECTED_KEYS"]
