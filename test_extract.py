"""
tests/test_extract.py
----------------------
Unit tests for extraction and cost calculation logic.
Run: pytest tests/
"""

import pytest
import pandas as pd


# ── Weight / chargeable weight logic ─────────────────────────────────────────

def compute_chargeable_weight(actual: float, volumetric: float) -> float:
    return max(actual or 0, volumetric or 0)


def test_chargeable_weight_uses_volumetric_when_larger():
    assert compute_chargeable_weight(1.5, 3.0) == 3.0


def test_chargeable_weight_uses_actual_when_larger():
    assert compute_chargeable_weight(5.0, 2.0) == 5.0


def test_chargeable_weight_handles_none():
    assert compute_chargeable_weight(None, 2.5) == 2.5
    assert compute_chargeable_weight(1.0, None) == 1.0


# ── Rate slab matching ────────────────────────────────────────────────────────

SAMPLE_RATES = pd.DataFrame([
    {"lane_code": "BLR-DEL", "weight_slab_min": 0.0,  "weight_slab_max": 0.5,  "rate_per_kg": 80.0,  "fuel_surcharge_pct": 10.0},
    {"lane_code": "BLR-DEL", "weight_slab_min": 0.5,  "weight_slab_max": 1.0,  "rate_per_kg": 75.0,  "fuel_surcharge_pct": 10.0},
    {"lane_code": "BLR-DEL", "weight_slab_min": 1.0,  "weight_slab_max": 5.0,  "rate_per_kg": 65.0,  "fuel_surcharge_pct": 10.0},
    {"lane_code": "BLR-DEL", "weight_slab_min": 5.0,  "weight_slab_max": 10.0, "rate_per_kg": 55.0,  "fuel_surcharge_pct": 10.0},
])


def get_rate(lane_code: str, weight_kg: float, rates_df: pd.DataFrame):
    match = rates_df[
        (rates_df["lane_code"] == lane_code) &
        (rates_df["weight_slab_min"] <= weight_kg) &
        (rates_df["weight_slab_max"] > weight_kg)
    ]
    return match.iloc[0] if not match.empty else None


def test_rate_slab_matches_correct_band():
    rate = get_rate("BLR-DEL", 3.0, SAMPLE_RATES)
    assert rate is not None
    assert rate["rate_per_kg"] == 65.0


def test_rate_slab_boundary_upper():
    # weight exactly at slab boundary should go to NEXT slab (< not <=)
    rate = get_rate("BLR-DEL", 5.0, SAMPLE_RATES)
    assert rate["rate_per_kg"] == 55.0


def test_rate_slab_no_match_returns_none():
    rate = get_rate("BLR-DEL", 100.0, SAMPLE_RATES)   # beyond max slab
    assert rate is None


# ── Cost calculation ──────────────────────────────────────────────────────────

def calculate_air_cost(weight_kg: float, rate_per_kg: float, fuel_surcharge_pct: float) -> dict:
    base = round(weight_kg * rate_per_kg, 2)
    fuel = round(base * fuel_surcharge_pct / 100, 2)
    return {"base_cost": base, "fuel_surcharge": fuel, "total": round(base + fuel, 2)}


def test_cost_calculation_basic():
    result = calculate_air_cost(2.0, 65.0, 10.0)
    assert result["base_cost"] == 130.0
    assert result["fuel_surcharge"] == 13.0
    assert result["total"] == 143.0


def test_cost_zero_weight():
    result = calculate_air_cost(0.0, 65.0, 10.0)
    assert result["total"] == 0.0
