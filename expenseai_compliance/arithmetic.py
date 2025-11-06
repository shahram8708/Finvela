"""Numeric helpers for invoice arithmetic verification."""
from __future__ import annotations

from decimal import ROUND_DOWN, ROUND_HALF_EVEN, ROUND_HALF_UP, Decimal, getcontext
from typing import Dict, Iterable, List, Tuple

from flask import current_app

ROUNDING_MODES = {
    "ROUND_HALF_UP": ROUND_HALF_UP,
    "ROUND_HALF_EVEN": ROUND_HALF_EVEN,
    "ROUND_DOWN": ROUND_DOWN,
}


def _quantize(value: Decimal, places: int, rounding) -> Decimal:
    quantizer = Decimal("1" if places == 0 else f"1.{'0' * places}")
    return value.quantize(quantizer, rounding=rounding)


def _get_rounding() -> Tuple[int, str]:
    places = current_app.config.get("ARITH_DECIMAL_PLACES", 2)
    mode_key = current_app.config.get("ARITH_ROUNDING_MODE", "ROUND_HALF_UP")
    rounding = ROUNDING_MODES.get(mode_key, ROUND_HALF_UP)
    return places, rounding


def recompute_line_totals(qty, unit_price, gst_rate) -> Tuple[Decimal, Decimal, Decimal]:
    """Recompute line-level totals using configured rounding policies."""
    places, rounding = _get_rounding()
    quantity = Decimal(qty or 0)
    price = Decimal(unit_price or 0)
    rate = Decimal(gst_rate or 0) / Decimal(100)

    subtotal = _quantize(quantity * price, places, rounding)
    tax = _quantize(subtotal * rate, places, rounding)
    total = _quantize(subtotal + tax, places, rounding)
    return subtotal, tax, total


def recompute_invoice_totals(lines: Iterable[Dict[str, Decimal | None]]) -> Tuple[Decimal, Decimal, Decimal, Dict[str, List[Dict[str, Decimal]]]]:
    """Aggregate invoice totals and capture line-level diffs."""
    places, rounding = _get_rounding()
    subtotal = Decimal(0)
    tax_total = Decimal(0)
    grand_total = Decimal(0)
    diffs: List[Dict[str, Decimal]] = []
    for line in lines:
        line_no = line.get("line_no")
        qty = line.get("qty") or Decimal(0)
        unit_price = line.get("unit_price") or Decimal(0)
        gst_rate = line.get("gst_rate") or Decimal(0)
        stored_subtotal = Decimal(line.get("line_subtotal") or 0)
        stored_tax = Decimal(line.get("line_tax") or 0)
        stored_total = Decimal(line.get("line_total") or 0)
        expected_subtotal, expected_tax, expected_total = recompute_line_totals(qty, unit_price, gst_rate)
        subtotal += expected_subtotal
        tax_total += expected_tax
        grand_total += expected_total
        diffs.append(
            {
                "line_no": line_no,
                "expected_subtotal": expected_subtotal,
                "expected_tax": expected_tax,
                "expected_total": expected_total,
                "subtotal_diff": stored_subtotal - expected_subtotal,
                "tax_diff": stored_tax - expected_tax,
                "total_diff": stored_total - expected_total,
            }
        )

    subtotal = _quantize(subtotal, places, rounding)
    tax_total = _quantize(tax_total, places, rounding)
    grand_total = _quantize(grand_total, places, rounding)
    return subtotal, tax_total, grand_total, {"lines": diffs}
