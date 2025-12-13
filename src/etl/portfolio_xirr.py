"""Portfolio-wide XIRR calculation helpers."""

from __future__ import annotations

import logging
import math
import sqlite3
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Tuple

import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configuration import DB_PATH
from src.repository.create_db import create_security_t, create_transaction_t

logger = logging.getLogger(__name__)

OUTFLOW_TYPES = {"buy"}
INFLOW_TYPES = {"sell", "dividend", "interest", "distribution"}

BRACKET_SCAN_POINTS = (
    -0.9999,
    -0.99,
    -0.95,
    -0.9,
    -0.8,
    -0.7,
    -0.6,
    -0.5,
    -0.4,
    -0.3,
    -0.2,
    -0.1,
    -0.05,
    -0.02,
    -0.01,
    0.0,
    0.01,
    0.02,
    0.05,
    0.1,
    0.2,
    0.5,
    1.0,
    2.0,
    5.0,
    10.0,
    20.0,
    50.0,
    100.0,
    200.0,
    500.0,
    1_000.0,
    2_000.0,
    5_000.0,
    10_000.0,
    100_000.0,
    1_000_000.0,
)


def _to_date(value: str | datetime | date | None) -> date | None:
    """Convert SQLite date/text values to ``date`` objects."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return datetime.fromisoformat(value).date()  # type: ignore[arg-type]
    except ValueError as exc:  # pragma: no cover - defensive guard
        logger.error("Invalid date value '%s': %s", value, exc)
        return None


def _coalesce_amount(row: sqlite3.Row, prefer_net: bool = True) -> float:
    """Return the best available monetary value for a transaction."""
    if prefer_net and row["net_amount"] is not None:
        return float(row["net_amount"])
    if row["total_value"] is not None:
        return float(row["total_value"])
    if row["price_per_share"] is not None and row["shares"]:
        return float(row["price_per_share"]) * float(row["shares"])
    return 0.0


def _normalize_cashflow(amount: float, tx_type: str | None) -> float:
    """Force expected signs for in/out flows based on transaction type."""
    normalized_type = (tx_type or "").strip().lower()
    if normalized_type in OUTFLOW_TYPES:
        return -abs(amount)
    if normalized_type in INFLOW_TYPES:
        return abs(amount)
    return amount


def _xirr_from_cashflows(cashflows: List[Tuple[date, float]]) -> float | None:
    """Compute XIRR for dated cashflows using a bounded bisection search."""
    if len(cashflows) < 2:
        return None
    has_positive = any(amount > 0 for _, amount in cashflows)
    has_negative = any(amount < 0 for _, amount in cashflows)
    if not (has_positive and has_negative):
        return None

    start_date = cashflows[0][0]
    timed_flows = [
        ((flow_date - start_date).days / 365.25, amount)
        for flow_date, amount in cashflows
    ]

    def npv(rate: float) -> float:
        factor = 1.0 + rate
        if factor <= 0:
            return math.copysign(math.inf, factor)
        total = 0.0
        for years, amount in timed_flows:
            total += amount / (factor ** years)
        return total

    low = -0.9999
    high = 0.1
    npv_low = npv(low)
    npv_high = npv(high)

    attempts = 0
    while npv_low * npv_high > 0 and attempts < 60 and high < 1e6:
        high *= 2
        npv_high = npv(high)
        attempts += 1

    if not (math.isfinite(npv_low) and math.isfinite(npv_high)):
        return None

    if npv_low * npv_high > 0:
        prev_rate = None
        prev_val = None
        for rate in BRACKET_SCAN_POINTS:
            val = npv(rate)
            if not math.isfinite(val):
                continue
            if abs(val) < 1e-7:
                return rate
            if prev_rate is not None and prev_val is not None and prev_val * val < 0:
                low = prev_rate
                npv_low = prev_val
                high = rate
                npv_high = val
                break
            prev_rate = rate
            prev_val = val
        else:
            return None

    for _ in range(200):
        mid = (low + high) / 2
        npv_mid = npv(mid)
        if not math.isfinite(npv_mid):
            return None
        if abs(npv_mid) < 1e-7:
            return mid
        if npv_low * npv_mid < 0:
            high = mid
            npv_high = npv_mid
        else:
            low = mid
            npv_low = npv_mid

    return (low + high) / 2


def calculate_portfolio_xirr(
    db_path: str | None = None,
    asset_type_filter: str = "stock",
) -> float | None:
    """Return the XIRR of all cash flows for the chosen asset type."""
    if db_path is None:
        db_path = str(DB_PATH)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    create_security_t(cursor)
    create_transaction_t(cursor)

    cursor.execute(
        """
        SELECT t.transaction_date, t.transaction_type, t.net_amount, t.total_value,
               t.price_per_share, t.shares
        FROM transaction_t t
        JOIN security_t s ON s.id = t.security_id
        WHERE t.transaction_date IS NOT NULL
          AND s.asset_type IS NOT NULL
          AND LOWER(s.asset_type) = LOWER(?)
        ORDER BY t.transaction_date, t.id
        """,
        (asset_type_filter,),
    )
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        logger.info("No %s transactions available for XIRR", asset_type_filter)
        return None

    cashflows: Dict[date, float] = defaultdict(float)
    for row in rows:
        tx_date = _to_date(row["transaction_date"])
        if tx_date is None:
            continue
        amount = _coalesce_amount(row)
        amount = _normalize_cashflow(amount, row["transaction_type"])
        if amount == 0.0:
            continue
        cashflows[tx_date] += amount

    if not cashflows:
        logger.info("No valid cash flows found for %s", asset_type_filter)
        return None

    ordered_cashflows = sorted(cashflows.items(), key=lambda item: item[0])
    return _xirr_from_cashflows(ordered_cashflows)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    result = calculate_portfolio_xirr()
    if result is not None:
        print(f"Calculated portfolio XIRR: {result:.2f}%")
    else:
        print("Could not calculate portfolio XIRR")