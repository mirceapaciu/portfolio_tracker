"""Portfolio-wide XIRR calculation helpers."""

from __future__ import annotations

import argparse
import csv
import logging
import math
import sqlite3
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configuration import DB_PATH
from src.repository.create_db import (
    create_dividend_allocation_t,
    create_security_t,
    create_transaction_match_t,
    create_transaction_t,
)

logger = logging.getLogger(__name__)

OUTFLOW_TYPES = {"buy"}
INFLOW_TYPES = {"sell", "dividend", "interest", "distribution"}
FLOAT_TOLERANCE = 1e-9
SQLITE_PARAM_LIMIT = 999

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
    asset_type_filter: Optional[str] = None,
    debug: bool = False,
    debug_csv_path: str | None = None,
) -> float | None:
    """Return the XIRR of all cash flows for the chosen asset scope."""
    if db_path is None:
        db_path = str(DB_PATH)

    if asset_type_filter is not None and asset_type_filter.lower() == "all":
        asset_type_filter = None

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    create_security_t(cursor)
    create_transaction_t(cursor)
    create_dividend_allocation_t(cursor)

    asset_filter_clause = ""
    dividend_filter_clause = ""
    params: Tuple[str, ...] = ()
    if asset_type_filter:
        asset_filter_clause = """
          AND s.asset_type IS NOT NULL
          AND LOWER(s.asset_type) = LOWER(?)
        """
        dividend_filter_clause = """
          AND sec.asset_type IS NOT NULL
          AND LOWER(sec.asset_type) = LOWER(?)
        """
        params = (asset_type_filter,)
    dividend_params = params

    cursor.execute(
        f"""
         SELECT t.security_id, t.transaction_date, t.transaction_type, t.net_amount, t.total_value,
             t.price_per_share, t.shares, s.security_name
        FROM transaction_t t
        JOIN security_t s ON s.id = t.security_id
        WHERE t.transaction_date IS NOT NULL
          AND LOWER(COALESCE(t.transaction_type, '')) <> 'dividend'
        {asset_filter_clause}
        ORDER BY t.transaction_date, t.id
        """,
        params,
    )
    rows = cursor.fetchall()
    cursor.execute(
        f"""
        SELECT da.allocated_amount,
               div_tx.transaction_date AS dividend_date,
               sec.security_name,
               sec.id AS security_id
        FROM dividend_allocation_t da
        JOIN transaction_t div_tx ON div_tx.id = da.dividend_transaction_id
        JOIN security_t sec ON sec.id = da.security_id
        WHERE div_tx.transaction_date IS NOT NULL
          AND COALESCE(div_tx.allocated, 0) = 1
        {dividend_filter_clause}
        ORDER BY div_tx.transaction_date, da.id
        """,
        dividend_params,
    )
    dividend_rows = cursor.fetchall()
    conn.close()

    scope_label = asset_type_filter or "all asset types"
    if not rows:
        logger.info("No %s transactions available for XIRR", scope_label)
        return None

    cashflows: Dict[date, float] = defaultdict(float)
    cashflow_details: List[Tuple[date, float, str]] = []
    open_positions: Dict[int, Dict[str, float | date | None | str]] = defaultdict(
        lambda: {
            "net_shares": 0.0,
            "last_price": None,
            "last_price_date": None,
            "security_name": None,
        }
    )
    for row in rows:
        tx_date = _to_date(row["transaction_date"])
        if tx_date is None:
            continue
        raw_amount = _coalesce_amount(row)
        amount = _normalize_cashflow(raw_amount, row["transaction_type"])
        if amount == 0.0:
            continue
        cashflows[tx_date] += amount
        security_name = row["security_name"] or f"Security {row['security_id']}"
        cashflow_details.append((tx_date, amount, security_name))

        tx_type = (row["transaction_type"] or "").strip().lower()
        shares = float(row["shares"] or 0.0)
        shares_abs = abs(shares)
        if tx_type in ("buy", "sell") and shares_abs > FLOAT_TOLERANCE:
            position = open_positions[int(row["security_id"])]
            if not position["security_name"]:
                position["security_name"] = security_name
            if tx_type == "buy":
                position["net_shares"] = float(position["net_shares"] or 0.0) + shares_abs
            else:
                position["net_shares"] = float(position["net_shares"] or 0.0) - shares_abs

            price = None
            if shares_abs > FLOAT_TOLERANCE:
                price = abs(raw_amount) / shares_abs
            if (price is None or price <= FLOAT_TOLERANCE) and row["price_per_share"] is not None:
                price = float(row["price_per_share"])
            if price is not None:
                last_date = position["last_price_date"]
                if last_date is None or tx_date >= last_date:
                    position["last_price"] = float(price)
                    position["last_price_date"] = tx_date

    for row in dividend_rows:
        div_date = _to_date(row["dividend_date"])
        if div_date is None:
            continue
        amount = float(row["allocated_amount"] or 0.0)
        if amount == 0.0:
            continue
        security_name = row["security_name"] or f"Security {row['security_id']}"
        cashflows[div_date] += amount
        cashflow_details.append((div_date, amount, f"{security_name} (dividend allocation)"))

    open_valuation_entries: List[Tuple[str, float]] = []
    for security_id, position in open_positions.items():
        net_shares = float(position["net_shares"] or 0.0)
        last_price = position["last_price"]
        if abs(net_shares) <= FLOAT_TOLERANCE or last_price is None:
            continue
        security_name = position.get("security_name") or f"Security {security_id}"
        open_value = net_shares * float(last_price)
        if abs(open_value) <= FLOAT_TOLERANCE:
            continue
        open_valuation_entries.append((security_name, open_value))

    if open_valuation_entries:
        valuation_date = date.today()
        latest_existing = max(cashflows) if cashflows else valuation_date
        if valuation_date < latest_existing:
            valuation_date = latest_existing
        for security_name, value in open_valuation_entries:
            cashflows[valuation_date] += value
            cashflow_details.append(
                (valuation_date, value, f"{security_name} (open position)")
            )

    if not cashflows:
        logger.info("No valid cash flows found for %s", scope_label)
        return None

    ordered_cashflows = sorted(cashflows.items(), key=lambda item: item[0])
    if debug:
        if debug_csv_path:
            _write_cashflow_debug_csv(debug_csv_path, cashflow_details)
        else:
            logger.warning("Debug flag enabled but no CSV path provided; skipping export")
    return _xirr_from_cashflows(ordered_cashflows)


def calculate_portfolio_xirr_closed_positions(
    db_path: str | None = None,
    asset_type_filter: Optional[str] = None,
    debug: bool = False,
    debug_csv_path: str | None = None,
) -> float | None:
    """Return XIRR using only matched BUY↔SELL lots and their dividend allocations."""

    if db_path is None:
        db_path = str(DB_PATH)

    if asset_type_filter is not None and asset_type_filter.lower() == "all":
        asset_type_filter = None

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    create_security_t(cursor)
    create_transaction_t(cursor)
    create_transaction_match_t(cursor)
    create_dividend_allocation_t(cursor)

    asset_filter_clause = ""
    params: Tuple[str, ...] = ()
    if asset_type_filter:
        asset_filter_clause = """
          AND sec.asset_type IS NOT NULL
          AND LOWER(sec.asset_type) = LOWER(?)
        """
        params = (asset_type_filter,)

    cursor.execute(
        f"""
        SELECT tm.buy_transaction_id,
               tm.sell_transaction_id,
               tm.allocated_cost,
               tm.allocated_proceeds,
               bt.transaction_date AS buy_date,
               st.transaction_date AS sell_date,
               sec.security_name
        FROM transaction_match_t tm
        JOIN transaction_t bt ON bt.id = tm.buy_transaction_id
        JOIN transaction_t st ON st.id = tm.sell_transaction_id
        JOIN security_t sec ON sec.id = tm.security_id
        WHERE bt.transaction_date IS NOT NULL
          AND st.transaction_date IS NOT NULL
        {asset_filter_clause}
        ORDER BY bt.transaction_date, tm.id
        """,
        params,
    )
    match_rows = cursor.fetchall()

    scope_label = asset_type_filter or "all asset types"
    if not match_rows:
        logger.info("No matched BUY/SELL lots available for XIRR (%s)", scope_label)
        conn.close()
        return None

    cashflows: Dict[date, float] = defaultdict(float)
    cashflow_details: List[Tuple[date, float, str]] = []
    matched_buy_ids: List[int] = []
    seen_buy_ids: set[int] = set()

    for row in match_rows:
        buy_date = _to_date(row["buy_date"])
        sell_date = _to_date(row["sell_date"])
        if buy_date is None or sell_date is None:
            continue

        security_name = row["security_name"] or f"Security {row['buy_transaction_id']}"

        cost = float(row["allocated_cost"] or 0.0)
        if abs(cost) > FLOAT_TOLERANCE:
            outflow = -abs(cost)
            cashflows[buy_date] += outflow
            cashflow_details.append((buy_date, outflow, f"{security_name} (buy allocation)"))

        proceeds = float(row["allocated_proceeds"] or 0.0)
        if abs(proceeds) > FLOAT_TOLERANCE:
            cashflows[sell_date] += proceeds
            cashflow_details.append((sell_date, proceeds, f"{security_name} (sell allocation)"))

        buy_id = int(row["buy_transaction_id"])
        if buy_id not in seen_buy_ids:
            matched_buy_ids.append(buy_id)
            seen_buy_ids.add(buy_id)

    dividend_filter_clause = ""
    if asset_type_filter:
        dividend_filter_clause = """
          AND sec.asset_type IS NOT NULL
          AND LOWER(sec.asset_type) = LOWER(?)
        """

    if matched_buy_ids:
        buy_id_list = sorted(matched_buy_ids)
        param_overhead = 1 if asset_type_filter else 0
        chunk_limit = max(1, SQLITE_PARAM_LIMIT - param_overhead)
        dividend_rows: List[sqlite3.Row] = []
        for start in range(0, len(buy_id_list), chunk_limit):
            chunk = buy_id_list[start : start + chunk_limit]
            placeholders = ",".join(["?"] * len(chunk))
            cursor.execute(
                f"""
                SELECT da.allocated_amount,
                       div_tx.transaction_date AS dividend_date,
                       sec.security_name
                FROM dividend_allocation_t da
                JOIN transaction_t div_tx ON div_tx.id = da.dividend_transaction_id
                JOIN security_t sec ON sec.id = da.security_id
                WHERE div_tx.transaction_date IS NOT NULL
                {dividend_filter_clause}
                  AND da.buy_transaction_id IN ({placeholders})
                """,
                ((asset_type_filter,) if asset_type_filter else ()) + tuple(chunk),
            )
            dividend_rows.extend(cursor.fetchall())

        for row in dividend_rows:
            div_date = _to_date(row["dividend_date"])
            if div_date is None:
                continue
            amount = float(row["allocated_amount"] or 0.0)
            if abs(amount) <= FLOAT_TOLERANCE:
                continue
            security_name = row["security_name"] or "Dividend"
            cashflows[div_date] += amount
            cashflow_details.append(
                (div_date, amount, f"{security_name} (dividend allocation)")
            )

    conn.close()

    if not cashflows:
        logger.info("No valid cash flows found for matched-lot XIRR (%s)", scope_label)
        return None

    ordered_cashflows = sorted(cashflows.items(), key=lambda item: item[0])
    if debug:
        if debug_csv_path:
            _write_cashflow_debug_csv(debug_csv_path, cashflow_details)
        else:
            logger.warning("Debug flag enabled but no CSV path provided; skipping export")
    return _xirr_from_cashflows(ordered_cashflows)


def _write_cashflow_debug_csv(
    csv_path: str,
    detail_entries: List[Tuple[date, float, str]],
) -> None:
    """Persist detailed cash flow debug data to CSV."""
    output_path = Path(csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["category", "date", "amount", "details"])

        for flow_date, amount, security_name in sorted(
            detail_entries, key=lambda entry: (entry[0], entry[1])
        ):
            writer.writerow(
                [flow_date.isoformat(), f"{amount:.2f}", security_name]
            )
            rows_written += 1

    logger.info(
        "Cash flow debug CSV written to %s (%d rows)", output_path, rows_written
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Portfolio XIRR calculator")
    parser.add_argument(
        "--asset-type",
        default="stock",
        help="Asset type filter (matches security_t.asset_type, default: stock)",
    )
    parser.add_argument(
        "--closed-only",
        action="store_true",
        help="Use only matched BUY/SELL lots (excludes open positions)",
    )
    parser.add_argument(
        "--debug-cashflows",
        action="store_true",
        help="Export every cash flow used to compute XIRR",
    )
    parser.add_argument(
        "--cashflow-debug-csv",
        default="cashflows_debug.csv",
        help="Output CSV path for --debug-cashflows",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    calculator = (
        calculate_portfolio_xirr_closed_positions
        if args.closed_only
        else calculate_portfolio_xirr
    )
    result = calculator(
        asset_type_filter=args.asset_type,
        debug=args.debug_cashflows,
        debug_csv_path=args.cashflow_debug_csv if args.debug_cashflows else None,
    )
    if result is not None:
        print(f"Calculated portfolio XIRR: {result * 100:.2f}%")
    else:
        print("Could not calculate portfolio XIRR")