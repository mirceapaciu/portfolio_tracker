"""Calculate realized gains from normalized transactions.

This module reads unprocessed rows from ``transaction_t`` and creates
entries in ``realized_gain_t`` using the same aggregation approach as the
legacy CSV-based ``create_aggregated_report.py`` script. Transactions are
matched FIFO by broker and security, dividends are linked to the matching
holding period, and used rows are flagged to avoid duplicate processing.
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Tuple

# Make sure the project root is discoverable when executed as a script
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configuration import DB_PATH
from src.repository.create_db import (
    create_broker_t,
    create_realized_gain_t,
    create_security_t,
    create_transaction_t,
)

logger = logging.getLogger(__name__)

TransactionRow = Dict[str, float | int | str | None]


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


def _calculate_cagr(initial_value: float, final_value: float, years: float) -> float:
    """Replicate CAGR logic from ``create_aggregated_report.py``."""
    if initial_value <= 0 or years <= 0:
        return 0.0
    if final_value <= 0:
        return -100.0 * (1 - (abs(final_value) / initial_value) ** (1 / years))
    return ((final_value / initial_value) ** (1 / years) - 1) * 100


def _coalesce_amount(row: sqlite3.Row, prefer_net: bool = True) -> float:
    """Return the best available monetary value for a transaction."""
    if prefer_net and row["net_amount"] is not None:
        return float(row["net_amount"])
    if row["total_value"] is not None:
        return float(row["total_value"])
    if row["price_per_share"] is not None and row["shares"]:
        return float(row["price_per_share"]) * float(row["shares"])
    return 0.0


def calculate_realized_gains(db_path: str | None = None) -> Dict[str, int | float]:
    """Populate ``realized_gain_t`` from unused ``transaction_t`` rows."""
    if db_path is None:
        db_path = str(DB_PATH)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Ensure required tables exist
    create_broker_t(cursor)
    create_security_t(cursor)
    create_transaction_t(cursor)
    create_realized_gain_t(cursor)

    cursor.execute(
        """
        SELECT id, security_id, broker_id, transaction_date, transaction_type,
               shares, price_per_share, total_value, fees, net_amount
        FROM transaction_t
        WHERE used_in_realized_gain = 0
          AND transaction_type IN ('buy', 'sell', 'dividend')
        ORDER BY broker_id, security_id, transaction_date, id
        """
    )
    rows = cursor.fetchall()

    if not rows:
        logger.info("No unused buy/sell/dividend transactions found")
        conn.close()
        return {"positions_created": 0, "transactions_marked": 0}

    grouped: Dict[
        Tuple[int, int],
        Dict[str, List[sqlite3.Row]]
    ] = defaultdict(lambda: {"buy": [], "sell": [], "dividend": []})

    for row in rows:
        key = (row["broker_id"], row["security_id"])
        grouped[key][row["transaction_type"].lower()].append(row)

    aggregated_positions: Dict[
        Tuple[int, int, date, date],
        Dict[str, float | int | date]
    ] = {}
    positions_by_security: Dict[Tuple[int, int], List[Dict[str, float | int | date]]] = defaultdict(list)
    dividends_by_security: Dict[Tuple[int, int], List[Dict[str, float | date | int]]] = {}
    used_transaction_ids: set[int] = set()

    # Prepare dividends collections once per security/broker
    for key, parts in grouped.items():
        dividend_rows = []
        for row in parts["dividend"]:
            div_date = _to_date(row["transaction_date"])
            if div_date is None:
                continue
            dividend_rows.append(
                {
                    "id": row["id"],
                    "date": div_date,
                    "amount": _coalesce_amount(row, prefer_net=False),
                    "shares": abs(float(row["shares"])) if row["shares"] else 0.0,
                }
            )
        dividends_by_security[key] = dividend_rows

    # Match buy/sell pairs FIFO per broker & security
    for key, parts in grouped.items():
        broker_id, security_id = key
        buys = sorted(parts["buy"], key=lambda r: (_to_date(r["transaction_date"]), r["id"]))
        sells = sorted(parts["sell"], key=lambda r: (_to_date(r["transaction_date"]), r["id"]))

        buy_lots = []
        for row in buys:
            buy_date = _to_date(row["transaction_date"])
            shares = abs(float(row["shares"])) if row["shares"] else 0.0
            if buy_date is None or shares <= 0:
                continue
            cost_basis = abs(_coalesce_amount(row))
            cost_per_share = cost_basis / shares if shares else 0.0
            buy_lots.append(
                {
                    "id": row["id"],
                    "buy_date": buy_date,
                    "shares_remaining": shares,
                    "cost_per_share": cost_per_share,
                }
            )

        for sell_row in sells:
            sell_date = _to_date(sell_row["transaction_date"])
            sell_shares = abs(float(sell_row["shares"])) if sell_row["shares"] else 0.0
            if sell_date is None or sell_shares <= 0:
                continue

            proceeds_total = abs(_coalesce_amount(sell_row))
            sell_price_per_share = proceeds_total / sell_shares if sell_shares else 0.0
            shares_remaining = sell_shares

            while shares_remaining > 0 and buy_lots:
                lot = buy_lots[0]
                matched_shares = min(shares_remaining, lot["shares_remaining"])
                invested_value = lot["cost_per_share"] * matched_shares
                realized_pl = sell_price_per_share * matched_shares - invested_value

                agg_key = (broker_id, security_id, lot["buy_date"], sell_date)
                position = aggregated_positions.get(agg_key)
                if not position:
                    position = {
                        "broker_id": broker_id,
                        "security_id": security_id,
                        "buy_date": lot["buy_date"],
                        "sell_date": sell_date,
                        "shares": 0.0,
                        "invested_value": 0.0,
                        "realized_pl": 0.0,
                        "total_dividend": 0.0,
                        "dividend_count": 0,
                        "cagr_percentage": 0.0,
                    }
                    aggregated_positions[agg_key] = position
                    positions_by_security[key].append(position)

                position["shares"] += matched_shares
                position["invested_value"] += invested_value
                position["realized_pl"] += realized_pl

                lot["shares_remaining"] -= matched_shares
                shares_remaining -= matched_shares

                if lot["shares_remaining"] <= 0:
                    used_transaction_ids.add(lot["id"])
                    buy_lots.pop(0)

            if shares_remaining == 0:
                used_transaction_ids.add(sell_row["id"])

    # Match dividends to aggregated positions
    for key, positions in positions_by_security.items():
        dividends = dividends_by_security.get(key, [])
        if not positions:
            continue

        sorted_positions = sorted(positions, key=lambda p: (p["buy_date"], p["sell_date"]))

        for dividend in dividends:
            div_date = dividend["date"]
            amount = float(dividend["amount"] or 0.0)
            if amount == 0.0:
                continue
            eligible_positions = [
                position
                for position in sorted_positions
                if position["buy_date"] <= div_date <= position["sell_date"]
            ]
            if not eligible_positions:
                continue

            declared_shares = float(dividend.get("shares") or 0.0)
            eligible_share_sum = sum((pos["shares"] or 0.0) for pos in eligible_positions)
            total_shares = declared_shares if declared_shares > 0 else eligible_share_sum
            if total_shares <= 0:
                continue

            per_share_amount = amount / total_shares
            shares_left = total_shares
            distributed_amount = 0.0
            for position in eligible_positions:
                if shares_left <= 0:
                    break
                shares = position["shares"] or 0.0
                if shares <= 0:
                    continue
                assign_shares = min(shares, shares_left)
                allocation = assign_shares * per_share_amount
                position["total_dividend"] += allocation
                if assign_shares > 0:
                    position["dividend_count"] += 1
                shares_left -= assign_shares
                distributed_amount += allocation

            if shares_left > 0 and eligible_positions and distributed_amount < amount:
                remainder = amount - distributed_amount
                eligible_positions[-1]["total_dividend"] += remainder

            used_transaction_ids.add(dividend["id"])

        for position in sorted_positions:
            buy_date = position["buy_date"]
            sell_date = position["sell_date"]
            holding_years = max((sell_date - buy_date).days / 365.25, 0.0)
            final_value = (
                position["invested_value"]
                + position["realized_pl"]
                + position["total_dividend"]
            )
            position["cagr_percentage"] = _calculate_cagr(position["invested_value"], final_value, holding_years)

    # Persist aggregated positions
    insert_payload = [
        (
            position["broker_id"],
            position["security_id"],
            position["shares"],
            position["invested_value"],
            position["buy_date"].isoformat(),
            position["sell_date"].isoformat(),
            position["realized_pl"],
            position["total_dividend"],
            position["dividend_count"],
            position["cagr_percentage"],
        )
        for position in aggregated_positions.values()
        if position["shares"] > 0
    ]

    cursor.executemany(
        """
        INSERT INTO realized_gain_t
        (broker_id, security_id, shares, invested_value, buy_date,
         sell_date, p_l, total_dividend, dividend_count, cagr_percentage)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        insert_payload,
    )

    if used_transaction_ids:
        cursor.executemany(
            "UPDATE transaction_t SET used_in_realized_gain = 1 WHERE id = ?",
            [(tx_id,) for tx_id in sorted(used_transaction_ids)],
        )

    conn.commit()
    conn.close()

    logger.info(
        "Created %s realized gain rows and marked %s transactions",
        len(insert_payload),
        len(used_transaction_ids),
    )

    return {
        "positions_created": len(insert_payload),
        "transactions_marked": len(used_transaction_ids),
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    stats = calculate_realized_gains()
    print(
        f"Created {stats['positions_created']} realized gain rows, "
        f"marked {stats['transactions_marked']} source transactions",
    )
