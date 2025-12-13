"""Calculate realized gains from normalized transactions.

This module reads allocation rows from ``transaction_match_t`` (BUY ↔ SELL
matching) and creates entries in ``realized_gain_t`` using the same
aggregation approach as the legacy CSV-based ``create_aggregated_report.py``
script.

Dividends are sourced from ``transaction_t`` (type: dividend) and linked to
the matching holding period. Transactions are flagged via
``transaction_t.used_in_realized_gain`` to avoid duplicate processing.
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from datetime import date
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
    create_transaction_match_t,
    create_transaction_t,
)
from src.etl.portfolio_xirr import _coalesce_amount, _to_date

logger = logging.getLogger(__name__)

TransactionRow = Dict[str, float | int | str | None]
FLOAT_TOLERANCE = 1e-9


def _calculate_cagr(initial_value: float, final_value: float, years: float) -> float:
    """Replicate CAGR logic from ``create_aggregated_report.py``."""
    if initial_value <= 0 or years <= 0:
        return 0.0
    if final_value <= 0:
        return -100.0 * (1 - (abs(final_value) / initial_value) ** (1 / years))
    return ((final_value / initial_value) ** (1 / years) - 1) * 100


def calculate_realized_gains(db_path: str | None = None) -> Dict[str, int | float]:
    """Populate ``realized_gain_t`` from unused, fully-matched SELL allocations."""
    if db_path is None:
        db_path = str(DB_PATH)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Ensure required tables exist
    create_broker_t(cursor)
    create_security_t(cursor)
    create_transaction_t(cursor)
    create_transaction_match_t(cursor)
    create_realized_gain_t(cursor)

    # Process only SELL transactions that are fully allocated in transaction_match_t.
    cursor.execute(
        """
        SELECT s.id AS sell_id,
               ABS(COALESCE(s.shares, 0)) AS sell_shares,
               COALESCE(SUM(m.shares), 0) AS matched_shares
        FROM transaction_t s
        LEFT JOIN transaction_match_t m ON m.sell_transaction_id = s.id
        WHERE s.used_in_realized_gain = 0
          AND s.transaction_type = 'sell'
        GROUP BY s.id
        """
    )
    sell_rows = cursor.fetchall()
    eligible_sell_ids: List[int] = []
    for row in sell_rows:
        sell_shares = float(row["sell_shares"] or 0.0)
        matched_shares = float(row["matched_shares"] or 0.0)
        if sell_shares > 0 and matched_shares + FLOAT_TOLERANCE >= sell_shares:
            eligible_sell_ids.append(int(row["sell_id"]))

    if not eligible_sell_ids:
        logger.info("No unused fully-matched sell transactions found")
        conn.close()
        return {"positions_created": 0, "transactions_marked": 0}

    placeholders = ",".join(["?"] * len(eligible_sell_ids))
    cursor.execute(
        f"""
        SELECT m.id AS match_id,
               m.broker_id,
               m.security_id,
               m.buy_transaction_id,
               m.sell_transaction_id,
               m.shares,
               m.allocated_cost,
               m.allocated_proceeds,
               bt.transaction_date AS buy_date,
               st.transaction_date AS sell_date
        FROM transaction_match_t m
        JOIN transaction_t bt ON bt.id = m.buy_transaction_id
        JOIN transaction_t st ON st.id = m.sell_transaction_id
        WHERE m.sell_transaction_id IN ({placeholders})
        ORDER BY m.broker_id, m.security_id, st.transaction_date, bt.transaction_date, m.id
        """,
        eligible_sell_ids,
    )
    match_rows = cursor.fetchall()

    if not match_rows:
        logger.info("No allocation rows found in transaction_match_t for eligible sells")
        conn.close()
        return {"positions_created": 0, "transactions_marked": 0}

    aggregated_positions: Dict[
        Tuple[int, int, date, date],
        Dict[str, float | int | date]
    ] = {}
    positions_by_security: Dict[Tuple[int, int], List[Dict[str, float | int | date]]] = defaultdict(list)
    used_transaction_ids: set[int] = set()

    # Aggregate realized positions from allocation rows
    for row in match_rows:
        broker_id = int(row["broker_id"])
        security_id = int(row["security_id"])
        buy_date = _to_date(row["buy_date"])
        sell_date = _to_date(row["sell_date"])
        matched_shares = abs(float(row["shares"])) if row["shares"] else 0.0
        if buy_date is None or sell_date is None or matched_shares <= 0:
            continue

        invested_value = float(row["allocated_cost"] or 0.0)
        proceeds_value = float(row["allocated_proceeds"] or 0.0)
        realized_pl = proceeds_value - invested_value

        key = (broker_id, security_id)
        agg_key = (broker_id, security_id, buy_date, sell_date)
        position = aggregated_positions.get(agg_key)
        if not position:
            position = {
                "broker_id": broker_id,
                "security_id": security_id,
                "buy_date": buy_date,
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

    # Fetch unused dividends and match them to aggregated positions
    cursor.execute(
        """
        SELECT id, security_id, broker_id, transaction_date, shares, total_value, net_amount, price_per_share
        FROM transaction_t
        WHERE used_in_realized_gain = 0
          AND transaction_type = 'dividend'
        ORDER BY broker_id, security_id, transaction_date, id
        """
    )
    dividend_source_rows = cursor.fetchall()
    dividends_by_security: Dict[Tuple[int, int], List[Dict[str, float | date | int]]] = defaultdict(list)
    relevant_keys = set(positions_by_security.keys())
    for row in dividend_source_rows:
        key = (int(row["broker_id"]), int(row["security_id"]))
        if key not in relevant_keys:
            continue
        div_date = _to_date(row["transaction_date"])
        if div_date is None:
            continue
        dividends_by_security[key].append(
            {
                "id": int(row["id"]),
                "date": div_date,
                "amount": _coalesce_amount(row, prefer_net=False),
                "shares": abs(float(row["shares"])) if row["shares"] else 0.0,
            }
        )

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

    # Mark eligible sells as used.
    for sell_id in eligible_sell_ids:
        used_transaction_ids.add(sell_id)

        # Mark BUY transactions as used only when fully allocated to sells that are already
        # processed, or are processed in this run.
        sell_placeholders = ",".join(["?"] * len(eligible_sell_ids))
        cursor.execute(
                f"""
                SELECT b.id AS buy_id,
                             ABS(COALESCE(b.shares, 0)) AS buy_shares,
                             COALESCE(SUM(m.shares), 0) AS matched_shares
                FROM transaction_t b
                JOIN transaction_match_t m ON m.buy_transaction_id = b.id
                JOIN transaction_t s ON s.id = m.sell_transaction_id
                WHERE b.used_in_realized_gain = 0
                    AND b.transaction_type = 'buy'
                    AND (
                        s.used_in_realized_gain = 1
                        OR s.id IN ({sell_placeholders})
                    )
                GROUP BY b.id
                """,
                eligible_sell_ids,
        )
    buy_allocation_rows = cursor.fetchall()
    for row in buy_allocation_rows:
        buy_shares = float(row["buy_shares"] or 0.0)
        matched_shares = float(row["matched_shares"] or 0.0)
        if buy_shares > 0 and matched_shares + FLOAT_TOLERANCE >= buy_shares:
            used_transaction_ids.add(int(row["buy_id"]))

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
