"""Create BUY↔SELL allocation rows in ``transaction_match_t``.

This module performs FIFO matching per (broker_id, security_id) using rows from
``transaction_t`` (types: buy/sell). It writes one row per partial allocation into
``transaction_match_t`` and can be re-run incrementally—only unmatched shares are
allocated when existing rows are present.

Notes on amounts:
- ``allocated_cost`` is based on BUY cash outflow per share (uses ``net_amount`` when present).
- ``allocated_proceeds`` is based on SELL net proceeds per share (uses ``net_amount`` when present).
- ``allocated_fees`` is a pro-rata allocation of SELL fees for that match.

You can run this module directly:
  python -m src.etl.create_transaction_matches --clear
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configuration import DB_PATH
from src.etl.portfolio_xirr import _coalesce_amount, _to_date
from src.repository.create_db import (
    create_broker_t,
    create_security_t,
    create_transaction_match_t,
    create_transaction_t,
)

logger = logging.getLogger(__name__)

FLOAT_TOLERANCE = 1e-9


@dataclass
class BuyLot:
    tx_id: int
    buy_date: date
    shares_remaining: float
    cost_per_share: float


def _abs_float(value: float | int | None) -> float:
    if value is None:
        return 0.0
    return abs(float(value))


def _load_existing_allocations(
    cursor: sqlite3.Cursor,
    column_name: str,
    ids: List[int],
) -> Dict[int, float]:
    if not ids:
        return {}
    placeholders = ",".join(["?"] * len(ids))
    query = (
        f"SELECT {column_name} AS tx_id, COALESCE(SUM(shares), 0) AS matched_shares "
        f"FROM transaction_match_t WHERE {column_name} IN ({placeholders}) GROUP BY {column_name}"
    )
    cursor.execute(query, ids)
    return {int(row["tx_id"]): float(row["matched_shares"] or 0.0) for row in cursor.fetchall()}


def create_transaction_matches(
    db_path: str | None = None,
    *,
    clear_existing: bool = False,
    only_unused: bool = False,
    cost_basis_method: str = "FIFO",
) -> Dict[str, int | float]:
    """Populate ``transaction_match_t`` with FIFO allocations from ``transaction_t``."""
    if db_path is None:
        db_path = str(DB_PATH)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    create_broker_t(cursor)
    create_security_t(cursor)
    create_transaction_t(cursor)
    create_transaction_match_t(cursor)

    if clear_existing:
        cursor.execute("DELETE FROM transaction_match_t")

    where_clauses = ["transaction_type IN ('buy','sell')", "transaction_date IS NOT NULL"]
    if only_unused:
        where_clauses.append("used_in_realized_gain = 0")

    cursor.execute(
        f"""
        SELECT id, security_id, broker_id, transaction_date, transaction_type,
               shares, total_value, fees, net_amount
        FROM transaction_t
        WHERE {' AND '.join(where_clauses)}
        ORDER BY broker_id, security_id, transaction_date, id
        """
    )
    rows = cursor.fetchall()

    if not rows:
        conn.close()
        return {"matches_created": 0, "unmatched_sell_shares": 0, "groups_processed": 0}

    grouped: Dict[Tuple[int, int], Dict[str, List[sqlite3.Row]]] = defaultdict(
        lambda: {"buy": [], "sell": []}
    )
    for row in rows:
        key = (int(row["broker_id"]), int(row["security_id"]))
        grouped[key][str(row["transaction_type"]).lower()].append(row)

    matches_created = 0
    unmatched_sell_shares = 0.0

    for (broker_id, security_id), parts in grouped.items():
        buys = sorted(parts["buy"], key=lambda r: (_to_date(r["transaction_date"]), r["id"]))
        sells = sorted(parts["sell"], key=lambda r: (_to_date(r["transaction_date"]), r["id"]))

        buy_ids = [int(row["id"]) for row in buys]
        sell_ids = [int(row["id"]) for row in sells]
        buy_allocations = _load_existing_allocations(cursor, "buy_transaction_id", buy_ids)
        sell_allocations = _load_existing_allocations(cursor, "sell_transaction_id", sell_ids)

        buy_lots: List[BuyLot] = []
        for buy_row in buys:
            buy_dt = _to_date(buy_row["transaction_date"])
            shares = _abs_float(buy_row["shares"])
            if buy_dt is None or shares <= 0:
                continue

            cost_basis_total = abs(_coalesce_amount(buy_row))
            cost_per_share = cost_basis_total / shares if shares else 0.0
            allocated = buy_allocations.get(int(buy_row["id"]), 0.0)
            shares_remaining = shares - allocated
            if shares_remaining <= FLOAT_TOLERANCE:
                continue
            buy_lots.append(
                BuyLot(
                    tx_id=int(buy_row["id"]),
                    buy_date=buy_dt,
                    shares_remaining=shares_remaining,
                    cost_per_share=cost_per_share,
                )
            )

        for sell_row in sells:
            sell_dt = _to_date(sell_row["transaction_date"])
            sell_tx_id = int(sell_row["id"])
            sell_shares_total = _abs_float(sell_row["shares"])
            if sell_dt is None or sell_shares_total <= 0:
                continue

            already_matched = sell_allocations.get(sell_tx_id, 0.0)
            shares_to_match = sell_shares_total - already_matched
            if shares_to_match <= FLOAT_TOLERANCE:
                continue

            proceeds_total = abs(_coalesce_amount(sell_row))
            proceeds_per_share = proceeds_total / sell_shares_total if sell_shares_total else 0.0

            sell_fees_total = _abs_float(sell_row["fees"])
            sell_fee_per_share = sell_fees_total / sell_shares_total if sell_shares_total else 0.0
            while shares_to_match > FLOAT_TOLERANCE and buy_lots:
                lot = buy_lots[0]
                matched_shares = min(shares_to_match, lot.shares_remaining)

                allocated_cost = lot.cost_per_share * matched_shares
                allocated_proceeds = proceeds_per_share * matched_shares
                allocated_fees = sell_fee_per_share * matched_shares

                cursor.execute(
                    """
                    INSERT INTO transaction_match_t
                    (broker_id, security_id, buy_transaction_id, sell_transaction_id,
                     shares, allocated_cost, allocated_proceeds, allocated_fees, cost_basis_method)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        broker_id,
                        security_id,
                        lot.tx_id,
                        sell_tx_id,
                        matched_shares,
                        allocated_cost,
                        allocated_proceeds,
                        allocated_fees,
                        cost_basis_method,
                    ),
                )
                matches_created += 1

                lot.shares_remaining -= matched_shares
                shares_to_match -= matched_shares
                if lot.shares_remaining <= FLOAT_TOLERANCE:
                    buy_lots.pop(0)

            if shares_to_match > FLOAT_TOLERANCE:
                unmatched_sell_shares += shares_to_match

    conn.commit()
    conn.close()

    return {
        "matches_created": matches_created,
        "unmatched_sell_shares": float(unmatched_sell_shares),
        "groups_processed": len(grouped),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Create transaction_match_t FIFO allocations")
    parser.add_argument(
        "--db",
        default=None,
        help=f"Path to SQLite DB (default: {DB_PATH})",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Delete existing rows from transaction_match_t before inserting new ones",
    )
    parser.add_argument(
        "--only-unused",
        action="store_true",
        help="Only match transaction_t rows where used_in_realized_gain = 0",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    stats = create_transaction_matches(
        db_path=args.db,
        clear_existing=args.clear,
        only_unused=args.only_unused,
        cost_basis_method="FIFO",
    )

    print(
        "Created {matches_created} match rows across {groups_processed} groups; "
        "unmatched sell shares: {unmatched_sell_shares}".format(**stats)
    )


if __name__ == "__main__":
    main()
