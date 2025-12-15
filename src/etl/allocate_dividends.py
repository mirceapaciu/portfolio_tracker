"""Allocate dividend transactions to the buy lots that earned them.

This ETL consumes normalized data in ``transaction_t`` and ``transaction_match_t``
(FIFO allocations) and persists per-lot dividend splits inside
``dividend_allocation_t``. XIRR and other analytics can then rely on those
validated allocations instead of raw dividend rows.
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Tuple

import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configuration import DB_PATH
from src.etl.portfolio_xirr import FLOAT_TOLERANCE, _coalesce_amount, _to_date
from src.repository.create_db import (
    create_broker_t,
    create_dividend_allocation_t,
    create_security_t,
    create_transaction_match_t,
    create_transaction_t,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HoldingSegment:
    """Represents a block of shares held between buy and sell dates."""

    buy_transaction_id: int
    broker_id: int
    security_id: int
    buy_date: date
    sell_date: date | None
    shares: float


def _load_match_segments(cursor: sqlite3.Cursor) -> Tuple[
    DefaultDict[Tuple[int, int], List[HoldingSegment]],
    Dict[int, float],
]:
    """Return matched share segments and their totals per BUY transaction."""

    segments: DefaultDict[Tuple[int, int], List[HoldingSegment]] = defaultdict(list)
    matched_shares: Dict[int, float] = defaultdict(float)

    cursor.execute(
        """
        SELECT m.broker_id,
               m.security_id,
               m.buy_transaction_id,
               m.shares,
               bt.transaction_date AS buy_date,
               st.transaction_date AS sell_date
        FROM transaction_match_t m
        JOIN transaction_t bt ON bt.id = m.buy_transaction_id
        JOIN transaction_t st ON st.id = m.sell_transaction_id
        ORDER BY m.broker_id, m.security_id, bt.transaction_date, st.transaction_date, m.id
        """
    )
    for row in cursor.fetchall():
        shares = abs(float(row["shares"] or 0.0))
        if shares <= FLOAT_TOLERANCE:
            continue
        buy_date = _to_date(row["buy_date"])
        sell_date = _to_date(row["sell_date"])
        if buy_date is None or sell_date is None:
            continue
        buy_id = int(row["buy_transaction_id"])
        broker_id = int(row["broker_id"])
        security_id = int(row["security_id"])
        segments[(broker_id, security_id)].append(
            HoldingSegment(
                buy_transaction_id=buy_id,
                broker_id=broker_id,
                security_id=security_id,
                buy_date=buy_date,
                sell_date=sell_date,
                shares=shares,
            )
        )
        matched_shares[buy_id] = matched_shares.get(buy_id, 0.0) + shares

    return segments, matched_shares


def _add_open_segments(
    cursor: sqlite3.Cursor,
    segments: DefaultDict[Tuple[int, int], List[HoldingSegment]],
    matched_shares: Dict[int, float],
) -> None:
    """Append unmatched BUY shares as open-ended holding segments."""

    cursor.execute(
        """
        SELECT id, broker_id, security_id, transaction_date, shares
        FROM transaction_t
        WHERE transaction_type = 'buy'
        ORDER BY broker_id, security_id, transaction_date, id
        """
    )
    for row in cursor.fetchall():
        total_shares = abs(float(row["shares"] or 0.0))
        if total_shares <= FLOAT_TOLERANCE:
            continue
        buy_date = _to_date(row["transaction_date"])
        if buy_date is None:
            continue
        buy_id = int(row["id"])
        matched = matched_shares.get(buy_id, 0.0)
        remaining = total_shares - matched
        if remaining <= FLOAT_TOLERANCE:
            continue
        broker_id = int(row["broker_id"])
        security_id = int(row["security_id"])
        segments[(broker_id, security_id)].append(
            HoldingSegment(
                buy_transaction_id=buy_id,
                broker_id=broker_id,
                security_id=security_id,
                buy_date=buy_date,
                sell_date=None,
                shares=remaining,
            )
        )


def _sort_segments(segments: DefaultDict[Tuple[int, int], List[HoldingSegment]]) -> None:
    for key in segments:
        segments[key].sort(
            key=lambda seg: (
                seg.buy_date,
                seg.sell_date if seg.sell_date is not None else date.max,
            )
        )


def _eligible_segments(
    all_segments: Iterable[HoldingSegment],
    dividend_date: date,
    previous_dividend_date: date | None,
) -> List[HoldingSegment]:
    """Return segments that were open between previous and current dividend dates."""

    eligible: List[HoldingSegment] = []
    prev_cutoff = previous_dividend_date or date.min
    for segment in all_segments:
        if segment.buy_date >= dividend_date:
            continue
        sell_date = segment.sell_date
        if sell_date is None or sell_date >= dividend_date or sell_date > prev_cutoff:
            eligible.append(segment)
    return eligible


def _record_error(cursor: sqlite3.Cursor, dividend_id: int, message: str) -> None:
    logger.warning("Dividend %s skipped: %s", dividend_id, message)
    cursor.execute(
        "UPDATE transaction_t SET error_message = ?, allocated = 0 WHERE id = ?",
        (message[:500], dividend_id),
    )


def _allocate_for_dividend(
    cursor: sqlite3.Cursor,
    dividend_row: sqlite3.Row,
    segments_by_security: Dict[Tuple[int, int], List[HoldingSegment]],
    dividend_date: date,
    previous_dividend_date: date | None,
) -> Tuple[bool, int]:
    dividend_id = int(dividend_row["id"])
    broker_id = int(dividend_row["broker_id"])
    security_id = int(dividend_row["security_id"])

    amount = _coalesce_amount(dividend_row, prefer_net=False)
    if abs(amount) <= FLOAT_TOLERANCE:
        _record_error(cursor, dividend_id, "Dividend amount is zero")
        return False, 0

    key = (broker_id, security_id)
    segments = segments_by_security.get(key, [])
    eligible_segments = _eligible_segments(segments, dividend_date, previous_dividend_date)
    if not eligible_segments:
        _record_error(cursor, dividend_id, "No eligible holdings on dividend date")
        return False, 0

    available_shares = sum(segment.shares for segment in eligible_segments)
    declared_shares = abs(float(dividend_row["shares"] or 0.0))
    total_shares = declared_shares if declared_shares > FLOAT_TOLERANCE else available_shares
    if total_shares <= FLOAT_TOLERANCE:
        _record_error(cursor, dividend_id, "No share count available for allocation")
        return False, 0

    per_share_amount = amount / total_shares
    shares_left = total_shares
    distributed_amount = 0.0
    allocations: Dict[int, Dict[str, float]] = {}
    allocation_order: List[int] = []

    for segment in eligible_segments:
        if declared_shares > FLOAT_TOLERANCE and shares_left <= FLOAT_TOLERANCE:
            break
        shares_to_use = segment.shares
        if declared_shares > FLOAT_TOLERANCE:
            shares_to_use = min(segment.shares, shares_left)
        if shares_to_use <= FLOAT_TOLERANCE:
            continue
        allocation_value = shares_to_use * per_share_amount
        buy_id = segment.buy_transaction_id
        bucket = allocations.get(buy_id)
        if bucket is None:
            bucket = {"shares": 0.0, "amount": 0.0}
            allocations[buy_id] = bucket
            allocation_order.append(buy_id)
        bucket["shares"] += shares_to_use
        bucket["amount"] += allocation_value
        shares_left -= shares_to_use
        distributed_amount += allocation_value

    if not allocations:
        _record_error(cursor, dividend_id, "Could not distribute dividend across holdings")
        return False, 0

    if shares_left > FLOAT_TOLERANCE and distributed_amount < amount:
        remainder = amount - distributed_amount
        if allocation_order:
            last_bucket = allocations[allocation_order[-1]]
            last_bucket["amount"] += remainder
        distributed_amount += remainder

    cursor.execute(
        "DELETE FROM dividend_allocation_t WHERE dividend_transaction_id = ?",
        (dividend_id,),
    )
    cursor.executemany(
        """
        INSERT INTO dividend_allocation_t
        (broker_id, security_id, dividend_transaction_id, buy_transaction_id, shares, allocated_amount)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                broker_id,
                security_id,
                dividend_id,
                buy_id,
                allocation["shares"],
                allocation["amount"],
            )
            for buy_id, allocation in allocations.items()
        ],
    )
    cursor.execute(
        "UPDATE transaction_t SET allocated = 1, error_message = NULL WHERE id = ?",
        (dividend_id,),
    )
    return True, len(allocations)


def _load_unallocated_dividends(cursor: sqlite3.Cursor) -> List[sqlite3.Row]:
    cursor.execute(
        """
        SELECT id, broker_id, security_id, transaction_date, shares,
               total_value, net_amount, price_per_share
        FROM transaction_t
        WHERE transaction_type = 'dividend'
          AND COALESCE(allocated, 0) = 0
        ORDER BY broker_id, security_id, transaction_date, id
        """
    )
    return cursor.fetchall()


def allocate_dividends(db_path: str | None = None) -> Dict[str, int]:
    """Allocate every unallocated dividend to the correct buy lots."""

    if db_path is None:
        db_path = str(DB_PATH)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    create_broker_t(cursor)
    create_security_t(cursor)
    create_transaction_t(cursor)
    create_transaction_match_t(cursor)
    create_dividend_allocation_t(cursor)

    segments, matched_shares = _load_match_segments(cursor)
    _add_open_segments(cursor, segments, matched_shares)
    _sort_segments(segments)

    dividend_rows = _load_unallocated_dividends(cursor)
    stats = {
        "dividends_processed": len(dividend_rows),
        "dividends_allocated": 0,
        "dividends_failed": 0,
        "allocations_created": 0,
    }
    previous_dividend_dates: Dict[Tuple[int, int], date] = {}

    if not dividend_rows:
        logger.info("No unallocated dividends found")
        conn.commit()
        conn.close()
        return stats

    for dividend_row in dividend_rows:
        dividend_date = _to_date(dividend_row["transaction_date"])
        if dividend_date is None:
            _record_error(cursor, int(dividend_row["id"]), "Missing transaction_date on dividend")
            stats["dividends_failed"] += 1
            continue

        key = (int(dividend_row["broker_id"]), int(dividend_row["security_id"]))
        previous_dividend_date = previous_dividend_dates.get(key)

        success, allocation_count = _allocate_for_dividend(
            cursor,
            dividend_row,
            segments,
            dividend_date,
            previous_dividend_date,
        )
        previous_dividend_dates[key] = dividend_date

        if success:
            stats["dividends_allocated"] += 1
            stats["allocations_created"] += allocation_count
        else:
            stats["dividends_failed"] += 1

    conn.commit()
    conn.close()

    logger.info(
        "Allocated %s dividends (%s rows), %s failed",
        stats["dividends_allocated"],
        stats["allocations_created"],
        stats["dividends_failed"],
    )

    return stats


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Allocate dividends to buy lots")
    parser.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="Path to the SQLite database (default: configuration.DB_PATH)",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    args = _parse_args()
    stats = allocate_dividends(db_path=args.db_path)
    print(
        "Processed {dividends_processed} dividends, allocated {dividends_allocated}, "
        "failed {dividends_failed}".format(**stats)
    )


if __name__ == "__main__":
    main()
