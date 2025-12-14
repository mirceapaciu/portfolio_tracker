"""Helpers that power the Streamlit portfolio overview."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from configuration import DB_PATH
from src.etl.portfolio_xirr import _to_date, calculate_portfolio_xirr

logger = logging.getLogger(__name__)
FLOAT_TOLERANCE = 1e-9


@dataclass(frozen=True)
class PositionSnapshot:
    """Represents one open position with its latest price."""

    security_id: int
    security_name: str
    net_shares: float
    last_price: Optional[float]
    last_price_date: Optional[date]

    @property
    def valuation(self) -> Optional[float]:
        if self.last_price is None:
            return None
        return self.net_shares * self.last_price


@dataclass(frozen=True)
class OpenPositionSummary:
    total_value: float
    position_count: int
    priced_position_count: int
    positions: Sequence[PositionSnapshot]


def database_ready(db_path: str | Path | None = None) -> bool:
    path = _resolve_db_path(db_path)
    return path.exists() and path.is_file()


def get_asset_type_options(db_path: str | Path | None = None) -> List[str]:
    path = _resolve_db_path(db_path)
    if not path.exists():
        return ["stock"]

    with sqlite3.connect(path) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT LOWER(asset_type)
            FROM security_t
            WHERE asset_type IS NOT NULL AND TRIM(asset_type) <> ''
            ORDER BY LOWER(asset_type)
            """
        ).fetchall()

    options = [row[0] for row in rows if row[0]]
    return options or ["stock"]


def get_open_positions_summary(
    *,
    db_path: str | Path | None = None,
    asset_type_filter: Optional[str] = None,
) -> OpenPositionSummary:
    path = _ensure_existing_db(db_path)

    query = """
    WITH typed AS (
        SELECT security_id,
               transaction_type,
               ABS(COALESCE(shares, 0)) AS shares
        FROM transaction_t
        WHERE transaction_type IN ('buy', 'sell')
    ),
    net_positions AS (
        SELECT security_id,
               SUM(CASE WHEN transaction_type = 'buy' THEN shares ELSE -shares END) AS net_shares
        FROM typed
        GROUP BY security_id
        HAVING SUM(CASE WHEN transaction_type = 'buy' THEN shares ELSE -shares END) > :threshold
        ),
        last_trade AS (
            SELECT security_id,
                   unit_price,
                   transaction_date,
                   ROW_NUMBER() OVER (PARTITION BY security_id ORDER BY transaction_date DESC, id DESC) AS rn
            FROM (
             SELECT security_id,
                 transaction_date,
                 id,
                 CASE
                  WHEN ABS(COALESCE(total_value, 0)) > 0 AND ABS(COALESCE(shares, 0)) > 0 THEN ABS(total_value) / ABS(shares)
                  WHEN ABS(COALESCE(net_amount, 0)) > 0 AND ABS(COALESCE(shares, 0)) > 0 THEN ABS(net_amount) / ABS(shares)
                  ELSE price_per_share
                 END AS unit_price
             FROM transaction_t
             WHERE transaction_type IN ('buy', 'sell')
            ) AS latest_raw
        )
    SELECT np.security_id,
           s.security_name,
           np.net_shares,
            lt.unit_price,
           lt.transaction_date
    FROM net_positions np
    JOIN security_t s ON s.id = np.security_id
    LEFT JOIN last_trade lt ON lt.security_id = np.security_id AND lt.rn = 1
    WHERE (
        :asset_type IS NULL
        OR (
            s.asset_type IS NOT NULL
            AND LOWER(s.asset_type) = LOWER(:asset_type)
        )
    )
    ORDER BY s.security_name COLLATE NOCASE
    """

    params = {
        "threshold": FLOAT_TOLERANCE,
        "asset_type": asset_type_filter,
    }

    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()

    positions = _rows_to_positions(rows)
    priced_values = [pos.valuation for pos in positions if pos.valuation is not None]
    total_value = sum(priced_values)
    priced_count = len(priced_values)
    return OpenPositionSummary(
        total_value=total_value,
        position_count=len(positions),
        priced_position_count=priced_count,
        positions=positions,
    )


def get_transaction_date_range(
    *,
    db_path: str | Path | None = None,
    asset_type_filter: Optional[str] = None,
) -> Tuple[Optional[date], Optional[date]]:
    path = _ensure_existing_db(db_path)
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT MIN(t.transaction_date) AS start_date,
                   MAX(t.transaction_date) AS end_date
            FROM transaction_t t
            JOIN security_t s ON s.id = t.security_id
            WHERE t.transaction_date IS NOT NULL
              AND (
                  :asset_type IS NULL
                  OR (
                      s.asset_type IS NOT NULL
                      AND LOWER(s.asset_type) = LOWER(:asset_type)
                  )
              )
            """,
            {"asset_type": asset_type_filter},
        ).fetchone()

    if row is None:
        return None, None

    return _to_date(row["start_date"]), _to_date(row["end_date"])


def get_portfolio_xirr(
    *,
    db_path: str | Path | None = None,
    asset_type_filter: Optional[str] = None,
) -> Optional[float]:
    path = _ensure_existing_db(db_path)
    try:
        return calculate_portfolio_xirr(
            db_path=str(path), asset_type_filter=asset_type_filter
        )
    except Exception as exc:  # pragma: no cover - defensive guard for UI
        logger.error("Failed to calculate XIRR: %s", exc)
        return None


def _rows_to_positions(rows: Iterable[sqlite3.Row]) -> List[PositionSnapshot]:
    positions: List[PositionSnapshot] = []
    for row in rows:
        last_price = float(row["unit_price"]) if row["unit_price"] is not None else None
        positions.append(
            PositionSnapshot(
                security_id=int(row["security_id"]),
                security_name=str(row["security_name"]),
                net_shares=float(row["net_shares"] or 0.0),
                last_price=last_price,
                last_price_date=_to_date(row["transaction_date"]),
            )
        )
    return positions


def _resolve_db_path(db_path: str | Path | None) -> Path:
    if db_path is None:
        return Path(DB_PATH)
    return Path(db_path)


def _ensure_existing_db(db_path: str | Path | None) -> Path:
    path = _resolve_db_path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"SQLite database not found at {path}")
    return path
