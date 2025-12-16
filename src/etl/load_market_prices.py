"""Transform open_position_staging_t rows into market_price snapshots."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from configuration import DB_PATH, LOADER_LOG_PATH
from src.repository.create_db import create_market_price_t
from src.repository.security_repository import get_or_create_security

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOADER_LOG_PATH),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def fetch_open_positions(cursor: sqlite3.Cursor) -> list[dict]:
    cursor.execute(
        """
        SELECT
            security_name,
            share_price,
            position_date
        FROM open_position_staging_t
        WHERE processed = 0
        """
    )
    rows = cursor.fetchall()
    return [
        {
            "security_name": row[0],
            "share_price": row[1],
            "position_date": row[2],
        }
        for row in rows
    ]


def load_market_prices(db_path: str | None = None) -> None:
    if db_path is None:
        db_path = str(DB_PATH)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    create_market_price_t(cursor)

    positions = fetch_open_positions(cursor)
    if not positions:
        logger.info("No new open positions to process.")
        print("No new open positions to process.")
        return

    inserted = 0
    for position in positions:
        security_id = get_or_create_security(cursor, position["security_name"])
        share_price = position["share_price"]
        price_date = position["position_date"]

        if share_price is None or price_date is None:
            logger.warning("Skipping position with missing price/date: %s", position)
            continue

        cursor.execute(
            """
            INSERT INTO market_price (security_id, share_price, price_date)
            VALUES (?, ?, ?)
            ON CONFLICT(security_id, price_date) DO UPDATE SET
                share_price = excluded.share_price,
                created_at = CURRENT_TIMESTAMP
            """,
            (security_id, share_price, price_date),
        )

        inserted += 1

    cursor.execute("UPDATE open_position_staging_t SET processed = 1 WHERE processed = 0")
    conn.commit()
    conn.close()

    logger.info("Upserted %s market price rows", inserted)
    print(f"Upserted {inserted} market price rows")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=None, help=f"Path to SQLite database (default: {DB_PATH})")
    args = parser.parse_args()

    try:
        load_market_prices(args.db)
    except Exception as exc:  # pragma: no cover
        logger.exception("Failed to load market prices")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
