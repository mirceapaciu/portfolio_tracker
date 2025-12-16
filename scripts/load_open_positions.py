"""Load normalized open positions CSV into open_position_staging_t."""

from __future__ import annotations

import argparse
import csv
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Iterable

sys.path.insert(0, str(Path(__file__).parent.parent))

from configuration import DB_PATH, LOADER_LOG_PATH
from src.repository.create_db import create_open_position_staging_t
from src.utils.parse import parse_german_date, parse_german_decimal

REQUIRED_COLUMNS = {"broker", "security_name", "shares", "share_price", "amount", "date"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOADER_LOG_PATH),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def read_csv_rows(filepath: Path) -> Iterable[dict[str, str]]:
    with filepath.open("r", encoding="utf-8", newline="") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";")
        missing = REQUIRED_COLUMNS - set(h.strip() for h in reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")
        for row in reader:
            yield row


def load_open_positions(filepath: str, db_path: str | None = None) -> None:
    if db_path is None:
        db_path = str(DB_PATH)

    source_path = Path(filepath)
    if not source_path.exists():
        logger.error("File not found: %s", filepath)
        raise FileNotFoundError(filepath)

    logger.info("Starting import of open positions file: %s", source_path.name)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    create_open_position_staging_t(cursor)

    records_imported = 0
    for row in read_csv_rows(source_path):
        broker = (row.get("broker") or "").strip()
        security_name = (row.get("security_name") or "").strip()
        if not broker or not security_name:
            logger.warning("Skipping row with missing broker or security name: %s", row)
            continue

        shares = parse_german_decimal(row.get("shares") or "")
        share_price = parse_german_decimal(row.get("share_price") or "")
        amount = parse_german_decimal(row.get("amount") or "")
        position_date = parse_german_date(row.get("date") or "")

        shares_float = float(shares) if shares is not None else None
        share_price_float = float(share_price) if share_price is not None else None
        amount_float = float(amount) if amount is not None else None

        cursor.execute(
            """
            INSERT INTO open_position_staging_t
                (broker, security_name, shares, share_price, amount, position_date, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                broker,
                security_name,
                shares_float,
                share_price_float,
                amount_float,
                position_date,
                source_path.name,
            ),
        )

        records_imported += 1

    conn.commit()
    conn.close()

    logger.info("Successfully imported %s records from %s", records_imported, source_path.name)
    print(f"Successfully imported {records_imported} records from {source_path.name}")
    print("Data loaded into: open_position_staging_t")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("filepath", help="Path to the normalized open positions CSV file")
    parser.add_argument("--db", default=None, help=f"Path to SQLite database (default: {DB_PATH})")
    args = parser.parse_args()

    try:
        load_open_positions(args.filepath, args.db)
    except Exception as exc:  # pragma: no cover
        logger.exception("Failed to load open positions")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
