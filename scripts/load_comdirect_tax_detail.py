"""
Load Comdirect tax detail export (steuerlichedetailansichtexport) into staging table.

Usage:
    python scripts/load_comdirect_tax_detail.py <filepath>

Example:
    python scripts/load_comdirect_tax_detail.py data/input/steuerlichedetailansichtexport_9772900462_20251205-1606.csv
"""

import argparse
import csv
import logging
import re
import sqlite3
import sys
import unicodedata
from decimal import Decimal
from pathlib import Path

# Add parent directory to path to import configuration
sys.path.insert(0, str(Path(__file__).parent.parent))
from configuration import DB_PATH, LOADER_LOG_PATH
from src.utils.parse import parse_german_decimal, parse_german_date
from src.repository.create_db import create_comdirect_tax_detail_staging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOADER_LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def _normalize_header(header: str) -> str:
    """Convert CSV header to a compact ASCII key for mapping."""
    if header is None:
        return ""
    normalized = unicodedata.normalize('NFKD', header)
    normalized = normalized.encode('ascii', 'ignore').decode('ascii')
    normalized = normalized.lower()
    normalized = re.sub(r'[^a-z0-9]+', '', normalized)
    return normalized


def _parse_int(value: str) -> int | None:
    """Parse integer values, ignoring thousands separators."""
    if not value or value.strip() == "":
        return None
    cleaned = value.replace('.', '').replace(',', '').strip()
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_text(value: str) -> str | None:
    return value.strip() if value and value.strip() != "" else None


FIELD_DEFINITIONS = {
    'steuerjahr': ('steuerjahr', _parse_int),
    'buchungstag': ('buchungstag', parse_german_date),
    'steuerlichesdatum': ('steuerliches_datum', parse_german_date),
    'referenznummer': ('referenznummer', _parse_text),
    'vorgang': ('vorgang', _parse_text),
    'stucknominale': ('stueck_nominale', parse_german_decimal),
    'bezeichnung': ('bezeichnung', _parse_text),
    'wkn': ('wkn', _parse_text),
    'betragbrutto': ('betrag_brutto', parse_german_decimal),
    'gewinnverlust': ('gewinn_verlust', parse_german_decimal),
    'gewinnaktien': ('gewinn_aktien', parse_german_decimal),
    'verlustaktien': ('verlust_aktien', parse_german_decimal),
    'gewinnsonstige': ('gewinn_sonstige', parse_german_decimal),
    'verlustsonstige': ('verlust_sonstige', parse_german_decimal),
}


def load_comdirect_tax_detail(filepath: str, db_path: str = None):
    """Load Comdirect tax detail CSV into staging table."""
    if db_path is None:
        db_path = str(DB_PATH)
    
    filepath_obj = Path(filepath)
    if not filepath_obj.exists():
        logger.error(f"File not found: {filepath}")
        print(f"Error: File not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    logger.info(f"Starting import of Comdirect tax detail file: {filepath_obj.name}")

    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create staging table if it doesn't exist
    create_comdirect_tax_detail_staging(cursor)

    # Read and import CSV
    records_imported = 0

    # Try different encodings
    encodings = ['windows-1252', 'utf-8', 'latin-1']
    csv_data = None
    
    for encoding in encodings:
        try:
            with open(filepath, 'r', encoding=encoding) as f:
                csv_data = f.read()
            break
        except UnicodeDecodeError:
            continue
    
    if csv_data is None:
        error_msg = "Could not read file with any supported encoding"
        logger.error(f"{error_msg}: {filepath}")
        print(f"Error: {error_msg}", file=sys.stderr)
        sys.exit(1)

    # Parse CSV
    reader = csv.DictReader(csv_data.splitlines(), delimiter=';')

    cursor.execute("PRAGMA table_info(comdirect_tax_detail_staging)")
    table_columns = {row[1] for row in cursor.fetchall()}

    for row in reader:
        record = {}

        for header, value in row.items():
            normalized = _normalize_header(header)
            field_info = FIELD_DEFINITIONS.get(normalized)
            if not field_info:
                continue
            column_name, parser = field_info
            parsed_value = parser(value)
            record[column_name] = parsed_value

        record['source_file'] = filepath_obj.name

        for column_name, column_value in list(record.items()):
            if isinstance(column_value, Decimal):
                record[column_name] = float(column_value)

        insert_columns = [col for col in record.keys() if col in table_columns]
        if not insert_columns:
            logger.debug("Skipping row because no mapped columns were found in staging table")
            continue

        placeholders = ','.join(['?'] * len(insert_columns))
        column_sql = ','.join(insert_columns)
        cursor.execute(
            f"INSERT INTO comdirect_tax_detail_staging ({column_sql}) VALUES ({placeholders})",
            tuple(record[col] for col in insert_columns)
        )

        records_imported += 1

    conn.commit()
    conn.close()

    success_msg = f"Successfully imported {records_imported} records from {filepath_obj.name}"
    table_msg = f"Data loaded into: comdirect_tax_detail_staging"
    logger.info(success_msg)
    logger.info(table_msg)
    print(success_msg)
    print(table_msg)


def main():
    parser = argparse.ArgumentParser(
        description="Load Comdirect tax detail export into staging table"
    )
    parser.add_argument(
        "filepath",
        help="Path to the Comdirect tax detail CSV file"
    )
    parser.add_argument(
        "--db",
        default=None,
        help=f"Path to SQLite database (default: {DB_PATH})"
    )
    
    args = parser.parse_args()
    load_comdirect_tax_detail(args.filepath, args.db)


if __name__ == "__main__":
    main()
