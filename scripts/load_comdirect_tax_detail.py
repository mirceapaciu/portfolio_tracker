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
import sqlite3
import sys
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
    
    for row in reader:
        # Map column names (handle potential encoding issues)
        steuerliches_datum = None
        vorgang = None
        bezeichnung = None
        gewinn_verlust = None
        
        for key, value in row.items():
            key_lower = key.lower().strip()
            if 'steuerliches datum' in key_lower or 'steuerliches_datum' in key_lower:
                steuerliches_datum = parse_german_date(value)
            elif 'vorgang' in key_lower:
                vorgang = value.strip() if value else None
            elif 'bezeichnung' in key_lower:
                bezeichnung = value.strip() if value else None
            elif 'gewinn/verlust' == key_lower:
                gewinn_verlust = parse_german_decimal(value)
        
        # Convert Decimal to float for SQLite compatibility
        gewinn_verlust_float = float(gewinn_verlust) if gewinn_verlust is not None else None
        
        cursor.execute("""
            INSERT INTO comdirect_tax_detail_staging 
            (steuerliches_datum, vorgang, bezeichnung, gewinn_verlust, source_file)
            VALUES (?, ?, ?, ?, ?)
        """, (steuerliches_datum, vorgang, bezeichnung, gewinn_verlust_float, filepath_obj.name))
        
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
