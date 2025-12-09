"""
Load Comdirect transaction export (abrechnungsdaten) into staging table.

Usage:
    python scripts/load_comdirect_transactions.py <filepath>

Example:
    python scripts/load_comdirect_transactions.py data/input/abrechnungsdaten_comdirect_20251205.csv
"""

import argparse
import csv
import logging
import re
import sqlite3
import sys
from pathlib import Path

# Add parent directory to path to import configuration
sys.path.insert(0, str(Path(__file__).parent.parent))
from configuration import DB_PATH, LOADER_LOG_PATH
from src.utils.parse import parse_german_decimal, parse_german_date
from src.repository.create_db import create_comdirect_transactions_staging

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


def load_comdirect_transactions(filepath: str, db_path: str = None):
    """Load Comdirect transaction CSV into staging table."""
    if db_path is None:
        db_path = str(DB_PATH)
    
    filepath_obj = Path(filepath)
    if not filepath_obj.exists():
        logger.error(f"File not found: {filepath}")
        print(f"Error: File not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    logger.info(f"Starting import of Comdirect transactions file: {filepath_obj.name}")

    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create staging table if it doesn't exist
    create_comdirect_transactions_staging(cursor)

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
        datum_ausfuehrung = None
        bezeichnung = None
        geschaeftsart = None
        stuecke_nominal = None
        kurs = None
        kurswert_eur = None
        kundenendbetrag_eur = None
        entgelt_eur = None
        
        for key, value in row.items():
            key_lower = key.lower().strip()
            
            if 'datum' in key_lower and 'ausf' in key_lower:
                datum_ausfuehrung = parse_german_date(value)
            elif 'bezeichnung' in key_lower:
                bezeichnung = value.strip() if value else None
            elif re.search(r'gesch.*ftsart', key_lower):
                geschaeftsart = value.strip() if value else None
            elif re.search(r'st.*cke/nom.', key_lower):
                stuecke_nominal = parse_german_decimal(value)
            elif 'kurs' == key_lower:
                kurs = parse_german_decimal(value)
            elif 'kurswert eur' in key_lower:
                kurswert_eur = parse_german_decimal(value)
            elif 'kundenendbetrag eur' in key_lower:
                kundenendbetrag_eur = parse_german_decimal(value)
            elif 'entgelt (summe eigen und fremd) eur' in key_lower:
                entgelt_eur = parse_german_decimal(value)
        
        # Convert Decimal to float for SQLite compatibility
        stuecke_nominal_float = float(stuecke_nominal) if stuecke_nominal is not None else None
        kurs_float = float(kurs) if kurs is not None else None
        kurswert_eur_float = float(kurswert_eur) if kurswert_eur is not None else None
        kundenendbetrag_eur_float = float(kundenendbetrag_eur) if kundenendbetrag_eur is not None else None
        entgelt_eur_float = float(entgelt_eur) if entgelt_eur is not None else None
        
        cursor.execute("""
            INSERT INTO comdirect_transactions_staging 
            (datum_ausfuehrung, bezeichnung, geschaeftsart, stuecke_nominal, 
             kurs, kurswert_eur, kundenendbetrag_eur, entgelt_eur, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (datum_ausfuehrung, bezeichnung, geschaeftsart, stuecke_nominal_float,
              kurs_float, kurswert_eur_float, kundenendbetrag_eur_float, entgelt_eur_float, filepath_obj.name))
        
        records_imported += 1

    conn.commit()
    conn.close()

    success_msg = f"Successfully imported {records_imported} records from {filepath_obj.name}"
    table_msg = f"Data loaded into: comdirect_transactions_staging"
    logger.info(success_msg)
    logger.info(table_msg)
    print(success_msg)
    print(table_msg)


def main():
    parser = argparse.ArgumentParser(
        description="Load Comdirect transaction export into staging table"
    )
    parser.add_argument(
        "filepath",
        help="Path to the Comdirect transaction CSV file"
    )
    parser.add_argument(
        "--db",
        default=None,
        help=f"Path to SQLite database (default: {DB_PATH})"
    )
    
    args = parser.parse_args()
    load_comdirect_transactions(args.filepath, args.db)


if __name__ == "__main__":
    main()
