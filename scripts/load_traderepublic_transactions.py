"""
Load TradeRepublic transaction export into staging table.

Usage:
    python scripts/load_traderepublic_transactions.py <filepath>

Example:
    python scripts/load_traderepublic_transactions.py data/input/traderepublic_transactions.csv
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
from src.utils.parse import parse_german_decimal as parse_decimal, parse_date
from src.repository.create_db import create_traderepublic_transactions_staging

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


def load_traderepublic_transactions(filepath: str, db_path: str = None):
    """Load TradeRepublic transaction CSV into staging table."""
    if db_path is None:
        db_path = str(DB_PATH)
    
    filepath_obj = Path(filepath)
    if not filepath_obj.exists():
        logger.error(f"File not found: {filepath}")
        print(f"Error: File not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    logger.info(f"Starting import of TradeRepublic transactions file: {filepath_obj.name}")

    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create staging table if it doesn't exist
    create_traderepublic_transactions_staging(cursor)

    # Read and import CSV
    records_imported = 0
    
    # Try different encodings
    encodings = ['utf-8', 'windows-1252', 'latin-1']
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

    # Parse CSV (semicolon-delimited)
    reader = csv.DictReader(csv_data.splitlines(), delimiter=';')
    
    for row in reader:
        # Map column names (handle various possible column names)
        date = None
        transaction_type = None
        security_name = None
        shares = None
        price = None
        amount = None
        financial_transaction_tax = None
        
        for key, value in row.items():
            key_lower = key.lower().strip()
            
            if key_lower == 'date':
                date = parse_date(value)
            elif 'transaction_type' == key_lower:
                transaction_type = value.strip().lower() if value else None
            elif 'security_name' == key_lower:
                security_name = value.strip() if value else None
            elif key_lower == 'shares':
                shares = parse_decimal(value)
            elif key_lower == 'price':
                price = parse_decimal(value)
            elif key_lower == 'amount':
                amount = parse_decimal(value)
            elif key_lower == 'financial_transaction_tax':
                financial_transaction_tax = parse_decimal(value)
        
        # Convert Decimal to float for SQLite compatibility
        shares_float = float(shares) if shares is not None else None
        price_float = float(price) if price is not None else None
        amount_float = float(amount) if amount is not None else None
        financial_transaction_tax_float = float(financial_transaction_tax) if financial_transaction_tax is not None else None
        
        cursor.execute("""
            INSERT INTO traderepublic_transactions_staging 
            (date, transaction_type, security_name, shares, price, amount, 
             financial_transaction_tax, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (date, transaction_type, security_name, shares_float, price_float, amount_float,
              financial_transaction_tax_float, filepath_obj.name))
        
        records_imported += 1

    conn.commit()
    conn.close()

    success_msg = f"Successfully imported {records_imported} records from {filepath_obj.name}"
    table_msg = f"Data loaded into: traderepublic_transactions_staging"
    logger.info(success_msg)
    logger.info(table_msg)
    print(success_msg)
    print(table_msg)


def main():
    parser = argparse.ArgumentParser(
        description="Load TradeRepublic transaction export into staging table"
    )
    parser.add_argument(
        "filepath",
        help="Path to the TradeRepublic transaction CSV file"
    )
    parser.add_argument(
        "--db",
        default=None,
        help=f"Path to SQLite database (default: {DB_PATH})"
    )
    
    args = parser.parse_args()
    load_traderepublic_transactions(args.filepath, args.db)


if __name__ == "__main__":
    main()
