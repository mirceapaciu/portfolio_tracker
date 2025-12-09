"""
Transform TradeRepublic staging data to normalized tables.

This module handles the Transform and Load phases of the ETL pipeline,
converting raw staging data into the normalized schema.
"""

import logging
import sqlite3
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from configuration import DB_PATH
from src.repository.create_db import (
    create_broker_t,
    create_security_t,
    create_table_t,
    create_transaction_t
)
from src.repository.broker_repository import get_or_create_broker
from src.repository.security_repository import get_or_create_security
from src.repository.table_repository import get_or_create_table_id
from src.etl.transform_utils import transform_transaction_type

logger = logging.getLogger(__name__)

def transform_traderepublic_transactions(db_path: str = None) -> dict:
    """
    Transform TradeRepublic staging data into normalized transaction_t table.
    
    Processes all unprocessed records from traderepublic_transactions_staging,
    transforms them, and loads them into transaction_t. Marks processed records.
    
    Args:
        db_path: Path to SQLite database (default: from configuration)
        
    Returns:
        Dictionary with statistics: {'processed': int, 'errors': int, 'skipped': int}
    """
    if db_path is None:
        db_path = str(DB_PATH)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Ensure required tables exist
    create_broker_t(cursor)
    create_security_t(cursor)
    create_table_t(cursor)
    create_transaction_t(cursor)
    
    # Get broker and table IDs
    broker_id = get_or_create_broker(cursor, 'traderepublic')
    staging_table_id = get_or_create_table_id(cursor, 'traderepublic_transactions_staging')
    
    # Fetch unprocessed records
    cursor.execute("""
        SELECT id, date, transaction_type, security_name, shares, price, 
               amount, financial_transaction_tax
        FROM traderepublic_transactions_staging
        WHERE processed = 0
        ORDER BY date, id
    """)
    
    records = cursor.fetchall()
    stats = {'processed': 0, 'errors': 0, 'skipped': 0}
    
    logger.info(f"Processing {len(records)} unprocessed TradeRepublic transactions")
    
    for row in records:
        staging_row_id, date, transaction_type, security_name, shares, price, amount, tax = row
        
        try:
            # Skip if missing critical data
            if not security_name or not transaction_type:
                logger.warning(f"Skipping staging row {staging_row_id}: missing security_name or transaction_type")
                stats['skipped'] += 1
                continue
            
            # Get or create security
            security_id = get_or_create_security(cursor, security_name)
            
            # Normalize transaction type
            normalized_type = transform_transaction_type(transaction_type)
            
            # Calculate values
            # For TradeRepublic, amount is always positive for both buys and sells
            # We need to determine the sign based on transaction type
            total_value = float(amount) if amount else 0.0
            if normalized_type == 'buy':
                total_value = -abs(total_value)  # Buys are negative (money out)
            elif normalized_type == 'sell':
                total_value = abs(total_value)  # Sells are positive (money in)
            
            shares_float = float(shares) if shares else 0.0
            price_float = float(price) if price else 0.0
            fees = float(tax) if tax else 0.0
            
            # Net amount is total_value minus fees for sells, or total_value plus fees for buys
            if normalized_type == 'sell':
                net_amount = total_value - fees
            else:
                net_amount = total_value - fees  # Both negative for buys
            
            # Insert into transaction_t
            cursor.execute("""
                INSERT INTO transaction_t 
                (security_id, broker_id, transaction_date, transaction_type,
                 shares, price_per_share, total_value, fees, net_amount,
                 currency, staging_table_id, staging_row_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                security_id, broker_id, date, normalized_type,
                shares_float, price_float, total_value, fees, net_amount,
                'EUR', staging_table_id, staging_row_id
            ))
            
            # Mark as processed
            cursor.execute(
                "UPDATE traderepublic_transactions_staging SET processed = 1 WHERE id = ?",
                (staging_row_id,)
            )
            
            stats['processed'] += 1
            
        except Exception as e:
            logger.error(f"Error processing staging row {staging_row_id}: {e}")
            stats['errors'] += 1
            continue
    
    conn.commit()
    conn.close()
    
    logger.info(f"Transform complete. Processed: {stats['processed']}, Errors: {stats['errors']}, Skipped: {stats['skipped']}")
    
    return stats


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run transformation
    stats = transform_traderepublic_transactions()
    
    print(f"\nTransformation Results:")
    print(f"  Processed: {stats['processed']}")
    print(f"  Errors: {stats['errors']}")
    print(f"  Skipped: {stats['skipped']}")
