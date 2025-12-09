"""
Transform Comdirect staging data to normalized tables.

This module handles the Transform and Load phases for Comdirect transaction data,
copying staged rows into the normalized transaction_t table.
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


def transform_comdirect_transactions(db_path: str = None) -> dict:
    """Transform Comdirect staging data into transaction_t."""
    if db_path is None:
        db_path = str(DB_PATH)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Ensure tables exist
    create_broker_t(cursor)
    create_security_t(cursor)
    create_table_t(cursor)
    create_transaction_t(cursor)

    broker_id = get_or_create_broker(cursor, 'comdirect')
    staging_table_id = get_or_create_table_id(cursor, 'comdirect_transactions_staging')

    cursor.execute(
        """
         SELECT id, datum_ausfuehrung, bezeichnung, wkn, geschaeftsart,
               stuecke_nominal, kurs, kurswert_eur, kundenendbetrag_eur, entgelt_eur
        FROM comdirect_transactions_staging
        WHERE processed = 0
        ORDER BY datum_ausfuehrung, id
        """
    )

    records = cursor.fetchall()
    stats = {'processed': 0, 'errors': 0, 'skipped': 0}

    logger.info(f"Processing {len(records)} unprocessed Comdirect transactions")

    for row in records:
        (
            staging_row_id,
            datum_ausfuehrung,
            bezeichnung,
            wkn,
            geschaeftsart,
            stuecke_nominal,
            kurs,
            kurswert_eur,
            kundenendbetrag_eur,
            entgelt_eur,
        ) = row

        try:
            if not bezeichnung or not geschaeftsart:
                logger.warning(
                    f"Skipping staging row {staging_row_id}: missing bezeichnung or geschaeftsart"
                )
                stats['skipped'] += 1
                continue

            security_id = get_or_create_security(cursor, bezeichnung, wkn=wkn)
            normalized_type = transform_transaction_type(geschaeftsart)
            if not normalized_type and geschaeftsart:
                normalized_type = geschaeftsart.lower().strip()

            shares = float(stuecke_nominal) if stuecke_nominal is not None else 0.0
            price = float(kurs) if kurs is not None else 0.0
            total_value = float(kurswert_eur) if kurswert_eur is not None else None
            fees = float(entgelt_eur) if entgelt_eur is not None else 0.0
            net_amount = float(kundenendbetrag_eur) if kundenendbetrag_eur is not None else None

            if total_value is None:
                total_value = shares * price

            if normalized_type == 'buy' and total_value > 0:
                total_value = -abs(total_value)
            elif normalized_type == 'sell' and total_value < 0:
                total_value = abs(total_value)

            if net_amount is None:
                net_amount = total_value - fees

            cursor.execute(
                """
                INSERT INTO transaction_t
                (security_id, broker_id, transaction_date, transaction_type,
                 shares, price_per_share, total_value, fees, net_amount,
                 currency, staging_table_id, staging_row_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    security_id,
                    broker_id,
                    datum_ausfuehrung,
                    normalized_type,
                    shares,
                    price,
                    total_value,
                    fees,
                    net_amount,
                    'EUR',
                    staging_table_id,
                    staging_row_id,
                ),
            )

            cursor.execute(
                "UPDATE comdirect_transactions_staging SET processed = 1 WHERE id = ?",
                (staging_row_id,),
            )

            stats['processed'] += 1
        except Exception as exc:
            logger.error(f"Error processing staging row {staging_row_id}: {exc}")
            stats['errors'] += 1
            continue

    conn.commit()
    conn.close()

    logger.info(
        "Transform complete. Processed: %s, Errors: %s, Skipped: %s",
        stats['processed'],
        stats['errors'],
        stats['skipped'],
    )

    return stats


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    stats = transform_comdirect_transactions()

    print("\nTransformation Results:")
    print(f"  Processed: {stats['processed']}")
    print(f"  Errors: {stats['errors']}")
    print(f"  Skipped: {stats['skipped']}")
