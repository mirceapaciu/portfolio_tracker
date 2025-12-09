"""
Transform Comdirect tax detail staging data into the normalized transaction table.

Only dividend-like entries (e.g. 'Ausl. Dividenden', 'Investm. Ausschuettung')
are copied from comdirect_tax_detail_staging to transaction_t, ensuring
staging records are marked as processed once loaded.
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
    create_transaction_t,
)
from src.repository.broker_repository import get_or_create_broker
from src.repository.security_repository import get_or_create_security
from src.repository.table_repository import get_or_create_table_id
from src.etl.transform_utils import transform_transaction_type

logger = logging.getLogger(__name__)

DIVIDEND_TYPES = {'dividend', 'distribution'}


def transform_comdirect_tax_detail(db_path: str = None) -> dict:
    """Transform dividend entries from comdirect tax detail staging."""
    if db_path is None:
        db_path = str(DB_PATH)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    create_broker_t(cursor)
    create_security_t(cursor)
    create_table_t(cursor)
    create_transaction_t(cursor)

    broker_id = get_or_create_broker(cursor, 'comdirect')
    staging_table_id = get_or_create_table_id(cursor, 'comdirect_tax_detail_staging')

    cursor.execute(
        """
         SELECT id, steuerliches_datum, buchungstag, vorgang, bezeichnung,
             wkn, stueck_nominale, betrag_brutto, gewinn_verlust
        FROM comdirect_tax_detail_staging
        WHERE processed = 0
        ORDER BY COALESCE(steuerliches_datum, buchungstag), id
        """
    )

    records = cursor.fetchall()
    stats = {'processed': 0, 'errors': 0, 'skipped': 0}

    logger.info("Processing %s unprocessed Comdirect tax detail rows", len(records))

    for row in records:
        (
            staging_row_id,
            steuerliches_datum,
            buchungstag,
            vorgang,
            bezeichnung,
            wkn,
            stueck_nominale,
            betrag_brutto,
            gewinn_verlust,
        ) = row

        try:
            if not bezeichnung or not vorgang:
                stats['skipped'] += 1
                continue

            normalized_type = transform_transaction_type(vorgang)
            if normalized_type not in DIVIDEND_TYPES:
                continue

            transaction_date = steuerliches_datum or buchungstag
            if not transaction_date:
                logger.warning(
                    "Skipping staging row %s: missing steuerliches_datum and buchungstag",
                    staging_row_id,
                )
                stats['skipped'] += 1
                continue

            security_id = get_or_create_security(cursor, bezeichnung, wkn=wkn)

            total_value = None
            if betrag_brutto is not None:
                total_value = float(betrag_brutto)
            elif gewinn_verlust is not None:
                total_value = float(gewinn_verlust)

            if total_value is None:
                stats['skipped'] += 1
                continue

            shares = float(stueck_nominale) if stueck_nominale is not None else 0.0
            price_per_share = total_value / shares if shares else 0.0
            fees = 0.0
            net_amount = total_value - fees

            transaction_type = 'dividend' if normalized_type in DIVIDEND_TYPES else normalized_type

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
                    transaction_date,
                    transaction_type,
                    shares,
                    price_per_share,
                    total_value,
                    fees,
                    net_amount,
                    'EUR',
                    staging_table_id,
                    staging_row_id,
                ),
            )

            cursor.execute(
                "UPDATE comdirect_tax_detail_staging SET processed = 1 WHERE id = ?",
                (staging_row_id,),
            )

            stats['processed'] += 1
        except Exception as exc:
            logger.error("Error processing staging row %s: %s", staging_row_id, exc)
            stats['errors'] += 1
            continue

    conn.commit()
    conn.close()

    logger.info(
        "Tax detail transform complete. Processed: %s, Errors: %s, Skipped: %s",
        stats['processed'],
        stats['errors'],
        stats['skipped'],
    )

    return stats


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )

    results = transform_comdirect_tax_detail()

    print("\nTransformation Results:")
    print(f"  Processed: {results['processed']}")
    print(f"  Errors: {results['errors']}")
    print(f"  Skipped: {results['skipped']}")
