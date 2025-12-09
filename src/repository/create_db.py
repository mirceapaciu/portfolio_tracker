"""
Database table creation functions.

Each function creates a specific table if it doesn't exist.
Loader scripts call only the functions they need.
"""

import sqlite3


def create_comdirect_tax_detail_staging(cursor: sqlite3.Cursor):
    """Create comdirect_tax_detail_staging table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comdirect_tax_detail_staging (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            steuerliches_datum DATE,
            vorgang TEXT,
            bezeichnung TEXT,
            gewinn_verlust DECIMAL,
            import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_file TEXT,
            processed BOOLEAN DEFAULT 0
        )
    """)


def create_comdirect_transactions_staging(cursor: sqlite3.Cursor):
    """Create comdirect_transactions_staging table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comdirect_transactions_staging (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datum_ausfuehrung DATE,
            bezeichnung TEXT,
            geschaeftsart TEXT,
            stuecke_nominal DECIMAL,
            kurs DECIMAL,
            kurswert_eur DECIMAL,
            kundenendbetrag_eur DECIMAL,
            entgelt_eur DECIMAL,
            import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_file TEXT,
            processed BOOLEAN DEFAULT 0
        )
    """)


def create_traderepublic_transactions_staging(cursor: sqlite3.Cursor):
    """Create traderepublic_transactions_staging table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS traderepublic_transactions_staging (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE,
            transaction_type TEXT,
            security_name TEXT,
            shares DECIMAL,
            price DECIMAL,
            amount DECIMAL,
            financial_transaction_tax DECIMAL,
            import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_file TEXT,
            processed BOOLEAN DEFAULT 0
        )
    """)


def create_broker_t(cursor: sqlite3.Cursor):
    """Create broker_t master table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS broker_t (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            broker_name TEXT UNIQUE NOT NULL
        )
    """)


def create_security_t(cursor: sqlite3.Cursor):
    """Create security_t master table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS security_t (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            security_name TEXT UNIQUE NOT NULL,
            isin TEXT UNIQUE,
            symbol TEXT,
            asset_type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def create_table_t(cursor: sqlite3.Cursor):
    """Create table_t metadata table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS table_t (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT UNIQUE NOT NULL
        )
    """)


def create_transaction_t(cursor: sqlite3.Cursor):
    """Create transaction_t normalized table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transaction_t (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            security_id INTEGER,
            broker_id INTEGER,
            transaction_date DATE,
            transaction_type TEXT,
            shares DECIMAL,
            price_per_share DECIMAL,
            total_value DECIMAL,
            fees DECIMAL,
            net_amount DECIMAL,
            currency TEXT DEFAULT 'EUR',
            staging_table_id INTEGER,
            staging_row_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (security_id) REFERENCES security_t(id),
            FOREIGN KEY (broker_id) REFERENCES broker_t(id),
            FOREIGN KEY (staging_table_id) REFERENCES table_t(id)
        )
    """)


def create_dividend_t(cursor: sqlite3.Cursor):
    """Create dividend_t normalized table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dividend_t (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            security_id INTEGER,
            broker_id INTEGER,
            payment_date DATE,
            tax_date DATE,
            dividend_amount DECIMAL,
            currency TEXT DEFAULT 'EUR',
            staging_table_id INTEGER,
            staging_row_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (security_id) REFERENCES security_t(id),
            FOREIGN KEY (broker_id) REFERENCES broker_t(id),
            FOREIGN KEY (staging_table_id) REFERENCES table_t(id)
        )
    """)
