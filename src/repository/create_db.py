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
            steuerjahr INTEGER,
            buchungstag DATE,
            steuerliches_datum DATE,
            referenznummer TEXT,
            vorgang TEXT,
            stueck_nominale DECIMAL,
            bezeichnung TEXT,
            wkn TEXT,
            betrag_brutto DECIMAL,
            gewinn_verlust DECIMAL,
            gewinn_aktien DECIMAL,
            verlust_aktien DECIMAL,
            gewinn_sonstige DECIMAL,
            verlust_sonstige DECIMAL,
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
            wkn TEXT,
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


def create_open_position_staging_t(cursor: sqlite3.Cursor):
    """Create open_position_staging_t table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS open_position_staging_t (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            broker TEXT NOT NULL,
            security_name TEXT NOT NULL,
            shares DECIMAL,
            share_price DECIMAL,
            amount DECIMAL,
            position_date DATE,
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
            wkn TEXT UNIQUE,
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
    cursor.execute(
        """
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
            used_in_realized_gain BOOLEAN DEFAULT 0,
            allocated BOOLEAN DEFAULT 0,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (security_id) REFERENCES security_t(id),
            FOREIGN KEY (broker_id) REFERENCES broker_t(id),
            FOREIGN KEY (staging_table_id) REFERENCES table_t(id),
            UNIQUE (staging_table_id, staging_row_id)
        )
        """
    )


def create_market_price_t(cursor: sqlite3.Cursor):
    """Create market_price table if it doesn't exist."""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS market_price (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            security_id INTEGER NOT NULL,
            share_price DECIMAL NOT NULL,
            price_date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (security_id) REFERENCES security_t(id),
            UNIQUE (security_id, price_date)
        )
        """
    )


def create_realized_gain_t(cursor: sqlite3.Cursor):
    """Create realized_gain_t summary table if it doesn't exist."""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS realized_gain_t (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            broker_id INTEGER NOT NULL,
            security_id INTEGER NOT NULL,
            shares DECIMAL NOT NULL,
            invested_value DECIMAL NOT NULL,
            buy_date DATE NOT NULL,
            sell_date DATE NOT NULL,
            p_l DECIMAL NOT NULL,
            total_dividend DECIMAL DEFAULT 0,
            dividend_count INTEGER DEFAULT 0,
            cagr_percentage DECIMAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (broker_id) REFERENCES broker_t(id),
            FOREIGN KEY (security_id) REFERENCES security_t(id)
        )
        """
    )


def create_transaction_match_t(cursor: sqlite3.Cursor):
    """Create transaction_match_t allocation table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transaction_match_t (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            broker_id INTEGER NOT NULL,
            security_id INTEGER NOT NULL,
            buy_transaction_id INTEGER NOT NULL,
            sell_transaction_id INTEGER NOT NULL,
            shares DECIMAL NOT NULL,
            allocated_cost DECIMAL NOT NULL,
            allocated_proceeds DECIMAL NOT NULL,
            allocated_fees DECIMAL DEFAULT 0,
            cost_basis_method TEXT DEFAULT 'FIFO',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (broker_id) REFERENCES broker_t(id),
            FOREIGN KEY (security_id) REFERENCES security_t(id),
            FOREIGN KEY (buy_transaction_id) REFERENCES transaction_t(id),
            FOREIGN KEY (sell_transaction_id) REFERENCES transaction_t(id)
        )
    """)

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_transaction_match_broker_security ON transaction_match_t (broker_id, security_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_transaction_match_buy ON transaction_match_t (buy_transaction_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_transaction_match_sell ON transaction_match_t (sell_transaction_id)"
    )


def create_dividend_allocation_t(cursor: sqlite3.Cursor):
    """Create dividend_allocation_t table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dividend_allocation_t (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            broker_id INTEGER NOT NULL,
            security_id INTEGER NOT NULL,
            dividend_transaction_id INTEGER NOT NULL,
            buy_transaction_id INTEGER NOT NULL,
            shares DECIMAL NOT NULL,
            allocated_amount DECIMAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (broker_id) REFERENCES broker_t(id),
            FOREIGN KEY (security_id) REFERENCES security_t(id),
            FOREIGN KEY (dividend_transaction_id) REFERENCES transaction_t(id),
            FOREIGN KEY (buy_transaction_id) REFERENCES transaction_t(id),
            UNIQUE (dividend_transaction_id, buy_transaction_id)
        )
    """)

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_dividend_allocation_dividend"
        " ON dividend_allocation_t (dividend_transaction_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_dividend_allocation_buy"
        " ON dividend_allocation_t (buy_transaction_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_dividend_allocation_security"
        " ON dividend_allocation_t (broker_id, security_id)"
    )
