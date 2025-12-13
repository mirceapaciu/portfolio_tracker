# Portfolio Tracker Project - AI Agent Instructions

## Project Overview
Investment portfolio analysis tool for processing German tax/broker export data. Primary data source is CSV exports with German column headers containing transaction history, gains/losses, and tax calculations.

## Input Data
- CSV files located in `data/input` directory

The files are received from the following brokers:
1. Comdirect
2. TradeRepublic

There are the following types of CSV files (semicolon-delimited):

### Comdirect Tax Detail File
e.g. steuerlichedetailansichtexport_9772900462_20251205-1606.csv
This file is used for extracting dividend data

Key columns:
- **Steuerliches Datum**: Tax date
- **Vorgang**: Transaction type (Verkauf, Kauf, Dividenden, Zinsen, Ausschüttung, Abgang)
- **Bezeichnung**: Security name
- **Gewinn/Verlust**: Profit/loss

Data Processing:

- Parse semicolon-delimited CSVs with German decimal format (comma separator: `47,53` not `47.53`)
- Handle encoding issues (file contains `St�ck` instead of `Stück` - likely Windows-1252 encoding)
- Date format: `DD.MM.YYYY` with strings like `"02.12.2025"`


### Comdirect transaction settlement file
e.g. abrechnungsdaten_comdirect_20251205.csv

Purpose: Complete transaction history with buy and sell details, including fees.
Note: This file does not contain bond liquidations, but it contain buy/sell transactions for bonds. The bond liquidations are recorded in umsaetze_*.csv
This file contains a long history of transactions (multiple years).

Key columns:
- "Datum Ausführung": Execution date (Note that the column name in the header may have encoding issues, e.g. "Datum Ausf�hrung")
- "Bezeichnung": Security name
- "Geschäftsart": Transaction type (Kauf, Verkauf). Note thatthe column name in the header may have encoding issues, e.g. "Gesch�ftsart"
- "St�cke/Nom.": Share count (note encoding issues, e.g. "St�cke/Nom.")
- "Kurs": Price per share
- "Kurswert EUR": Total value. For buys, this is negative (money spent); for sells, positive (money received)
- "Kundenendbetrag EUR": Final amount after fees. For buys, negative; for sells, positive
- "Entgelt (Summe eigen und fremd) EUR": Fees associated with the transaction
 
This file is used for:

- Understanding the complete investment flow
- Calculating cost basis and capital gains
- Tracking transaction fees and costs

Data Processing:

- Parse semicolon-delimited CSVs with German decimal format (comma separator: `47,53` not `47.53`)
- Handle encoding issues (file contains `St�ck` instead of `Stück` - likely Windows-1252 encoding)
- Date format: `DD.MM.YYYY` with strings like `"02.12.2025"`

### Comdirect transaction file
e.g. umsaetze_XXXXXXXX_20251209-1435.csv
Contains all transactions on the portfolio, including bond liquidations.
This file can contain maximum 2 years of history. It is recommended to regularly download and archive these files for a complete history.

Key columns:
- "Buchungstag": Booking date
- "Gesch�ftstag": Transaction date
- "St�ck / Nom.": Share count (Negative for sells, positive for buys)
- "Bezeichnung": Security name
- "WKN": Security identifier
- "W�hrung": Currency
- "Ausf�hrungskurs": Execution price
- "Umsatz in EUR": Amount in EUR (Negative for sells, positive for buys)

Data Processing:

- Parse semicolon-delimited CSVs with German decimal format (comma separator: `47,53` not `47.53`)
- Handle encoding issues (file contains `St�ck` instead of `Stück` - likely Windows-1252 encoding)
- Date format: `DD.MM.YYYY` with strings like `"02.12.2025"`

### TradeRepublic transactions file

Key columns:
- date
- transaction_type" (buy/sell/dividend)
- security_name"
- "shares": share count
- price: price per share
- amount: total value (positive both for buys and for sells)
- financial_transaction_tax

## Database Schema

### Staging Tables (Broker-Specific)

#### `comdirect_tax_detail_staging`
Raw import of Comdirect tax detail exports (steuerlichedetailansichtexport files).

Columns:
- `id`: Primary key (auto-increment)
- `steuerliches_datum`: Tax date (DATE)
- `vorgang`: Transaction type (TEXT) - e.g., Verkauf, Kauf, Dividenden, Zinsen, Ausschüttung, Abgang
- `bezeichnung`: Security name (TEXT)
- `gewinn_verlust`: Profit/loss (DECIMAL)
- `import_timestamp`: When this record was imported (TIMESTAMP)
- `source_file`: Filename of the CSV source (TEXT)
- `processed`: Whether this record has been processed (BOOLEAN)

#### `comdirect_transactions_staging`
Raw import of Comdirect transaction exports (abrechnungsdaten files).

Columns:
- `id`: Primary key (auto-increment)
- `datum_ausfuehrung`: Execution date (DATE)
- `bezeichnung`: Security name (TEXT)
- `geschaeftsart`: Transaction type (TEXT) - Kauf, Verkauf
- `stuecke_nominal`: Share count (DECIMAL)
- `kurs`: Price per share (DECIMAL)
- `kurswert_eur`: Total value in EUR (DECIMAL) - negative for buys, positive for sells
- `kundenendbetrag_eur`: Final amount after fees in EUR (DECIMAL)
- `entgelt_eur`: Transaction fees in EUR (DECIMAL)
- `import_timestamp`: When this record was imported (TIMESTAMP)
- `source_file`: Filename of the CSV source (TEXT)
- `processed`: Whether this record has been processed (BOOLEAN)

#### `traderepublic_transactions_staging`
Raw import of TradeRepublic transaction exports.

Columns:
- `id`: Primary key (auto-increment)
- `date`: Transaction date (DATE)
- `transaction_type`: Transaction type (TEXT) - buy, sell, dividend
- `security_name`: Security name (TEXT)
- `shares`: Share count (DECIMAL)
- `price`: Price per share (DECIMAL)
- `amount`: Total value (DECIMAL) - positive for both buys and sells
- `financial_transaction_tax`: Tax amount (DECIMAL)
- `import_timestamp`: When this record was imported (TIMESTAMP)
- `source_file`: Filename of the CSV source (TEXT)
- `processed`: Whether this record has been processed (BOOLEAN)

### Common Schema Tables (Normalized)

#### `broker_t`
Master data for all brokers.

Columns:
- `id`: Primary key (auto-increment)
- `broker_name`: Broker name (TEXT, UNIQUE) - e.g., comdirect, traderepublic

#### `security_t`
Master data for all securities across brokers.

Columns:
- `id`: Primary key (auto-increment)
- `security_name`: Normalized security name (TEXT, UNIQUE)
- `isin`: International Securities Identification Number (TEXT, UNIQUE)
- `symbol`: Trading symbol (TEXT)
- `asset_type`: Type of asset (TEXT) - e.g., stock, ETF, fund, bond
- `created_at`: Record creation timestamp (TIMESTAMP)
- `updated_at`: Last update timestamp (TIMESTAMP)

#### `table_t`
Metadata that lists the tables.

Columns:
- `id`: Primary key (auto-increment)
- `table_name`: Table name (TEXT, UNIQUE) - e.g., comdirect_transactions_staging, traderepublic_transactions_staging

#### `transaction_t`
Normalized transaction history from all brokers.

Columns:
- `id`: Primary key (auto-increment)
- `security_id`: Foreign key to securities table (INTEGER)
- `broker_id`: Foreign key to brokers table (INTEGER)
- `transaction_date`: Date of transaction (DATE)
- `transaction_type`: Normalized type (TEXT) - buy, sell, dividend, interest, distribution
- `shares`: Number of shares (DECIMAL)
- `price_per_share`: Price per share (DECIMAL)
- `total_value`: Total transaction value (DECIMAL)
- `fees`: Transaction fees (DECIMAL)
- `net_amount`: Final amount after fees (DECIMAL)
- `currency`: Currency code (TEXT) - default EUR
- `staging_table_id`: Source staging table (INTEGER). References `table_t.id`
- `staging_row_id`: ID in staging table (INTEGER)
- `used_in_realized_gain`: Whether this transaction has been used in realized gain calculations (BOOLEAN)
- `created_at`: Record creation timestamp (TIMESTAMP)

#### `dividend_t`
Normalized dividend/income data from all brokers.

Columns:
- `id`: Primary key (auto-increment)
- `security_id`: Foreign key to securities table (INTEGER)
- `broker_id`: Foreign key to brokers table (INTEGER)
- `payment_date`: Date dividend was paid (DATE)
- `tax_date`: Tax-relevant date (DATE)
- `dividend_amount`: Gross dividend amount (DECIMAL)
- `currency`: Currency code (TEXT) - default EUR
- `staging_table_id`: Source staging table (INTEGER). References `table_t.id`
- `staging_row_id`: ID in staging table (INTEGER)
- `created_at`: Record creation timestamp (TIMESTAMP)

#### `transaction_match_t`
Intermediate table that stores the BUY ↔ SELL allocation used for realized gain calculations (e.g., FIFO). This avoids mutating/splitting rows in `transaction_t` while keeping the matching auditable.

Columns:
- `id`: Primary key (auto-increment)
- `broker_id`: Foreign key to broker table (INTEGER)
- `security_id`: Foreign key to security table (INTEGER)
- `buy_transaction_id`: FK to `transaction_t.id` for the BUY leg (INTEGER)
- `sell_transaction_id`: FK to `transaction_t.id` for the SELL leg (INTEGER)
- `shares`: Number of shares allocated from BUY to SELL (DECIMAL)
- `allocated_cost`: Cost basis allocated to this match (DECIMAL)
- `allocated_proceeds`: Proceeds allocated to this match (DECIMAL)
- `allocated_fees`: Fees allocated to this match (DECIMAL)
- `created_at`: Record creation timestamp (TIMESTAMP)

#### `realized_gain_t`
Tracks realized gains/losses from sales.

Columns:
- `id`: Primary key (auto-increment)  
- `broker_id`: Foreign key to broker table (INTEGER)
- `security_id`: Foreign key to security table (INTEGER)
- `shares`: Number of shares sold (DECIMAL)
- `invested_value`: Total value of the BUY transactions (DECIMAL)
- `buy_date`: Date of the BUY transaction (DATE)
- `sell_date`: Date of the SELL transaction (DATE)
- `p_l`: Realized profit/loss (DECIMAL)
- `total_dividend`: Total dividends received during holding period (DECIMAL)
- `dividend_count`: Number of dividend payments received (INTEGER)
- `cagr_percentage`: Compound Annual Growth Rate (DECIMAL)

### Data Flow
1. Import CSV files into broker-specific staging tables (preserves original data)
2. Transform and load from staging tables into normalized common schema
3. Link transactions/dividends to securities master data
4. Run analysis queries against normalized tables

#### 