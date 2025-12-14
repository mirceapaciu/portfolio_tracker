# Portfolio Tracker

## What This Project Does
- Imports semicolon-delimited CSV exports from Comdirect and TradeRepublic, handling German-specific encodings, date formats (`DD.MM.YYYY`), and decimal separators (`,`).
- Loads every raw row into broker-specific staging tables inside the SQLite database at `data/db/portfolio_tracker.db`, preserving provenance via `source_file` and timestamps.
- Normalizes staged data into common tables (`broker_t`, `security_t`, `transaction_t`, `dividend_t`) so that performance analytics can look across brokers.
- Produces CSV analytics such as `data/comdirect_transaction_analysis.csv`, `data/dividends_with_purchases.csv`, and `data/aggregated_investment_report.csv` (which includes dividends matched to positions plus CAGR calculations).

## Data Flow Overview
1. **Input drop** – place broker exports in `data/input/` (see `examples` below for file names).
2. **Staging import** – run the loader scripts to populate `comdirect_*_staging` or `traderepublic_transactions_staging`.
3. **Normalization** – ETL jobs in `src/` link staging rows to master data (`broker_t`, `security_t`) and populate the shared fact tables.
4. **Analytics** – scripts in `scripts/` aggregate realized gains, dividends, unmatched rows, and CAGR metrics for downstream reporting.
5. **Outputs** – curated CSVs land in `data/`, logs in `log/`, and the federated SQLite database remains the system of record.

### Key Staging Tables
| Table | Purpose | Sample Columns |
| --- | --- | --- |
| `comdirect_tax_detail_staging` | Raw dividend/tax rows from `steuerlichedetailansichtexport_*.csv` | `steuerliches_datum`, `vorgang`, `gewinn_verlust`, `processed` |
| `comdirect_transactions_staging` | Buys/sells from `abrechnungsdaten_*.csv` | `datum_ausfuehrung`, `geschaeftsart`, `kundenendbetrag_eur`, `entgelt_eur` |
| `traderepublic_transactions_staging` | TradeRepublic transaction ledger | `date`, `transaction_type`, `shares`, `financial_transaction_tax` |

### Normalized Tables
- `broker_t`, `security_t`, `table_t` – master data used to de-duplicate brokers, securities, and staging sources.
- `transaction_t` – normalized trade history with references to the staging table and row id for traceability.
- `dividend_t` – dividend/interest facts keyed to securities and brokers.

## Scripts & Automation
| Script | Description |
| --- | --- |
| `scripts/load_comdirect_transactions.py` | Imports a Comdirect transaction CSV; auto-detects encodings and writes into `comdirect_transactions_staging`. |
| `scripts/load_comdirect_tax_detail.py` | Ingests dividend/tax exports into `comdirect_tax_detail_staging`. |
| `scripts/load_traderepublic_transactions.py` | Parses TradeRepublic CSV exports with German locale handling and loads `traderepublic_transactions_staging`. |
| `scripts/analyze_transactions.py` | Builds enriched buy/sell analysis CSVs from staged data. |
| `scripts/extract_dividends.py` & `scripts/filter_dividends_with_purchases.py` | Derive dividend datasets that can be matched to holding periods. |
| `scripts/create_aggregated_report.py` | Combines transaction and dividend feeds to compute total return and CAGR per closed position. |
| `scripts/verify_tax_export_completeness.py` | Guards against missing or duplicate staging imports. |

## Getting Started
1. **Environment** – use Python 3.13+ (see `pyproject.toml`). Create and activate a virtual environment, then install dependencies (`pip install -e .` once dependencies are added).
2. **Prepare directories** – the `configuration.py` bootstrap ensures `data/input`, `data/db`, and `log` exist, but you can create them manually if running scripts outside the repo root.
3. **Load data** – run loader scripts from the project root:
	- `python scripts/load_comdirect_transactions.py data/input/abrechnungsdaten_comdirect_20251205.csv`
	- `python scripts/load_comdirect_tax_detail.py data/input/steuerlichedetailansichtexport_9772900462_20251205-1606.csv`
	- `python scripts/load_traderepublic_transactions.py data/input/traderepublic_transactions.csv`
4. **Run analytics** – execute the transformation/analysis scripts as needed, e.g. `python scripts/create_aggregated_report.py` to refresh `data/aggregated_investment_report.csv`.

## Streamlit UI
Once the normalized tables are populated, you can explore the data through a lightweight dashboard:

1. Install the UI dependencies (`pip install streamlit pandas` or `pip install -e .`).
2. Launch the app from the project root with:

	```
	streamlit run src/ui/app.py
	```

The main **Overview** page highlights the total value and count of open positions plus the portfolio-level XIRR (including the transaction date range used for the calculation). A second page, **Open Positions**, lists every holding with the latest trade price so you can filter, sort, and export the detailed view. Both pages default to an **All** asset-type filter, but you can narrow the metrics down to a specific classification whenever needed.

## Data Files
- `data/input/` – raw broker exports (semicolon CSV, Windows-1252 compatible) that remain untouched.
- `data/db/portfolio_tracker.db` – SQLite database containing staging and normalized tables for reproducible analytics.
- `data/*.csv` – generated outputs such as transaction analyses, dividend extracts, and the aggregated investment KPI report.
- `log/loader.log` – combined log for all loader scripts (configured via `configuration.py`).

## Extending the Project
- Add new broker loaders by following the staging/normalization pattern (`staging table` ➜ `table_t` reference ➜ normalized fact table).
- Expand `src/etl/` with additional transformations (e.g., unrealized performance, tax reconciliation).
- Update `pyproject.toml` with production dependencies (e.g., `pandas`, `typer`) and expose CLI entry points via `main.py` if you want a unified interface.

> Need help running a specific script or extending the schema? Open an issue or document the new workflow directly in this README so future runs remain reproducible.
