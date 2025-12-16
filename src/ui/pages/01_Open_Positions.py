from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st


def _bootstrap_project_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").exists():
            root = parent
            break
    else:  # pragma: no cover - fallback for unusual layouts
        root = current.parents[-1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


_PROJECT_ROOT = _bootstrap_project_root()

from src.ui.components.filters import asset_type_selector, resolve_asset_type_filter
from src.services import portfolio_metrics

st.set_page_config(page_title="Open Positions", page_icon="📊", layout="wide")

st.title("Open Positions")
st.write("Dive deeper into every position that still has positive net shares.")

if not portfolio_metrics.database_ready():
    st.error(
        "SQLite database not found. Run the ETL loaders to create data/db/portfolio_tracker.db "
        "and refresh this page."
    )
    st.stop()

asset_type_selection = asset_type_selector()
asset_type_filter = resolve_asset_type_filter(asset_type_selection)
summary = portfolio_metrics.get_open_positions_summary(asset_type_filter=asset_type_filter)

if summary.position_count == 0:
    st.warning("No positions available for the selected asset type.")
    st.stop()

search_term = st.text_input("Filter by security name", placeholder="e.g. MSCI World")

records = []
for position in summary.positions:
    records.append(
        {
            "Security": position.security_name,
            "Net shares": position.net_shares,
            "Last price": position.last_price,
            "Last price date": position.last_price_date.isoformat() if position.last_price_date else None,
            "Position value": position.valuation,
        }
    )

df = pd.DataFrame(records)

if search_term:
    mask = df["Security"].str.contains(search_term, case=False, na=False)
    df = df[mask]

# Render the DataFrame with numeric formatting for the "Position value" column
st.dataframe(
    df,
    column_config={
        "Position value": st.column_config.NumberColumn(
            "Position value",
            format="%.2f",
        )
    },
    use_container_width=True,
)

csv_bytes = df.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download as CSV",
    csv_bytes,
    file_name="open_positions.csv",
    mime="text/csv",
)
