from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

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
from src.ui.services import portfolio_metrics

st.set_page_config(
    page_title="Portfolio Overview",
    page_icon="💼",
    layout="wide",
)


def _format_currency(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:,.2f} €"


def _format_percentage(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value * 100:,.2f}%"


def _format_date(value: date | None) -> str:
    if value is None:
        return "—"
    return value.strftime("%d %b %Y")


def main() -> None:
    st.title("Overview")
    st.write("Track live positions and cash-flow performance across brokers.")

    if not portfolio_metrics.database_ready():
        st.error(
            "SQLite database not found. Run the ETL loaders to create "
            "data/db/portfolio_tracker.db and then refresh this page."
        )
        st.stop()

    asset_type_selection = asset_type_selector()
    asset_type_filter = resolve_asset_type_filter(asset_type_selection)
    summary = portfolio_metrics.get_open_positions_summary(asset_type_filter=asset_type_filter)
    xirr_value = portfolio_metrics.get_portfolio_xirr(asset_type_filter=asset_type_filter)
    start_date, end_date = portfolio_metrics.get_transaction_date_range(
        asset_type_filter=asset_type_filter
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        valuation_value = summary.total_value if summary.priced_position_count else None
        st.metric(
            "Total open position value",
            _format_currency(valuation_value),
            help="Derived from the most recent trade price per security",
        )
    with col2:
        st.metric(
            "Open positions",
            f"{summary.position_count}",
            help="Distinct securities with positive net shares",
        )
    with col3:
        st.metric(
            "Portfolio XIRR",
            _format_percentage(xirr_value),
            help="Calculated from normalized cash flows in transaction_t",
        )

    st.caption(
        f"Transactions considered for XIRR: {_format_date(start_date)} → {_format_date(end_date)}"
    )

    if summary.position_count == 0:
        st.warning("No open positions detected for the selected asset type.")
    else:
        st.subheader("At a glance")
        st.write(
            "Use the Open Positions page for the full breakdown, including last trade "
            "dates and per-security valuations."
        )
        st.page_link("pages/01_Open_Positions.py", label="View detailed positions →")


if __name__ == "__main__":
    main()
