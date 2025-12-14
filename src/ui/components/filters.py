"""Reusable UI controls."""

from __future__ import annotations

from typing import List, Optional

import streamlit as st

from src.ui.services.portfolio_metrics import get_asset_type_options

ALL_ASSET_TYPES_OPTION = "All"


def resolve_asset_type_filter(selection: Optional[str]) -> Optional[str]:
    """Normalize the UI selection so callers can pass ``None`` for "All"."""

    if selection is None:
        return None
    if selection.lower() == ALL_ASSET_TYPES_OPTION.lower():
        return None
    return selection


def asset_type_selector(label: str = "Asset type") -> str:
    """Render the shared asset type dropdown and return the selection."""

    raw_options: List[str] = get_asset_type_options()
    filtered_options = [opt for opt in raw_options if opt.lower() != ALL_ASSET_TYPES_OPTION.lower()]
    options = [ALL_ASSET_TYPES_OPTION, *filtered_options]
    stored_choice = st.session_state.get("asset_type_filter")
    default_value = stored_choice if stored_choice in options else ALL_ASSET_TYPES_OPTION
    default_index = options.index(default_value)

    return st.selectbox(
        label,
        options,
        index=default_index,
        key="asset_type_filter",
        help="Asset classification comes from security_t.asset_type",
    )
