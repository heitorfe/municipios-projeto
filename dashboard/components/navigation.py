"""
Navigation utilities for the Municipios Analytics dashboard.

Provides session state management for cross-page navigation,
particularly for navigating from ranking tables to municipality profiles.
"""

import streamlit as st
import pandas as pd
from typing import Optional, List, Dict, Any


def navigate_to_municipality(id_municipio: str, nome_municipio: str = "") -> None:
    """
    Store municipality in session state and navigate to profile page.

    Args:
        id_municipio: IBGE 7-digit municipality code.
        nome_municipio: Municipality name for display purposes.
    """
    st.session_state["selected_municipio_id"] = id_municipio
    st.session_state["selected_municipio_name"] = nome_municipio
    st.switch_page("pages/2_Municipality_Profile.py")


def get_selected_municipality() -> Optional[str]:
    """
    Get municipality ID from session state if navigated from another page.

    Returns:
        Municipality ID if set, None otherwise.
    """
    return st.session_state.get("selected_municipio_id")


def get_selected_municipality_name() -> Optional[str]:
    """
    Get municipality name from session state if navigated from another page.

    Returns:
        Municipality name if set, None otherwise.
    """
    return st.session_state.get("selected_municipio_name")


def clear_navigation_state() -> None:
    """Clear municipality selection from session state."""
    st.session_state.pop("selected_municipio_id", None)
    st.session_state.pop("selected_municipio_name", None)


def render_clickable_ranking_table(
    df: pd.DataFrame,
    display_columns: List[str],
    column_config: Dict[str, Any],
    id_column: str = "id_municipio",
    name_column: str = "nome_municipio",
    key: str = "ranking_table",
    height: int = 400,
) -> None:
    """
    Render a ranking table with clickable municipality names.

    Uses Streamlit's dataframe selection to enable row clicks that
    navigate to the municipality profile page.

    Args:
        df: DataFrame with ranking data.
        display_columns: List of columns to display.
        column_config: Streamlit column configuration dict.
        id_column: Column name containing municipality IDs.
        name_column: Column name containing municipality names.
        key: Unique key for the dataframe widget.
        height: Table height in pixels.
    """
    # Ensure id_column exists in dataframe
    if id_column not in df.columns:
        st.warning(f"Column '{id_column}' not found in data.")
        st.dataframe(df[display_columns], use_container_width=True, hide_index=True)
        return

    # Create selection dataframe with all necessary columns
    selection_df = df.copy()

    # Display the dataframe with selection enabled
    event = st.dataframe(
        selection_df[display_columns],
        use_container_width=True,
        hide_index=True,
        height=height,
        column_config=column_config,
        on_select="rerun",
        selection_mode="single-row",
        key=key,
    )

    # Handle row selection
    if event and event.selection and event.selection.rows:
        selected_row_idx = event.selection.rows[0]
        selected_id = str(selection_df.iloc[selected_row_idx][id_column])
        selected_name = str(selection_df.iloc[selected_row_idx].get(name_column, ""))

        # Navigate to municipality profile
        navigate_to_municipality(selected_id, selected_name)
