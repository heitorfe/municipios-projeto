"""
Overview Page - National KPIs and Choropleth Maps.

This page provides a high-level view of Brazilian municipalities
with interactive choropleth maps colored by various indicators.
"""

from pathlib import Path

import plotly.express as px
import streamlit as st

from dashboard.data.queries import get_connection, load_municipalities_summary

st.set_page_config(page_title="Overview - Brazil Analytics", page_icon="🗺️", layout="wide")

# Constants
PROJECT_ROOT = Path(__file__).parent.parent.parent
WAREHOUSE_PATH = PROJECT_ROOT / "data" / "warehouse" / "analytics.duckdb"


def main() -> None:
    """Main page content."""
    st.title("🗺️ National Overview")
    st.markdown("### Socio-economic indicators across Brazilian municipalities")

    # Check data availability
    if not WAREHOUSE_PATH.exists():
        st.error("Data not available. Please run the ETL pipeline first.")
        return

    # Filters
    st.sidebar.header("Filters")

    # Region filter
    regions = ["All", "Norte", "Nordeste", "Sudeste", "Sul", "Centro-Oeste"]
    selected_region = st.sidebar.selectbox("Region", regions, index=0)

    # Year filter (for IDHM)
    years = [2010, 2000, 1991]
    selected_year = st.sidebar.selectbox("IDHM Year", years, index=0)

    # Indicator selector
    indicators = {
        "IDHM": "idhm",
        "IDHM Education": "idhm_educacao",
        "IDHM Longevity": "idhm_longevidade",
        "IDHM Income": "idhm_renda",
        "Life Expectancy": "esperanca_vida",
        "Literacy Rate": "taxa_analfabetismo_18_mais",
        "Gini Index": "indice_gini",
    }
    selected_indicator = st.sidebar.selectbox("Indicator", list(indicators.keys()))

    # Load data
    try:
        df = load_municipalities_summary(selected_year, selected_region)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.info("Make sure you've run `dbt build` in the dbt_project directory.")
        return

    if df.is_empty():
        st.warning("No data available for the selected filters.")
        return

    # KPI Cards
    st.markdown("### Key Metrics")
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "Municipalities",
            f"{len(df):,}",
            help="Number of municipalities in selection",
        )

    with col2:
        avg_idhm = df["idhm"].mean()
        st.metric(
            "Avg. IDHM",
            f"{avg_idhm:.3f}" if avg_idhm else "N/A",
            help="Average Human Development Index",
        )

    with col3:
        total_pop = df["populacao"].sum() if "populacao" in df.columns else 0
        st.metric(
            "Total Population",
            f"{total_pop / 1_000_000:.1f}M" if total_pop else "N/A",
            help="Total population in selection",
        )

    with col4:
        avg_gini = df["indice_gini"].mean() if "indice_gini" in df.columns else None
        st.metric(
            "Avg. Gini",
            f"{avg_gini:.3f}" if avg_gini else "N/A",
            help="Average inequality index (0=equal, 1=unequal)",
        )

    with col5:
        avg_life = df["esperanca_vida"].mean() if "esperanca_vida" in df.columns else None
        st.metric(
            "Life Expectancy",
            f"{avg_life:.1f} yrs" if avg_life else "N/A",
            help="Average life expectancy at birth",
        )

    st.markdown("---")

    # Two columns: Map and Distribution
    col_map, col_dist = st.columns([2, 1])

    with col_map:
        st.subheader("Geographic Distribution")

        # Placeholder for choropleth map
        st.info(
            """
            🗺️ **Choropleth Map Coming Soon**

            This section will display an interactive map of Brazil
            colored by the selected indicator.

            To enable maps, you need to:
            1. Download GeoJSON boundaries from IBGE or geobr package
            2. Join with the municipality data
            3. Render with Plotly choropleth_mapbox

            For now, see the regional bar chart below.
            """
        )

    with col_dist:
        st.subheader("Distribution")

        # Histogram of selected indicator
        indicator_col = indicators[selected_indicator]
        if indicator_col in df.columns:
            fig = px.histogram(
                df.to_pandas(),
                x=indicator_col,
                nbins=50,
                title=f"Distribution of {selected_indicator}",
                labels={indicator_col: selected_indicator},
            )
            fig.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Regional comparison
    st.subheader("Regional Comparison")

    if "regiao" in df.columns and "idhm" in df.columns:
        regional_stats = (
            df.group_by("regiao")
            .agg([
                pl.col("idhm").mean().alias("avg_idhm"),
                pl.col("populacao").sum().alias("total_pop"),
                pl.len().alias("num_municipios"),
            ])
            .sort("avg_idhm", descending=True)
        )

        fig = px.bar(
            regional_stats.to_pandas(),
            x="regiao",
            y="avg_idhm",
            color="regiao",
            title="Average IDHM by Region",
            labels={"regiao": "Region", "avg_idhm": "Average IDHM"},
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # State-level breakdown
    st.subheader("State-Level Statistics")

    if "sigla_uf" in df.columns:
        state_stats = (
            df.group_by("sigla_uf")
            .agg([
                pl.col("idhm").mean().alias("avg_idhm"),
                pl.col("idhm").min().alias("min_idhm"),
                pl.col("idhm").max().alias("max_idhm"),
                pl.len().alias("num_municipios"),
            ])
            .sort("avg_idhm", descending=True)
        )

        st.dataframe(
            state_stats.to_pandas(),
            use_container_width=True,
            hide_index=True,
            column_config={
                "sigla_uf": "State",
                "avg_idhm": st.column_config.NumberColumn("Avg IDHM", format="%.3f"),
                "min_idhm": st.column_config.NumberColumn("Min IDHM", format="%.3f"),
                "max_idhm": st.column_config.NumberColumn("Max IDHM", format="%.3f"),
                "num_municipios": "# Municipalities",
            },
        )


# Import polars for DataFrame operations
try:
    import polars as pl
except ImportError:
    st.error("Polars not installed. Run: pip install polars")
    st.stop()

if __name__ == "__main__":
    main()
