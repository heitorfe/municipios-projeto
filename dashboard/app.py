"""
Brazilian Municipalities Analytics Dashboard.

Main entry point for the Streamlit multi-page application.
This dashboard provides interactive visualizations for analyzing
socio-economic indicators across Brazilian municipalities.
"""

from pathlib import Path

import streamlit as st

# Page configuration - must be first Streamlit command
st.set_page_config(
    page_title="Brazil Municipalities Analytics",
    page_icon="🇧🇷",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": "https://github.com/yourusername/municipios-projeto",
        "Report a bug": "https://github.com/yourusername/municipios-projeto/issues",
        "About": """
        # Brazilian Municipalities Analytics

        A comprehensive data analytics project analyzing socio-economic
        indicators across all 5,570 Brazilian municipalities.

        **Data Sources:**
        - IBGE (Population, GDP)
        - Atlas Brasil / PNUD (IDHM)
        - TSE (Electoral data)
        - SICONFI / Tesouro (Municipal finances)
        - SNIS (Sanitation)
        """,
    },
)

# Constants
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
WAREHOUSE_PATH = DATA_DIR / "warehouse" / "analytics.duckdb"


def check_data_availability() -> bool:
    """Check if the data warehouse exists."""
    return WAREHOUSE_PATH.exists()


def main() -> None:
    """Main application entry point."""
    # Sidebar
    with st.sidebar:
        st.image(
            "https://upload.wikimedia.org/wikipedia/commons/thumb/0/05/Flag_of_Brazil.svg/320px-Flag_of_Brazil.svg.png",
            width=100,
        )
        st.title("🇧🇷 Brazil Analytics")
        st.markdown("---")

        st.markdown(
            """
            ### Navigation
            Use the sidebar to navigate between pages:

            - **Overview**: National KPIs and maps
            - **Municipality Profile**: Deep-dive analysis
            - **Comparisons**: Side-by-side comparison
            - **Rankings**: Sortable tables
            - **Correlations**: Statistical analysis
            """
        )

        st.markdown("---")
        st.markdown(
            """
            ### About

            This dashboard analyzes socio-economic indicators
            across all **5,570 Brazilian municipalities**.

            Data sources:
            - IBGE, Atlas Brasil, TSE, SICONFI, SNIS

            Built with:
            - [Streamlit](https://streamlit.io)
            - [DuckDB](https://duckdb.org)
            - [Plotly](https://plotly.com)
            """
        )

    # Main content
    st.title("🇧🇷 Brazilian Municipalities Analytics")
    st.markdown("### Analyzing socio-economic indicators across 5,570 municipalities")

    # Check data availability
    if not check_data_availability():
        st.warning(
            """
            ⚠️ **Data not yet loaded**

            The analytics database has not been created yet.
            Please run the following commands to set up the data:

            ```bash
            # 1. Extract data from Base dos Dados
            python scripts/extract_data.py

            # 2. Run dbt transformations
            cd dbt_project
            dbt deps
            dbt build
            ```

            After running these commands, refresh this page.
            """
        )

        # Show setup instructions
        with st.expander("📚 Setup Instructions"):
            st.markdown(
                """
                ### Prerequisites

                1. **Google Cloud Account**
                   - Create a project at [console.cloud.google.com](https://console.cloud.google.com)
                   - Enable the BigQuery API
                   - Create a service account with BigQuery User role
                   - Download credentials JSON

                2. **Environment Configuration**
                   ```bash
                   cp .env.example .env
                   # Edit .env with your GCP project ID and credentials path
                   ```

                3. **Install Dependencies**
                   ```bash
                   pip install -e ".[all]"
                   ```
                """
            )
        return

    # Dashboard is ready - show overview
    st.success("✅ Data loaded successfully!")

    # Quick stats
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Municipalities",
            value="5,570",
            help="Total number of Brazilian municipalities",
        )

    with col2:
        st.metric(
            label="States",
            value="27",
            help="26 states + Federal District",
        )

    with col3:
        st.metric(
            label="Regions",
            value="5",
            help="Norte, Nordeste, Sudeste, Sul, Centro-Oeste",
        )

    with col4:
        st.metric(
            label="Population (2022)",
            value="203M",
            help="Total Brazilian population from 2022 Census",
        )

    st.markdown("---")

    # Navigation cards
    st.subheader("📊 Explore the Dashboard")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            """
            #### 🗺️ Overview
            National-level KPIs and choropleth maps showing
            regional variations in development indicators.

            #### 🏛️ Municipality Profile
            Deep-dive into any municipality with historical
            trends and comparative analysis.

            #### ⚖️ Comparisons
            Side-by-side comparison of selected municipalities
            across multiple dimensions.
            """
        )

    with col2:
        st.markdown(
            """
            #### 🏆 Rankings
            Sortable rankings by IDHM, GDP, population,
            sanitation coverage, and more.

            #### 📈 Correlations
            Interactive scatter plots exploring relationships
            between socio-economic indicators.

            #### 📋 Data Explorer
            Browse and export raw data with customizable
            filters and aggregations.
            """
        )

    st.markdown("---")

    # Footer
    st.markdown(
        """
        <div style='text-align: center; color: gray; padding: 20px;'>
            Built with ❤️ using Streamlit, DuckDB, and dbt<br>
            Data from <a href='https://basedosdados.org'>Base dos Dados</a>
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
