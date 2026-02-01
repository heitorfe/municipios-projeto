"""
Overview Page - National KPIs and Choropleth Maps.

This page provides a high-level view of Brazilian municipalities
with interactive choropleth maps colored by various indicators.
"""

import streamlit as st
import plotly.express as px
import polars as pl

st.set_page_config(page_title="Overview - Brazil Analytics", page_icon="🗺️", layout="wide")

# Import query functions
from dashboard.data.queries import (
    get_database_stats,
    load_municipalities_summary,
    get_regional_summary,
    get_state_summary,
    get_regions,
    WAREHOUSE_PATH,
)


# Indicator configuration
INDICATORS = {
    "IDHM (2010)": "idhm_2010",
    "IDHM Educação": "idhm_educacao",
    "IDHM Longevidade": "idhm_longevidade",
    "IDHM Renda": "idhm_renda",
    "IVS (2010)": "ivs_2010",
    "Gini (2010)": "gini_2010",
    "Renda per Capita": "renda_per_capita_2010",
    "Esperança de Vida": "esperanca_vida_2010",
    "População": "populacao",
}


def format_number(value: float, decimals: int = 0) -> str:
    """Format a number for display."""
    if value is None:
        return "N/A"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}k"
    if decimals > 0:
        return f"{value:.{decimals}f}"
    return f"{int(value):,}"


def main() -> None:
    """Main page content."""
    st.title("🗺️ Visão Geral Nacional")
    st.markdown("### Indicadores socioeconômicos dos municípios brasileiros")

    # Check data availability
    if not WAREHOUSE_PATH.exists():
        st.error("Dados não disponíveis. Execute o pipeline dbt primeiro.")
        st.code("cd dbt_project && dbt build", language="bash")
        return

    # Sidebar filters
    st.sidebar.header("🔍 Filtros")

    # Region filter
    regions = ["Todas"] + get_regions()
    selected_region = st.sidebar.selectbox("Região", regions, index=0)
    region_filter = "All" if selected_region == "Todas" else selected_region

    # Indicator selector
    selected_indicator_label = st.sidebar.selectbox(
        "Indicador para visualização",
        list(INDICATORS.keys()),
        index=0,
    )
    selected_indicator = INDICATORS[selected_indicator_label]

    # Load data
    try:
        df = load_municipalities_summary(region_filter)
        stats = get_database_stats()
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        st.info("Certifique-se de ter executado `dbt build` no diretório dbt_project.")
        return

    if df.is_empty():
        st.warning("Nenhum dado disponível para os filtros selecionados.")
        return

    # KPI Cards
    st.markdown("### 📊 Métricas Principais")
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "Municípios",
            f"{len(df):,}",
            help="Número de municípios na seleção",
        )

    with col2:
        avg_idhm = df["idhm_2010"].mean()
        st.metric(
            "IDHM Médio",
            f"{avg_idhm:.3f}" if avg_idhm else "N/A",
            help="Índice de Desenvolvimento Humano Municipal médio (2010)",
        )

    with col3:
        total_pop = df["populacao"].sum() if "populacao" in df.columns else 0
        st.metric(
            "População Total",
            format_number(total_pop),
            help="População total na seleção",
        )

    with col4:
        avg_gini = df["gini_2010"].mean() if "gini_2010" in df.columns else None
        st.metric(
            "Gini Médio",
            f"{avg_gini:.3f}" if avg_gini else "N/A",
            help="Índice de desigualdade médio (0=igual, 1=desigual)",
        )

    with col5:
        avg_life = df["esperanca_vida_2010"].mean() if "esperanca_vida_2010" in df.columns else None
        st.metric(
            "Esperança de Vida",
            f"{avg_life:.1f} anos" if avg_life else "N/A",
            help="Esperança de vida média ao nascer",
        )

    st.markdown("---")

    # Two columns: Distribution and IDHM breakdown
    col_dist, col_breakdown = st.columns([1, 1])

    with col_dist:
        st.subheader(f"📈 Distribuição: {selected_indicator_label}")

        # Histogram of selected indicator
        if selected_indicator in df.columns:
            df_pd = df.select([selected_indicator]).drop_nulls().to_pandas()
            fig = px.histogram(
                df_pd,
                x=selected_indicator,
                nbins=50,
                labels={selected_indicator: selected_indicator_label},
                color_discrete_sequence=["#1f77b4"],
            )
            fig.update_layout(
                showlegend=False,
                height=350,
                xaxis_title=selected_indicator_label,
                yaxis_title="Frequência",
                margin=dict(l=20, r=20, t=20, b=20),
            )
            st.plotly_chart(fig, use_container_width=True)

    with col_breakdown:
        st.subheader("🎯 Composição do IDHM")

        # IDHM components breakdown
        if all(c in df.columns for c in ["idhm_educacao", "idhm_longevidade", "idhm_renda"]):
            idhm_data = {
                "Componente": ["Educação", "Longevidade", "Renda"],
                "Média": [
                    float(df["idhm_educacao"].mean() or 0),
                    float(df["idhm_longevidade"].mean() or 0),
                    float(df["idhm_renda"].mean() or 0),
                ],
            }

            fig = px.bar(
                idhm_data,
                x="Componente",
                y="Média",
                color="Componente",
                color_discrete_sequence=["#2ecc71", "#3498db", "#e74c3c"],
            )
            fig.update_layout(
                showlegend=False,
                height=350,
                yaxis_range=[0, 1],
                yaxis_title="Índice (0-1)",
                margin=dict(l=20, r=20, t=20, b=20),
            )
            fig.add_hline(
                y=float(df["idhm_2010"].mean() or 0),
                line_dash="dash",
                line_color="gray",
                annotation_text="IDHM Geral",
            )
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Regional comparison
    st.subheader("🌎 Comparação Regional")

    try:
        regional_stats = get_regional_summary()

        if not regional_stats.is_empty():
            col_bar, col_table = st.columns([1.2, 1])

            with col_bar:
                # Bar chart by region
                fig = px.bar(
                    regional_stats.to_pandas(),
                    x="regiao",
                    y="avg_idhm",
                    color="regiao",
                    labels={"regiao": "Região", "avg_idhm": "IDHM Médio"},
                    color_discrete_map={
                        "Norte": "#27ae60",
                        "Nordeste": "#e74c3c",
                        "Sudeste": "#3498db",
                        "Sul": "#9b59b6",
                        "Centro-Oeste": "#f39c12",
                    },
                )
                fig.update_layout(
                    showlegend=False,
                    height=400,
                    yaxis_range=[0.5, 0.85],
                    xaxis_title="",
                    margin=dict(l=20, r=20, t=40, b=20),
                )
                st.plotly_chart(fig, use_container_width=True)

            with col_table:
                # Summary table
                st.dataframe(
                    regional_stats.select([
                        "regiao",
                        "num_municipios",
                        "total_populacao",
                        "avg_idhm",
                        "avg_ivs",
                        "avg_gini",
                    ]).to_pandas(),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "regiao": "Região",
                        "num_municipios": st.column_config.NumberColumn("Municípios", format="%d"),
                        "total_populacao": st.column_config.NumberColumn("População", format="%.0f"),
                        "avg_idhm": st.column_config.NumberColumn("IDHM Médio", format="%.3f"),
                        "avg_ivs": st.column_config.NumberColumn("IVS Médio", format="%.3f"),
                        "avg_gini": st.column_config.NumberColumn("Gini Médio", format="%.3f"),
                    },
                )

    except Exception as e:
        st.warning(f"Não foi possível carregar dados regionais: {e}")

    st.markdown("---")

    # State-level breakdown
    st.subheader("🏛️ Estatísticas por Estado")

    try:
        state_stats = get_state_summary()

        if not state_stats.is_empty():
            # Sort by IDHM
            state_df = state_stats.sort("avg_idhm", descending=True)

            # Create tabs for different views
            tab_table, tab_chart = st.tabs(["📋 Tabela", "📊 Gráfico"])

            with tab_table:
                st.dataframe(
                    state_df.select([
                        "sigla_uf",
                        "nome_uf",
                        "regiao",
                        "num_municipios",
                        "total_populacao",
                        "avg_idhm",
                        "min_idhm",
                        "max_idhm",
                        "avg_esperanca_vida",
                    ]).to_pandas(),
                    use_container_width=True,
                    hide_index=True,
                    height=400,
                    column_config={
                        "sigla_uf": "UF",
                        "nome_uf": "Estado",
                        "regiao": "Região",
                        "num_municipios": st.column_config.NumberColumn("Municípios", format="%d"),
                        "total_populacao": st.column_config.NumberColumn("População", format="%.0f"),
                        "avg_idhm": st.column_config.NumberColumn("IDHM Médio", format="%.3f"),
                        "min_idhm": st.column_config.NumberColumn("IDHM Mín", format="%.3f"),
                        "max_idhm": st.column_config.NumberColumn("IDHM Máx", format="%.3f"),
                        "avg_esperanca_vida": st.column_config.NumberColumn("Esperança Vida", format="%.1f"),
                    },
                )

            with tab_chart:
                # Horizontal bar chart
                fig = px.bar(
                    state_df.to_pandas(),
                    y="sigla_uf",
                    x="avg_idhm",
                    color="regiao",
                    orientation="h",
                    labels={"sigla_uf": "Estado", "avg_idhm": "IDHM Médio", "regiao": "Região"},
                    color_discrete_map={
                        "Norte": "#27ae60",
                        "Nordeste": "#e74c3c",
                        "Sudeste": "#3498db",
                        "Sul": "#9b59b6",
                        "Centro-Oeste": "#f39c12",
                    },
                )
                fig.update_layout(
                    height=700,
                    yaxis={"categoryorder": "total ascending"},
                    margin=dict(l=20, r=20, t=20, b=20),
                )
                st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.warning(f"Não foi possível carregar dados estaduais: {e}")

    st.markdown("---")

    # IDHM distribution by category
    st.subheader("📊 Distribuição por Faixa de IDHM")

    if "faixa_idhm" in df.columns:
        faixa_counts = (
            df.group_by("faixa_idhm")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
        )

        # Order categories correctly
        order = ["Muito Alto", "Alto", "Médio", "Baixo", "Muito Baixo"]
        colors = {
            "Muito Alto": "#27ae60",
            "Alto": "#2ecc71",
            "Médio": "#f1c40f",
            "Baixo": "#e67e22",
            "Muito Baixo": "#e74c3c",
        }

        fig = px.pie(
            faixa_counts.to_pandas(),
            values="count",
            names="faixa_idhm",
            color="faixa_idhm",
            color_discrete_map=colors,
            category_orders={"faixa_idhm": order},
        )
        fig.update_layout(
            height=400,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)

    # Footer
    st.markdown("---")
    st.caption(
        "Dados: Base dos Dados (IBGE, IPEA, Atlas Brasil) | "
        "Processamento: dbt-duckdb | "
        "Visualização: Streamlit + Plotly"
    )


if __name__ == "__main__":
    main()
