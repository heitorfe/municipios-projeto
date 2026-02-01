"""
Rankings Page - Municipality rankings by various indicators.

This page displays sortable rankings of municipalities by IDHM,
population, income, and other socio-economic indicators.
"""

import streamlit as st
import plotly.express as px

st.set_page_config(
    page_title="Rankings - Brazil Analytics",
    page_icon="🏆",
    layout="wide",
)

from dashboard.data.queries import (
    get_rankings,
    get_regions,
    load_municipalities_summary,
    WAREHOUSE_PATH,
)

# Available indicators for ranking
INDICATORS = {
    "IDHM (2010)": ("idhm_2010", False, "Índice de 0 a 1"),
    "IDHM Educação": ("idhm_educacao", False, "Índice de 0 a 1"),
    "IDHM Longevidade": ("idhm_longevidade", False, "Índice de 0 a 1"),
    "IDHM Renda": ("idhm_renda", False, "Índice de 0 a 1"),
    "IVS (Vulnerabilidade)": ("ivs_2010", True, "Menor = menos vulnerável"),
    "Índice de Gini": ("gini_2010", True, "Menor = mais igual"),
    "Renda per Capita": ("renda_per_capita_2010", False, "Em R$ mensais"),
    "Esperança de Vida": ("esperanca_vida_2010", False, "Em anos"),
    "População": ("populacao", False, "Habitantes"),
}


def format_value(value: float, indicator: str) -> str:
    """Format a value based on indicator type."""
    if value is None:
        return "N/A"

    if indicator == "populacao":
        if value >= 1_000_000:
            return f"{value / 1_000_000:.1f}M"
        if value >= 1_000:
            return f"{value / 1_000:.0f} mil"
        return f"{int(value)}"

    if indicator == "renda_per_capita_2010":
        return f"R$ {value:,.2f}"

    if indicator == "esperanca_vida_2010":
        return f"{value:.1f} anos"

    return f"{value:.3f}"


def main() -> None:
    """Main page content."""
    st.title("🏆 Rankings de Municípios")
    st.markdown("### Classificação por indicadores socioeconômicos")

    # Check data availability
    if not WAREHOUSE_PATH.exists():
        st.error("Dados não disponíveis. Execute o pipeline dbt primeiro.")
        return

    # Sidebar controls
    st.sidebar.header("⚙️ Configurações")

    # Indicator selector
    selected_label = st.sidebar.selectbox(
        "Indicador",
        list(INDICATORS.keys()),
        index=0,
        help="Selecione o indicador para ranking",
    )

    indicator, lower_is_better, description = INDICATORS[selected_label]
    st.sidebar.caption(description)

    # Ascending/descending
    show_best = st.sidebar.radio(
        "Exibir",
        ["🥇 Melhores", "📉 Piores"],
        index=0,
        horizontal=True,
    )

    # Determine sort order based on indicator and user choice
    if show_best == "🥇 Melhores":
        ascending = lower_is_better  # For Gini/IVS, lower is better
    else:
        ascending = not lower_is_better  # Inverse for "worst"

    # Number of results
    num_results = st.sidebar.slider(
        "Quantidade de resultados",
        min_value=10,
        max_value=500,
        value=50,
        step=10,
    )

    # Region filter
    regions = ["Todas"] + get_regions()
    selected_region = st.sidebar.selectbox(
        "Filtrar por região",
        regions,
        index=0,
    )

    # Load rankings
    try:
        df = get_rankings(indicator, limit=num_results, ascending=ascending)
    except Exception as e:
        st.error(f"Erro ao carregar rankings: {e}")
        return

    if df.is_empty():
        st.warning("Nenhum dado disponível.")
        return

    # Filter by region if selected
    if selected_region != "Todas":
        df = df.filter(df["regiao"] == selected_region)

    # Main content
    title_suffix = "Melhores" if show_best == "🥇 Melhores" else "Piores"
    st.subheader(f"📋 {title_suffix} Municípios por {selected_label}")

    # Two columns: table and chart
    col_table, col_chart = st.columns([1.2, 1])

    with col_table:
        # Prepare display dataframe
        display_df = df.to_pandas()
        display_df = display_df.rename(
            columns={
                "ranking": "Pos.",
                "nome_municipio": "Município",
                "sigla_uf": "UF",
                "regiao": "Região",
                "populacao": "População",
                "porte_municipio": "Porte",
                "valor": selected_label,
            }
        )

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            height=600,
            column_config={
                "Pos.": st.column_config.NumberColumn(width="small"),
                "Município": st.column_config.TextColumn(width="medium"),
                "UF": st.column_config.TextColumn(width="small"),
                "Região": st.column_config.TextColumn(width="small"),
                "População": st.column_config.NumberColumn(format="%.0f"),
                "Porte": st.column_config.TextColumn(width="small"),
                selected_label: st.column_config.NumberColumn(
                    format="%.3f" if indicator not in ["populacao", "renda_per_capita_2010"] else "%.0f"
                ),
            },
        )

    with col_chart:
        # Bar chart of top/bottom municipalities
        chart_data = df.head(20).to_pandas()

        fig = px.bar(
            chart_data,
            y="nome_municipio",
            x="valor",
            color="regiao",
            orientation="h",
            labels={
                "nome_municipio": "",
                "valor": selected_label,
                "regiao": "Região",
            },
            color_discrete_map={
                "Norte": "#27ae60",
                "Nordeste": "#e74c3c",
                "Sudeste": "#3498db",
                "Sul": "#9b59b6",
                "Centro-Oeste": "#f39c12",
            },
        )

        fig.update_layout(
            height=600,
            yaxis={"categoryorder": "total ascending" if not ascending else "total descending"},
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
            ),
        )

        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Regional summary
    st.subheader("🌎 Resumo por Região")

    try:
        # Load full summary for aggregation
        all_data = load_municipalities_summary("All")

        if not all_data.is_empty() and indicator in all_data.columns:
            import polars as pl

            regional_stats = (
                all_data.group_by("regiao")
                .agg([
                    pl.col(indicator).mean().alias("media"),
                    pl.col(indicator).min().alias("minimo"),
                    pl.col(indicator).max().alias("maximo"),
                    pl.col(indicator).std().alias("desvio_padrao"),
                    pl.len().alias("total_municipios"),
                ])
                .sort("media", descending=not lower_is_better)
            )

            col_stats, col_box = st.columns([1, 1.5])

            with col_stats:
                st.dataframe(
                    regional_stats.to_pandas(),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "regiao": "Região",
                        "media": st.column_config.NumberColumn("Média", format="%.3f"),
                        "minimo": st.column_config.NumberColumn("Mínimo", format="%.3f"),
                        "maximo": st.column_config.NumberColumn("Máximo", format="%.3f"),
                        "desvio_padrao": st.column_config.NumberColumn("Desvio Padrão", format="%.3f"),
                        "total_municipios": st.column_config.NumberColumn("Municípios", format="%d"),
                    },
                )

            with col_box:
                # Box plot by region
                fig = px.box(
                    all_data.to_pandas(),
                    x="regiao",
                    y=indicator,
                    color="regiao",
                    labels={
                        "regiao": "Região",
                        indicator: selected_label,
                    },
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
                    margin=dict(l=20, r=20, t=20, b=20),
                )
                st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.warning(f"Não foi possível calcular resumo regional: {e}")

    # Footer
    st.markdown("---")
    st.caption(
        "Fonte: Base dos Dados (IBGE, IPEA, Atlas Brasil) | "
        "Dados de IDHM referentes ao Censo de 2010"
    )


if __name__ == "__main__":
    main()
