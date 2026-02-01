"""
Correlations Page - Interactive scatter plots for exploring relationships.

This page allows users to explore correlations between different
socio-economic indicators across Brazilian municipalities.
"""

import streamlit as st
import plotly.express as px
import numpy as np

st.set_page_config(
    page_title="Correlações - Brazil Analytics",
    page_icon="📈",
    layout="wide",
)

from dashboard.data.queries import (
    get_correlation_data,
    get_regions,
    WAREHOUSE_PATH,
)

# Available indicators
INDICATORS = {
    "IDHM (2010)": "idhm_2010",
    "IDHM Educação": "idhm_educacao",
    "IDHM Longevidade": "idhm_longevidade",
    "IDHM Renda": "idhm_renda",
    "IVS (Vulnerabilidade)": "ivs_2010",
    "Índice de Gini": "gini_2010",
    "Renda per Capita (R$)": "renda_per_capita_2010",
    "Esperança de Vida (anos)": "esperanca_vida_2010",
    "População": "populacao",
}

# Preset correlations of interest
PRESETS = {
    "IDHM × Gini (Desigualdade)": ("idhm_2010", "gini_2010"),
    "IDHM × Renda per Capita": ("idhm_2010", "renda_per_capita_2010"),
    "Educação × Renda": ("idhm_educacao", "idhm_renda"),
    "Esperança de Vida × Renda": ("esperanca_vida_2010", "renda_per_capita_2010"),
    "IVS × IDHM": ("ivs_2010", "idhm_2010"),
    "Longevidade × Educação": ("idhm_longevidade", "idhm_educacao"),
    "Personalizado": (None, None),
}


def calculate_correlation(x: list, y: list) -> float:
    """Calculate Pearson correlation coefficient."""
    x_arr = np.array([v for v in x if v is not None])
    y_arr = np.array([v for v in y if v is not None])

    if len(x_arr) < 2 or len(y_arr) < 2:
        return 0.0

    # Filter to same length
    min_len = min(len(x_arr), len(y_arr))
    x_arr = x_arr[:min_len]
    y_arr = y_arr[:min_len]

    # Calculate correlation
    x_mean = np.mean(x_arr)
    y_mean = np.mean(y_arr)

    numerator = np.sum((x_arr - x_mean) * (y_arr - y_mean))
    denominator = np.sqrt(np.sum((x_arr - x_mean) ** 2) * np.sum((y_arr - y_mean) ** 2))

    if denominator == 0:
        return 0.0

    return numerator / denominator


def interpret_correlation(r: float) -> tuple[str, str]:
    """Interpret correlation coefficient."""
    abs_r = abs(r)

    if abs_r >= 0.7:
        strength = "Forte"
        color = "#27ae60" if r > 0 else "#e74c3c"
    elif abs_r >= 0.4:
        strength = "Moderada"
        color = "#3498db"
    elif abs_r >= 0.2:
        strength = "Fraca"
        color = "#f39c12"
    else:
        strength = "Muito fraca"
        color = "#95a5a6"

    direction = "positiva" if r > 0 else "negativa"

    return f"{strength} {direction}", color


def main() -> None:
    """Main page content."""
    st.title("📈 Análise de Correlações")
    st.markdown("### Explore relações entre indicadores socioeconômicos")

    # Check data availability
    if not WAREHOUSE_PATH.exists():
        st.error("Dados não disponíveis. Execute o pipeline dbt primeiro.")
        return

    # Sidebar controls
    st.sidebar.header("⚙️ Configurações")

    # Preset selector
    preset = st.sidebar.selectbox(
        "Análise Predefinida",
        list(PRESETS.keys()),
        index=0,
        help="Escolha uma análise predefinida ou personalize",
    )

    preset_x, preset_y = PRESETS[preset]

    # Custom indicator selection
    if preset == "Personalizado":
        st.sidebar.markdown("**Selecione os indicadores:**")

        x_label = st.sidebar.selectbox(
            "Eixo X",
            list(INDICATORS.keys()),
            index=0,
        )
        y_label = st.sidebar.selectbox(
            "Eixo Y",
            list(INDICATORS.keys()),
            index=5,  # Gini by default
        )

        x_indicator = INDICATORS[x_label]
        y_indicator = INDICATORS[y_label]
    else:
        x_indicator = preset_x
        y_indicator = preset_y

        # Find labels
        x_label = [k for k, v in INDICATORS.items() if v == x_indicator][0]
        y_label = [k for k, v in INDICATORS.items() if v == y_indicator][0]

    # Color by option
    color_by = st.sidebar.selectbox(
        "Colorir por",
        ["Região", "Porte do Município", "Nenhum"],
        index=0,
    )

    # Region filter
    regions = ["Todas"] + get_regions()
    selected_region = st.sidebar.selectbox(
        "Filtrar por região",
        regions,
        index=0,
    )

    # Size by population
    size_by_pop = st.sidebar.checkbox(
        "Tamanho proporcional à população",
        value=False,
    )

    # Load data
    try:
        df = get_correlation_data(x_indicator, y_indicator)
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return

    if df.is_empty():
        st.warning("Nenhum dado disponível para os indicadores selecionados.")
        return

    # Filter by region
    if selected_region != "Todas":
        df = df.filter(df["regiao"] == selected_region)

    if df.is_empty():
        st.warning("Nenhum dado disponível para a região selecionada.")
        return

    # Calculate correlation
    x_values = df["x_value"].to_list()
    y_values = df["y_value"].to_list()
    correlation = calculate_correlation(x_values, y_values)
    interpretation, interp_color = interpret_correlation(correlation)

    # Display correlation info
    col_corr, col_desc = st.columns([1, 2])

    with col_corr:
        st.metric(
            "Coeficiente de Correlação (r)",
            f"{correlation:.3f}",
            help="Coeficiente de Pearson: -1 (negativa perfeita) a +1 (positiva perfeita)",
        )
        st.markdown(
            f"<span style='color: {interp_color}; font-size: 1.2em; font-weight: bold;'>"
            f"Correlação {interpretation}</span>",
            unsafe_allow_html=True,
        )

    with col_desc:
        st.info(
            f"""
            **Interpretação:** Uma correlação de r = {correlation:.3f} indica uma relação
            **{interpretation.lower()}** entre **{x_label}** e **{y_label}**.

            {"Quanto maior o " + x_label + ", maior tende a ser o " + y_label + "." if correlation > 0.2 else ""}
            {"Quanto maior o " + x_label + ", menor tende a ser o " + y_label + "." if correlation < -0.2 else ""}
            {"Não há relação linear significativa entre as variáveis." if abs(correlation) <= 0.2 else ""}
            """
        )

    st.markdown("---")

    # Scatter plot
    st.subheader(f"🔍 {x_label} × {y_label}")

    # Prepare plot data
    plot_df = df.to_pandas()

    # Determine color and size columns
    color_col = None
    if color_by == "Região":
        color_col = "regiao"
    elif color_by == "Porte do Município":
        color_col = "porte_municipio"

    size_col = "populacao" if size_by_pop else None

    # Create scatter plot
    fig = px.scatter(
        plot_df,
        x="x_value",
        y="y_value",
        color=color_col,
        size=size_col if size_col else None,
        hover_name="nome_municipio",
        hover_data={
            "sigla_uf": True,
            "regiao": True,
            "populacao": ":,.0f",
            "x_value": ":.3f",
            "y_value": ":.3f",
        },
        labels={
            "x_value": x_label,
            "y_value": y_label,
            "regiao": "Região",
            "porte_municipio": "Porte",
            "populacao": "População",
            "sigla_uf": "UF",
        },
        color_discrete_map={
            "Norte": "#27ae60",
            "Nordeste": "#e74c3c",
            "Sudeste": "#3498db",
            "Sul": "#9b59b6",
            "Centro-Oeste": "#f39c12",
        } if color_col == "regiao" else None,
        trendline="ols",  # Add trend line
    )

    fig.update_layout(
        height=600,
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
    )

    fig.update_traces(
        marker=dict(
            opacity=0.7,
            line=dict(width=0.5, color="white"),
        ),
        selector=dict(mode="markers"),
    )

    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Summary statistics
    st.subheader("📊 Estatísticas Descritivas")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"**{x_label}**")
        x_stats = {
            "Média": df["x_value"].mean(),
            "Mediana": df["x_value"].median(),
            "Desvio Padrão": df["x_value"].std(),
            "Mínimo": df["x_value"].min(),
            "Máximo": df["x_value"].max(),
            "N válidos": df["x_value"].drop_nulls().len(),
        }
        for stat, value in x_stats.items():
            if isinstance(value, float):
                st.write(f"- {stat}: **{value:.3f}**")
            else:
                st.write(f"- {stat}: **{value}**")

    with col2:
        st.markdown(f"**{y_label}**")
        y_stats = {
            "Média": df["y_value"].mean(),
            "Mediana": df["y_value"].median(),
            "Desvio Padrão": df["y_value"].std(),
            "Mínimo": df["y_value"].min(),
            "Máximo": df["y_value"].max(),
            "N válidos": df["y_value"].drop_nulls().len(),
        }
        for stat, value in y_stats.items():
            if isinstance(value, float):
                st.write(f"- {stat}: **{value:.3f}**")
            else:
                st.write(f"- {stat}: **{value}**")

    st.markdown("---")

    # Distribution comparison
    st.subheader("📉 Distribuições")

    col_hist1, col_hist2 = st.columns(2)

    with col_hist1:
        fig = px.histogram(
            plot_df,
            x="x_value",
            nbins=50,
            color=color_col if color_col == "regiao" else None,
            labels={"x_value": x_label},
            color_discrete_map={
                "Norte": "#27ae60",
                "Nordeste": "#e74c3c",
                "Sudeste": "#3498db",
                "Sul": "#9b59b6",
                "Centro-Oeste": "#f39c12",
            } if color_col == "regiao" else None,
        )
        fig.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=20, b=20),
            showlegend=False,
            xaxis_title=x_label,
            yaxis_title="Frequência",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_hist2:
        fig = px.histogram(
            plot_df,
            x="y_value",
            nbins=50,
            color=color_col if color_col == "regiao" else None,
            labels={"y_value": y_label},
            color_discrete_map={
                "Norte": "#27ae60",
                "Nordeste": "#e74c3c",
                "Sudeste": "#3498db",
                "Sul": "#9b59b6",
                "Centro-Oeste": "#f39c12",
            } if color_col == "regiao" else None,
        )
        fig.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=20, b=20),
            showlegend=False,
            xaxis_title=y_label,
            yaxis_title="Frequência",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Footer
    st.markdown("---")
    st.caption(
        "Fonte: Base dos Dados (IBGE, IPEA, Atlas Brasil) | "
        "Dados referentes ao Censo de 2010 | "
        "Correlação calculada usando coeficiente de Pearson"
    )


if __name__ == "__main__":
    main()
