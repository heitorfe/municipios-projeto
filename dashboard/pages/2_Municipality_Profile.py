"""
Municipality Profile Page - Deep-dive into a single municipality.

This page allows users to search for and explore detailed information
about any Brazilian municipality, including IDHM history and comparisons.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Perfil Municipal - Brazil Analytics",
    page_icon="🏛️",
    layout="wide",
)

from dashboard.data.queries import (
    search_municipalities,
    get_municipality_profile,
    get_municipality_indicators_history,
    get_electoral_summary,
    get_financial_summary,
    get_state_summary,
    WAREHOUSE_PATH,
)


def format_currency(value: float) -> str:
    """Format a value as Brazilian currency."""
    if value is None:
        return "N/A"
    if value >= 1_000_000_000:
        return f"R$ {value / 1_000_000_000:.1f} bi"
    if value >= 1_000_000:
        return f"R$ {value / 1_000_000:.1f} mi"
    if value >= 1_000:
        return f"R$ {value / 1_000:.1f} mil"
    return f"R$ {value:.2f}"


def format_population(value: int) -> str:
    """Format population number."""
    if value is None:
        return "N/A"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.0f} mil"
    return str(value)


def get_idhm_color(idhm: float) -> str:
    """Get color based on IDHM value."""
    if idhm is None:
        return "#95a5a6"
    if idhm >= 0.800:
        return "#27ae60"  # Muito Alto - green
    if idhm >= 0.700:
        return "#2ecc71"  # Alto - light green
    if idhm >= 0.600:
        return "#f1c40f"  # Médio - yellow
    if idhm >= 0.500:
        return "#e67e22"  # Baixo - orange
    return "#e74c3c"  # Muito Baixo - red


def main() -> None:
    """Main page content."""
    st.title("🏛️ Perfil do Município")
    st.markdown("### Análise detalhada de indicadores municipais")

    # Check data availability
    if not WAREHOUSE_PATH.exists():
        st.error("Dados não disponíveis. Execute o pipeline dbt primeiro.")
        return

    # Municipality search
    st.sidebar.header("🔍 Buscar Município")

    search_query = st.sidebar.text_input(
        "Nome do município",
        placeholder="Ex: São Paulo, Curitiba...",
        help="Digite parte do nome do município",
    )

    selected_municipio = None

    if search_query and len(search_query) >= 2:
        results = search_municipalities(search_query, limit=10)

        if not results.is_empty():
            # Create display options
            options = []
            id_map = {}
            for row in results.to_dicts():
                display_name = f"{row['nome_municipio']} - {row['sigla_uf']}"
                options.append(display_name)
                id_map[display_name] = row["id_municipio_ibge"]

            selected_option = st.sidebar.selectbox(
                "Selecione o município",
                options,
                index=0,
            )
            selected_municipio = id_map.get(selected_option)
        else:
            st.sidebar.warning("Nenhum município encontrado.")
    else:
        st.sidebar.info("Digite pelo menos 2 caracteres para buscar.")

    if not selected_municipio:
        # Show instructions
        st.info(
            """
            👈 **Use a barra lateral para buscar um município**

            Digite o nome do município na caixa de busca e selecione
            o resultado desejado para ver informações detalhadas.
            """
        )

        # Show some statistics
        st.subheader("🎯 Destaques")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 🏆 Top 5 IDHM")
            try:
                from dashboard.data.queries import get_rankings
                top_5 = get_rankings("idhm_2010", limit=5, ascending=False)
                if not top_5.is_empty():
                    for i, row in enumerate(top_5.to_dicts(), 1):
                        st.write(
                            f"{i}. **{row['nome_municipio']}** ({row['sigla_uf']}) - "
                            f"IDHM: {row['valor']:.3f}"
                        )
            except Exception:
                st.write("Dados não disponíveis")

        with col2:
            st.markdown("#### 📈 Maiores Municípios")
            try:
                from dashboard.data.queries import get_rankings
                top_pop = get_rankings("populacao", limit=5, ascending=False)
                if not top_pop.is_empty():
                    for i, row in enumerate(top_pop.to_dicts(), 1):
                        st.write(
                            f"{i}. **{row['nome_municipio']}** ({row['sigla_uf']}) - "
                            f"Pop: {format_population(row['valor'])}"
                        )
            except Exception:
                st.write("Dados não disponíveis")

        return

    # Load municipality profile
    profile = get_municipality_profile(selected_municipio)

    if not profile:
        st.error("Município não encontrado.")
        return

    # Header with municipality name and key info
    st.markdown(f"## {profile.get('nome_municipio', 'N/A')} - {profile.get('sigla_uf', '')}")

    # Location info
    st.markdown(
        f"**{profile.get('nome_uf', '')}** | "
        f"Região: **{profile.get('regiao', 'N/A')}** | "
        f"Porte: **{profile.get('porte_municipio', 'N/A')}**"
    )

    if profile.get("is_capital"):
        st.markdown("🏛️ **Capital de Estado**")
    if profile.get("is_amazonia_legal"):
        st.markdown("🌳 **Amazônia Legal**")

    st.markdown("---")

    # Key metrics in cards
    col1, col2, col3, col4 = st.columns(4)

    idhm = profile.get("idhm_2010")
    idhm_color = get_idhm_color(idhm)

    with col1:
        st.metric(
            "IDHM (2010)",
            f"{idhm:.3f}" if idhm else "N/A",
            help=f"Faixa: {profile.get('faixa_idhm', 'N/A')}",
        )
        if idhm:
            st.markdown(
                f"<span style='color: {idhm_color}; font-weight: bold;'>"
                f"● {profile.get('faixa_idhm', '')}</span>",
                unsafe_allow_html=True,
            )

    with col2:
        pop = profile.get("populacao")
        st.metric(
            "População",
            format_population(pop) if pop else "N/A",
            help=f"Ano: {profile.get('ano_populacao', 'N/A')}",
        )

    with col3:
        renda = profile.get("renda_per_capita_2010")
        st.metric(
            "Renda per Capita",
            format_currency(renda) if renda else "N/A",
            help="Renda média mensal per capita (2010)",
        )

    with col4:
        vida = profile.get("esperanca_vida_2010")
        st.metric(
            "Esperança de Vida",
            f"{vida:.1f} anos" if vida else "N/A",
            help="Esperança de vida ao nascer (2010)",
        )

    st.markdown("---")

    # IDHM Components
    st.subheader("📊 Composição do IDHM")

    col_radar, col_bar = st.columns([1, 1])

    with col_radar:
        # Radar chart for IDHM components
        components = {
            "Educação": profile.get("idhm_educacao", 0),
            "Longevidade": profile.get("idhm_longevidade", 0),
            "Renda": profile.get("idhm_renda", 0),
        }

        if all(v for v in components.values()):
            categories = list(components.keys())
            values = list(components.values())
            values.append(values[0])  # Close the radar

            fig = go.Figure()

            fig.add_trace(
                go.Scatterpolar(
                    r=values,
                    theta=categories + [categories[0]],
                    fill="toself",
                    name="IDHM",
                    line_color="#3498db",
                    fillcolor="rgba(52, 152, 219, 0.3)",
                )
            )

            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 1],
                    )
                ),
                showlegend=False,
                height=300,
                margin=dict(l=60, r=60, t=40, b=40),
            )

            st.plotly_chart(fig, use_container_width=True)

    with col_bar:
        # Bar chart comparison
        if all(v for v in components.values()):
            fig = px.bar(
                x=list(components.keys()),
                y=list(components.values()),
                color=list(components.keys()),
                color_discrete_sequence=["#2ecc71", "#3498db", "#e74c3c"],
            )
            fig.update_layout(
                showlegend=False,
                yaxis_range=[0, 1],
                yaxis_title="Índice",
                xaxis_title="",
                height=300,
                margin=dict(l=20, r=20, t=20, b=20),
            )
            fig.add_hline(
                y=idhm or 0,
                line_dash="dash",
                line_color="gray",
                annotation_text="IDHM Geral",
            )
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Additional indicators
    st.subheader("📈 Indicadores Adicionais")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**Desigualdade**")
        gini = profile.get("gini_2010")
        st.write(f"Índice de Gini: **{gini:.3f}**" if gini else "Gini: N/A")

        ivs = profile.get("ivs_2010")
        faixa_ivs = profile.get("faixa_ivs", "")
        st.write(f"IVS: **{ivs:.3f}** ({faixa_ivs})" if ivs else "IVS: N/A")

    with col2:
        st.markdown("**Geografia**")
        st.write(f"Mesorregião: **{profile.get('mesorregiao', 'N/A')}**")
        st.write(f"Microrregião: **{profile.get('microrregiao', 'N/A')}**")
        if profile.get("nome_regiao_metropolitana"):
            st.write(f"Região Metropolitana: **{profile.get('nome_regiao_metropolitana')}**")

    with col3:
        st.markdown("**Identificadores**")
        st.write(f"Código IBGE: `{profile.get('id_municipio_ibge', 'N/A')}`")
        st.write(f"Código TSE: `{profile.get('id_municipio_tse', 'N/A')}`")
        if profile.get("ddd"):
            st.write(f"DDD: `{profile.get('ddd')}`")

    st.markdown("---")

    # Historical evolution (if available)
    st.subheader("📉 Evolução Histórica do IDHM")

    history = get_municipality_indicators_history(selected_municipio)

    if not history.is_empty():
        # Line chart for IDHM evolution
        fig = px.line(
            history.to_pandas(),
            x="ano",
            y=["idhm", "idhm_educacao", "idhm_longevidade", "idhm_renda"],
            markers=True,
            labels={
                "ano": "Ano",
                "value": "Índice",
                "variable": "Componente",
            },
        )

        # Rename legend entries
        newnames = {
            "idhm": "IDHM Geral",
            "idhm_educacao": "Educação",
            "idhm_longevidade": "Longevidade",
            "idhm_renda": "Renda",
        }
        fig.for_each_trace(lambda t: t.update(name=newnames.get(t.name, t.name)))

        fig.update_layout(
            yaxis_range=[0, 1],
            xaxis_title="Ano do Censo",
            yaxis_title="Índice",
            legend_title="Componente",
            height=400,
            margin=dict(l=20, r=20, t=20, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Show change between censuses
        if len(history) > 1:
            first = history.to_dicts()[0]
            last = history.to_dicts()[-1]

            delta = (last.get("idhm", 0) or 0) - (first.get("idhm", 0) or 0)
            pct_change = (delta / (first.get("idhm", 1) or 1)) * 100 if first.get("idhm") else 0

            st.info(
                f"📈 Variação do IDHM de {first['ano']} a {last['ano']}: "
                f"**{delta:+.3f}** ({pct_change:+.1f}%)"
            )
    else:
        st.info("Histórico de IDHM não disponível para este município.")

    # Footer
    st.markdown("---")
    st.caption(
        "Fonte: Base dos Dados (IBGE, IPEA, Atlas Brasil) | "
        "Dados de IDHM referentes aos Censos de 2000 e 2010"
    )


if __name__ == "__main__":
    main()
