"""
Choropleth Maps Page.

Interactive maps showing fiscal metrics at state and regional levels.
Municipality-level maps require external geojson data.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from data.queries import (
    get_connection,
    get_regions,
    get_states,
    MARTS_SCHEMA,
)

# Page config
st.set_page_config(
    page_title="Mapas | Municipios",
    page_icon="🗺️",
    layout="wide",
)

st.title("🗺️ Mapas Fiscais do Brasil")
st.markdown("""
Visualizacao geografica dos indicadores fiscais por **estado** e **regiao**.
Passe o mouse sobre cada area para ver detalhes.
""")

# Sidebar filters
st.sidebar.header("Filtros")
year_options = list(range(2023, 2012, -1))
selected_year = st.sidebar.selectbox("Ano", year_options, index=0)

metric_options = {
    "dependency_ratio": "Dependencia Fiscal (%)",
    "efficiency_index": "Indice de Eficiencia",
    "own_revenue_ratio": "Receita Propria (%)",
    "revenue_effort_index": "Esforco Arrecadatorio",
    "transferencias_per_capita": "Transferencias per Capita (R$)",
    "despesa_total_per_capita": "Despesa per Capita (R$)",
}
selected_metric = st.sidebar.selectbox(
    "Metrica",
    list(metric_options.keys()),
    format_func=lambda x: metric_options[x],
    index=0
)

# Brazilian states mapping for Plotly (ISO codes)
BR_STATES = {
    'AC': 'BR-AC', 'AL': 'BR-AL', 'AP': 'BR-AP', 'AM': 'BR-AM', 'BA': 'BR-BA',
    'CE': 'BR-CE', 'DF': 'BR-DF', 'ES': 'BR-ES', 'GO': 'BR-GO', 'MA': 'BR-MA',
    'MT': 'BR-MT', 'MS': 'BR-MS', 'MG': 'BR-MG', 'PA': 'BR-PA', 'PB': 'BR-PB',
    'PR': 'BR-PR', 'PE': 'BR-PE', 'PI': 'BR-PI', 'RJ': 'BR-RJ', 'RN': 'BR-RN',
    'RS': 'BR-RS', 'RO': 'BR-RO', 'RR': 'BR-RR', 'SC': 'BR-SC', 'SP': 'BR-SP',
    'SE': 'BR-SE', 'TO': 'BR-TO'
}

# Region colors
REGION_COLORS = {
    "Norte": "#27ae60",
    "Nordeste": "#e74c3c",
    "Sudeste": "#3498db",
    "Sul": "#9b59b6",
    "Centro-Oeste": "#f39c12",
}


@st.cache_data(ttl=3600)
def get_state_metrics(year: int) -> pl.DataFrame:
    """Get aggregated metrics by state."""
    conn = get_connection()

    query = f"""
    SELECT
        d.sigla_uf,
        d.regiao,
        COUNT(*) as num_municipios,
        AVG(d.dependency_ratio) as dependency_ratio,
        AVG(d.own_revenue_ratio) as own_revenue_ratio,
        AVG(d.revenue_effort_index) as revenue_effort_index,
        AVG(d.transferencias_per_capita) as transferencias_per_capita,
        AVG(e.efficiency_index) as efficiency_index,
        AVG(e.despesa_total_per_capita) as despesa_total_per_capita,
        AVG(e.social_outcome_score) as social_outcome_score,
        SUM(d.populacao) as populacao_total
    FROM {MARTS_SCHEMA}.mart_dependencia_fiscal d
    LEFT JOIN {MARTS_SCHEMA}.mart_eficiencia_municipal e
        ON d.id_municipio = e.id_municipio AND d.ano = e.ano
    WHERE d.ano = {year}
    GROUP BY d.sigla_uf, d.regiao
    ORDER BY d.sigla_uf
    """

    try:
        return conn.execute(query).pl()
    except Exception:
        return pl.DataFrame()


@st.cache_data(ttl=3600)
def get_region_metrics(year: int) -> pl.DataFrame:
    """Get aggregated metrics by region."""
    conn = get_connection()

    query = f"""
    SELECT
        d.regiao,
        COUNT(*) as num_municipios,
        AVG(d.dependency_ratio) as dependency_ratio,
        AVG(d.own_revenue_ratio) as own_revenue_ratio,
        AVG(d.revenue_effort_index) as revenue_effort_index,
        AVG(d.transferencias_per_capita) as transferencias_per_capita,
        AVG(e.efficiency_index) as efficiency_index,
        AVG(e.despesa_total_per_capita) as despesa_total_per_capita,
        AVG(e.social_outcome_score) as social_outcome_score,
        SUM(d.populacao) as populacao_total
    FROM {MARTS_SCHEMA}.mart_dependencia_fiscal d
    LEFT JOIN {MARTS_SCHEMA}.mart_eficiencia_municipal e
        ON d.id_municipio = e.id_municipio AND d.ano = e.ano
    WHERE d.ano = {year}
    GROUP BY d.regiao
    ORDER BY d.regiao
    """

    try:
        return conn.execute(query).pl()
    except Exception:
        return pl.DataFrame()


@st.cache_data(ttl=3600)
def get_municipality_map_data(year: int, uf: str = "All") -> pl.DataFrame:
    """Get municipality-level data for mapping."""
    conn = get_connection()

    uf_filter = f"AND d.sigla_uf = '{uf}'" if uf != "All" else ""

    query = f"""
    SELECT
        d.id_municipio,
        d.nome_municipio,
        d.sigla_uf,
        d.regiao,
        d.populacao,
        d.dependency_ratio,
        d.own_revenue_ratio,
        d.revenue_effort_index,
        d.transferencias_per_capita,
        d.categoria_dependencia,
        e.efficiency_index,
        e.despesa_total_per_capita,
        e.social_outcome_score,
        e.categoria_eficiencia
    FROM {MARTS_SCHEMA}.mart_dependencia_fiscal d
    LEFT JOIN {MARTS_SCHEMA}.mart_eficiencia_municipal e
        ON d.id_municipio = e.id_municipio AND d.ano = e.ano
    WHERE d.ano = {year}
      {uf_filter}
    """

    try:
        return conn.execute(query).pl()
    except Exception:
        return pl.DataFrame()


# =============================================================================
# STATE MAP
# =============================================================================
st.header("📍 Mapa por Estado")

try:
    state_data = get_state_metrics(selected_year)

    if not state_data.is_empty():
        df = state_data.to_pandas()
        df['iso_code'] = df['sigla_uf'].map(BR_STATES)

        # Create choropleth map
        fig = px.choropleth(
            df,
            geojson="https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson",
            locations='sigla_uf',
            featureidkey="properties.sigla",
            color=selected_metric,
            color_continuous_scale="RdYlGn" if selected_metric in ['efficiency_index', 'own_revenue_ratio', 'revenue_effort_index'] else "RdYlGn_r",
            hover_name='sigla_uf',
            hover_data={
                'regiao': True,
                'num_municipios': True,
                'dependency_ratio': ':.1f',
                'efficiency_index': ':.1f',
                'populacao_total': ':,.0f',
            },
            title=f"{metric_options[selected_metric]} por Estado ({selected_year})",
            labels={
                selected_metric: metric_options[selected_metric],
                'regiao': 'Região',
                'num_municipios': 'Municípios',
                'populacao_total': 'População'
            }
        )

        fig.update_geos(
            fitbounds="locations",
            visible=False,
            bgcolor='rgba(0,0,0,0)'
        )

        fig.update_layout(
            height=600,
            margin={"r": 0, "t": 40, "l": 0, "b": 0},
            geo=dict(bgcolor='rgba(0,0,0,0)')
        )

        st.plotly_chart(fig, use_container_width=True)

        # State ranking table
        st.subheader("📊 Ranking por Estado")
        df_display = df.sort_values(selected_metric, ascending=(selected_metric == 'dependency_ratio'))
        df_display['ranking'] = range(1, len(df_display) + 1)

        st.dataframe(
            df_display[['ranking', 'sigla_uf', 'regiao', 'num_municipios', selected_metric]],
            use_container_width=True,
            hide_index=True,
            column_config={
                'ranking': 'Pos',
                'sigla_uf': 'UF',
                'regiao': 'Região',
                'num_municipios': 'Municípios',
                selected_metric: st.column_config.NumberColumn(
                    metric_options[selected_metric],
                    format="%.1f"
                )
            }
        )
    else:
        st.warning("Dados não disponíveis para o ano selecionado.")
except Exception as e:
    st.error(f"Erro ao carregar mapa: {e}")

# =============================================================================
# REGION COMPARISON
# =============================================================================
st.header("🌎 Comparação Regional")

try:
    region_data = get_region_metrics(selected_year)

    if not region_data.is_empty():
        df_region = region_data.to_pandas()

        col1, col2 = st.columns(2)

        with col1:
            # Bar chart by region
            fig_bar = px.bar(
                df_region,
                x='regiao',
                y=selected_metric,
                color='regiao',
                color_discrete_map=REGION_COLORS,
                title=f"{metric_options[selected_metric]} por Região ({selected_year})",
                labels={
                    'regiao': 'Região',
                    selected_metric: metric_options[selected_metric]
                }
            )
            fig_bar.update_layout(showlegend=False)
            st.plotly_chart(fig_bar, use_container_width=True)

        with col2:
            # Radar chart for all metrics
            metrics_for_radar = ['dependency_ratio', 'efficiency_index', 'own_revenue_ratio']

            fig_radar = go.Figure()

            for _, row in df_region.iterrows():
                fig_radar.add_trace(go.Scatterpolar(
                    r=[row['dependency_ratio'], row['efficiency_index'], row['own_revenue_ratio']],
                    theta=['Dependência (%)', 'Eficiência', 'Receita Própria (%)'],
                    fill='toself',
                    name=row['regiao'],
                    line_color=REGION_COLORS.get(row['regiao'], '#888')
                ))

            fig_radar.update_layout(
                polar=dict(
                    radialaxis=dict(visible=True, range=[0, 100])
                ),
                showlegend=True,
                title=f"Perfil Fiscal por Região ({selected_year})",
                height=400
            )
            st.plotly_chart(fig_radar, use_container_width=True)

        # Region summary table
        st.subheader("📋 Resumo por Região")
        st.dataframe(
            df_region[['regiao', 'num_municipios', 'dependency_ratio', 'efficiency_index', 'own_revenue_ratio', 'populacao_total']],
            use_container_width=True,
            hide_index=True,
            column_config={
                'regiao': 'Região',
                'num_municipios': 'Municípios',
                'dependency_ratio': st.column_config.NumberColumn('Dependência %', format="%.1f"),
                'efficiency_index': st.column_config.NumberColumn('Eficiência', format="%.1f"),
                'own_revenue_ratio': st.column_config.NumberColumn('Receita Própria %', format="%.1f"),
                'populacao_total': st.column_config.NumberColumn('População', format="%,.0f")
            }
        )
except Exception as e:
    st.error(f"Erro ao carregar dados regionais: {e}")

# =============================================================================
# MUNICIPALITY BUBBLE MAP
# =============================================================================
st.header("🔵 Mapa de Municípios")

st.markdown("""
**Selecione um estado** para ver o mapa de bolhas dos municípios.
O tamanho da bolha representa a população e a cor representa a métrica selecionada.
""")

selected_uf_map = st.selectbox(
    "Estado para visualização detalhada",
    ["All"] + get_states(),
    index=0,
    key="uf_bubble_map"
)

try:
    muni_data = get_municipality_map_data(selected_year, selected_uf_map)

    if not muni_data.is_empty():
        df_muni = muni_data.to_pandas()

        # Limit to top 500 municipalities by population for performance
        if len(df_muni) > 500 and selected_uf_map == "All":
            df_muni = df_muni.nlargest(500, 'populacao')
            st.info("Mostrando os 500 municípios mais populosos para melhor performance.")

        # Summary stats
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Municípios", f"{len(df_muni):,}")
        with col2:
            st.metric(
                "Média " + metric_options[selected_metric][:15],
                f"{df_muni[selected_metric].mean():.1f}"
            )
        with col3:
            st.metric(
                "Min",
                f"{df_muni[selected_metric].min():.1f}"
            )
        with col4:
            st.metric(
                "Max",
                f"{df_muni[selected_metric].max():.1f}"
            )

        # Box plot by category
        cat_col = 'categoria_dependencia' if selected_metric == 'dependency_ratio' else 'categoria_eficiencia'
        if cat_col in df_muni.columns:
            fig_box = px.box(
                df_muni,
                x=cat_col,
                y=selected_metric,
                color=cat_col,
                title=f"Distribuição de {metric_options[selected_metric]} por Categoria",
                labels={
                    cat_col: 'Categoria',
                    selected_metric: metric_options[selected_metric]
                }
            )
            fig_box.update_layout(showlegend=False)
            st.plotly_chart(fig_box, use_container_width=True)

        # Histogram
        fig_hist = px.histogram(
            df_muni,
            x=selected_metric,
            nbins=50,
            color='regiao',
            color_discrete_map=REGION_COLORS,
            title=f"Distribuição de {metric_options[selected_metric]}",
            labels={
                selected_metric: metric_options[selected_metric],
                'count': 'Municípios'
            }
        )
        st.plotly_chart(fig_hist, use_container_width=True)

        # Top/Bottom municipalities
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("🏆 Top 10")
            top_10 = df_muni.nlargest(10, selected_metric)
            st.dataframe(
                top_10[['nome_municipio', 'sigla_uf', selected_metric, 'populacao']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    'nome_municipio': 'Município',
                    'sigla_uf': 'UF',
                    selected_metric: st.column_config.NumberColumn(
                        metric_options[selected_metric],
                        format="%.1f"
                    ),
                    'populacao': st.column_config.NumberColumn('População', format="%,.0f")
                }
            )

        with col2:
            st.subheader("📉 Bottom 10")
            bottom_10 = df_muni.nsmallest(10, selected_metric)
            st.dataframe(
                bottom_10[['nome_municipio', 'sigla_uf', selected_metric, 'populacao']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    'nome_municipio': 'Município',
                    'sigla_uf': 'UF',
                    selected_metric: st.column_config.NumberColumn(
                        metric_options[selected_metric],
                        format="%.1f"
                    ),
                    'populacao': st.column_config.NumberColumn('População', format="%,.0f")
                }
            )

    else:
        st.info("Nenhum dado disponível para os filtros selecionados.")

except Exception as e:
    st.error(f"Erro ao carregar dados de municípios: {e}")

# =============================================================================
# METHODOLOGY
# =============================================================================
with st.expander("📚 Sobre os Indicadores"):
    st.markdown("""
    ### Métricas Disponíveis

    **Dependência Fiscal (%)**
    - Percentual da receita municipal proveniente de transferências federais
    - Menor = mais autônomo

    **Índice de Eficiência (0-100)**
    - Mede se o município obtém resultados sociais acima ou abaixo do esperado para seu nível de gasto
    - 50 = média; >50 = eficiente; <50 = ineficiente

    **Receita Própria (%)**
    - Percentual da receita gerada pelo próprio município (tributos locais)
    - Maior = mais autonomia fiscal

    **Esforço Arrecadatório**
    - Receita própria per capita dividida pela mediana nacional
    - >1 = acima da média nacional

    **Transferências per Capita (R$)**
    - Valor médio de transferências federais por habitante

    **Despesa per Capita (R$)**
    - Valor médio de despesas públicas por habitante
    """)

# Footer
st.markdown("---")
st.caption("Dados: SICONFI (Tesouro Nacional), IBGE, Atlas Brasil")
