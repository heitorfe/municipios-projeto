"""
Cluster Analysis Page - Municipality Development Tiers.

Analyzes municipalities grouped by development level using K-Means clustering
based on IDHM, IVS, Gini, and income indicators.
"""

import streamlit as st
import plotly.express as px
import polars as pl

from data.queries import (
    get_cluster_summary,
    get_cluster_municipalities,
    get_cluster_distribution_by_region,
    get_cluster_distribution_by_state,
    get_cluster_transitions_potential,
    get_cluster_scatter_data,
    get_regions,
    get_states,
)
from components.navigation import render_clickable_ranking_table

# Page config
st.set_page_config(
    page_title="Analise de Clusters | Municipios",
    page_icon="🎯",
    layout="wide",
)

st.title("🎯 Analise de Clusters - Niveis de Desenvolvimento")
st.markdown("""
Municipios brasileiros agrupados em **5 niveis de desenvolvimento** com base em
indicadores socioeconomicos (IDHM, IVS, Gini, Renda per Capita).
""")

# Cluster color scheme (green=best, red=worst)
CLUSTER_COLORS = {
    0: "#27ae60",  # Green - Development Poles
    1: "#2ecc71",  # Light Green - Advanced Development
    2: "#f1c40f",  # Yellow - Developing
    3: "#e67e22",  # Orange - Vulnerable
    4: "#e74c3c",  # Red - Critical
}

CLUSTER_LABELS = {
    0: "Polos de Desenvolvimento",
    1: "Desenvolvimento Avancado",
    2: "Em Desenvolvimento",
    3: "Vulneraveis",
    4: "Criticos",
}

REGION_COLORS = {
    "Norte": "#27ae60",
    "Nordeste": "#e74c3c",
    "Sudeste": "#3498db",
    "Sul": "#9b59b6",
    "Centro-Oeste": "#f39c12",
}

# Sidebar filters
st.sidebar.header("Filtros")
regions = ["All"] + get_regions()
selected_region = st.sidebar.selectbox("Regiao", regions, index=0)

states = ["All"] + get_states()
selected_uf = st.sidebar.selectbox("Estado (UF)", states, index=0)

# =============================================================================
# CLUSTER OVERVIEW
# =============================================================================
st.header("📊 Visao Geral dos Clusters")

try:
    cluster_summary = get_cluster_summary()

    if not cluster_summary.is_empty():
        # KPI Cards - one per cluster
        cols = st.columns(5)

        for i, col in enumerate(cols):
            row = cluster_summary.filter(pl.col("cluster_id") == i)
            if not row.is_empty():
                with col:
                    count = int(row["num_municipios"][0])
                    label = CLUSTER_LABELS.get(i, f"Cluster {i}")
                    color = CLUSTER_COLORS.get(i, "#666")
                    avg_idhm = float(row["avg_idhm"][0])

                    st.markdown(
                        f"""<div style='text-align:center; padding:12px;
                        background-color:{color}20; border-radius:8px;
                        border-left: 4px solid {color};'>
                        <h3 style='margin:0; color:{color};'>{count:,}</h3>
                        <small style='font-size:11px;'>{label}</small><br>
                        <small style='color:#666;'>IDHM: {avg_idhm:.3f}</small>
                        </div>""",
                        unsafe_allow_html=True
                    )

        st.markdown("---")

        # Summary table
        st.subheader("Perfil dos Clusters")
        display_df = cluster_summary.to_pandas()
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "cluster_id": st.column_config.NumberColumn("ID", format="%d"),
                "cluster_label": "Nivel de Desenvolvimento",
                "num_municipios": st.column_config.NumberColumn("Municipios", format="%d"),
                "total_populacao": st.column_config.NumberColumn("Populacao Total", format="%.0f"),
                "avg_idhm": st.column_config.NumberColumn("IDHM Medio", format="%.3f"),
                "min_idhm": st.column_config.NumberColumn("IDHM Min", format="%.3f"),
                "max_idhm": st.column_config.NumberColumn("IDHM Max", format="%.3f"),
                "avg_ivs": st.column_config.NumberColumn("IVS Medio", format="%.3f"),
                "avg_gini": st.column_config.NumberColumn("Gini Medio", format="%.3f"),
                "avg_renda_pc": st.column_config.NumberColumn("Renda PC Media", format="R$ %.0f"),
                "avg_dependency": st.column_config.NumberColumn("Dependencia %", format="%.1f"),
                "avg_efficiency": st.column_config.NumberColumn("Eficiencia", format="%.1f"),
            }
        )
    else:
        st.warning(
            "Dados de clusters nao disponiveis. Execute o pipeline de clustering:\n\n"
            "1. `python -m src.analysis.clustering`\n"
            "2. `cd dbt_project && dbt seed && dbt run --select mart_cluster_analysis`"
        )
except Exception as e:
    st.error(f"Erro ao carregar dados de clusters: {e}")
    st.info(
        "Execute o pipeline de clustering primeiro:\n\n"
        "```bash\n"
        "python -m src.analysis.clustering\n"
        "cd dbt_project && dbt seed && dbt run --select mart_cluster_analysis\n"
        "```"
    )

# =============================================================================
# CLUSTER VISUALIZATION
# =============================================================================
st.header("🗺️ Distribuicao dos Clusters")

tab_pie, tab_bar, tab_scatter = st.tabs([
    "📊 Distribuicao Geral",
    "📍 Por Regiao",
    "🔬 Scatter Plot IDHM vs IVS"
])

with tab_pie:
    try:
        if 'cluster_summary' in dir() and not cluster_summary.is_empty():
            df = cluster_summary.to_pandas()

            # Map cluster_id to color
            df["color"] = df["cluster_id"].map(CLUSTER_COLORS)

            fig = px.pie(
                df,
                values="num_municipios",
                names="cluster_label",
                color="cluster_label",
                color_discrete_sequence=[CLUSTER_COLORS[i] for i in range(5)],
                title="Distribuicao de Municipios por Nivel de Desenvolvimento"
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(showlegend=True, legend=dict(orientation="h", y=-0.1))
            st.plotly_chart(fig, use_container_width=True)

            # Population distribution
            col1, col2 = st.columns(2)
            with col1:
                fig2 = px.pie(
                    df,
                    values="total_populacao",
                    names="cluster_label",
                    color_discrete_sequence=[CLUSTER_COLORS[i] for i in range(5)],
                    title="Distribuicao da Populacao por Cluster"
                )
                fig2.update_traces(textposition="inside", textinfo="percent+label")
                fig2.update_layout(showlegend=False)
                st.plotly_chart(fig2, use_container_width=True)

            with col2:
                # Average IDHM by cluster
                fig3 = px.bar(
                    df,
                    x="cluster_label",
                    y="avg_idhm",
                    color="cluster_id",
                    color_discrete_map={i: CLUSTER_COLORS[i] for i in range(5)},
                    title="IDHM Medio por Cluster",
                    labels={"cluster_label": "Cluster", "avg_idhm": "IDHM Medio"}
                )
                fig3.update_layout(showlegend=False, xaxis_tickangle=-45)
                st.plotly_chart(fig3, use_container_width=True)
    except Exception:
        st.info("Dados nao disponiveis para grafico de pizza.")

with tab_bar:
    try:
        region_dist = get_cluster_distribution_by_region()

        if not region_dist.is_empty():
            df = region_dist.to_pandas()

            fig = px.bar(
                df,
                x="regiao",
                y="count",
                color="cluster_label",
                color_discrete_sequence=[CLUSTER_COLORS[i] for i in range(5)],
                title="Distribuicao de Clusters por Regiao",
                labels={
                    "regiao": "Regiao",
                    "count": "Numero de Municipios",
                    "cluster_label": "Nivel"
                },
                barmode="stack"
            )
            fig.update_layout(legend=dict(orientation="h", y=-0.2))
            st.plotly_chart(fig, use_container_width=True)

            # Percentage breakdown table
            st.markdown("**Composicao Percentual por Regiao:**")
            pivot_df = df.pivot_table(
                index="regiao",
                columns="cluster_label",
                values="count",
                fill_value=0
            )
            pivot_pct = pivot_df.div(pivot_df.sum(axis=1), axis=0) * 100
            st.dataframe(
                pivot_pct.round(1).style.format("{:.1f}%"),
                use_container_width=True
            )
    except Exception:
        st.info("Dados de distribuicao regional nao disponiveis.")

with tab_scatter:
    try:
        scatter_data = get_cluster_scatter_data(
            region=selected_region,
            uf=selected_uf
        )

        if not scatter_data.is_empty():
            df = scatter_data.to_pandas()

            fig = px.scatter(
                df,
                x="idhm_2010",
                y="ivs_2010",
                color="cluster_label",
                color_discrete_sequence=[CLUSTER_COLORS[i] for i in range(5)],
                size="populacao",
                size_max=40,
                hover_name="nome_municipio",
                hover_data={
                    "sigla_uf": True,
                    "idhm_2010": ":.3f",
                    "ivs_2010": ":.3f",
                    "gini_2010": ":.3f",
                    "populacao": ":,.0f",
                    "cluster_label": False
                },
                title="Municipios: IDHM vs IVS (coloridos por cluster)",
                labels={
                    "idhm_2010": "IDHM (2010)",
                    "ivs_2010": "IVS (2010) - Vulnerabilidade",
                    "cluster_label": "Nivel"
                }
            )
            # Invert Y axis so lower vulnerability is at top
            fig.update_yaxes(autorange="reversed")
            fig.update_layout(
                legend=dict(orientation="h", y=-0.15),
                height=600
            )
            st.plotly_chart(fig, use_container_width=True)

            st.caption("*Tamanho dos pontos proporcional a populacao. IVS invertido (menor = melhor).*")
        else:
            st.info("Nenhum dado encontrado para os filtros selecionados.")
    except Exception as e:
        st.info(f"Dados nao disponiveis para scatter plot: {e}")

# =============================================================================
# CLUSTER EXPLORER
# =============================================================================
st.header("🔍 Explorar Municipios por Cluster")

selected_cluster = st.selectbox(
    "Selecione um Nivel de Desenvolvimento",
    options=list(CLUSTER_LABELS.keys()),
    format_func=lambda x: f"{x} - {CLUSTER_LABELS[x]}"
)

try:
    cluster_data = get_cluster_municipalities(
        cluster_id=selected_cluster,
        region=selected_region,
        uf=selected_uf,
        limit=200
    )

    if not cluster_data.is_empty():
        count = len(cluster_data)
        st.caption(
            f"Mostrando **{count}** municipios do cluster: "
            f"**{CLUSTER_LABELS[selected_cluster]}**"
        )
        st.caption("*Clique em uma linha para ver o perfil do municipio*")

        df = cluster_data.to_pandas()

        render_clickable_ranking_table(
            df=df,
            display_columns=[
                "nome_municipio", "sigla_uf", "regiao", "populacao",
                "idhm_2010", "ivs_2010", "dependency_ratio", "efficiency_index",
                "status_transicao"
            ],
            column_config={
                "nome_municipio": "Municipio",
                "sigla_uf": "UF",
                "regiao": "Regiao",
                "populacao": st.column_config.NumberColumn("Populacao", format="%d"),
                "idhm_2010": st.column_config.NumberColumn("IDHM", format="%.3f"),
                "ivs_2010": st.column_config.NumberColumn("IVS", format="%.3f"),
                "dependency_ratio": st.column_config.NumberColumn("Dependencia %", format="%.1f"),
                "efficiency_index": st.column_config.NumberColumn("Eficiencia", format="%.1f"),
                "status_transicao": "Status Transicao",
            },
            id_column="id_municipio_ibge",
            key=f"cluster_{selected_cluster}_table",
            height=400
        )
    else:
        st.info("Nenhum municipio encontrado para os filtros selecionados.")
except Exception as e:
    st.warning(f"Erro ao carregar dados: {e}")

# =============================================================================
# TRANSITION ANALYSIS
# =============================================================================
st.header("📈 Analise de Transicao")

st.markdown("""
Municipios proximos aos limites dos clusters que poderiam mudar de categoria
com melhorias ou declinio em seus indicadores.
""")

try:
    transitions = get_cluster_transitions_potential()

    if not transitions.is_empty():
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("🚀 Potencial Promocao")
            st.caption("Municipios com IDHM acima da media do cluster")

            promo = transitions.filter(
                pl.col("status_transicao") == "Potencial Promocao"
            ).head(20).to_pandas()

            if len(promo) > 0:
                st.dataframe(
                    promo[["nome_municipio", "sigla_uf", "cluster_label",
                           "idhm_2010", "idhm_vs_cluster"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "nome_municipio": "Municipio",
                        "sigla_uf": "UF",
                        "cluster_label": "Cluster Atual",
                        "idhm_2010": st.column_config.NumberColumn("IDHM", format="%.3f"),
                        "idhm_vs_cluster": st.column_config.NumberColumn(
                            "Desvio", format="+%.3f"
                        ),
                    }
                )
            else:
                st.info("Nenhum municipio identificado.")

        with col2:
            st.subheader("⚠️ Risco Rebaixamento")
            st.caption("Municipios com IDHM abaixo da media do cluster")

            risk = transitions.filter(
                pl.col("status_transicao") == "Risco Rebaixamento"
            ).head(20).to_pandas()

            if len(risk) > 0:
                st.dataframe(
                    risk[["nome_municipio", "sigla_uf", "cluster_label",
                          "idhm_2010", "idhm_vs_cluster"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "nome_municipio": "Municipio",
                        "sigla_uf": "UF",
                        "cluster_label": "Cluster Atual",
                        "idhm_2010": st.column_config.NumberColumn("IDHM", format="%.3f"),
                        "idhm_vs_cluster": st.column_config.NumberColumn(
                            "Desvio", format="%.3f"
                        ),
                    }
                )
            else:
                st.info("Nenhum municipio identificado.")
    else:
        st.info("Dados de transicao nao disponiveis.")
except Exception:
    st.info("Dados de transicao nao disponiveis.")

# =============================================================================
# STATE DISTRIBUTION (Expandable)
# =============================================================================
with st.expander("📍 Distribuicao por Estado"):
    try:
        state_dist = get_cluster_distribution_by_state()

        if not state_dist.is_empty():
            df = state_dist.to_pandas()

            # Filter by selected region if applicable
            if selected_region != "All":
                df = df[df["regiao"] == selected_region]

            if len(df) > 0:
                fig = px.bar(
                    df,
                    x="sigla_uf",
                    y="count",
                    color="cluster_label",
                    color_discrete_sequence=[CLUSTER_COLORS[i] for i in range(5)],
                    title="Distribuicao de Clusters por Estado",
                    labels={
                        "sigla_uf": "Estado",
                        "count": "Municipios",
                        "cluster_label": "Nivel"
                    },
                    barmode="stack"
                )
                fig.update_layout(
                    xaxis_tickangle=-45,
                    legend=dict(orientation="h", y=-0.25),
                    height=500
                )
                st.plotly_chart(fig, use_container_width=True)
    except Exception:
        st.info("Dados por estado nao disponiveis.")

# =============================================================================
# METHODOLOGY
# =============================================================================
with st.expander("📚 Metodologia de Clustering"):
    st.markdown("""
    ### Algoritmo: K-Means Clustering

    Os municipios foram agrupados usando o algoritmo K-Means com **5 clusters**,
    baseado nos seguintes indicadores (normalizados):

    **Variaveis de Entrada:**
    - IDHM 2010 (Indice de Desenvolvimento Humano Municipal)
    - IDHM Educacao
    - IDHM Renda
    - IVS 2010 invertido (1 - IVS, para que maior = melhor)
    - Gini 2010 invertido (1 - Gini, para que maior = melhor)
    - Renda per Capita (log-transformada)

    **Pre-processamento:**
    1. Valores nulos substituidos pela mediana
    2. Padronizacao Z-score (media=0, desvio=1)

    **Ordenacao dos Clusters:**
    - Clusters ordenados do mais ao menos desenvolvido
    - Baseado na media do IDHM de cada grupo

    ### Niveis de Desenvolvimento

    | Cluster | Label | Descricao |
    |---------|-------|-----------|
    | 0 | Polos de Desenvolvimento | Metropoles e cidades de referencia regional |
    | 1 | Desenvolvimento Avancado | Alto IDHM, economia diversificada |
    | 2 | Em Desenvolvimento | IDHM medio, potencial de crescimento |
    | 3 | Vulneraveis | Baixo IDHM, alta vulnerabilidade social |
    | 4 | Criticos | IDHM muito baixo, situacao critica |

    ### Limitacoes

    - Indicadores baseados no Censo 2010 (dados mais recentes disponiveis)
    - Clustering e estatico; nao captura mudancas recentes
    - K=5 definido a priori por interpretabilidade (validado com silhouette score)

    ### Como Reprocessar

    Para atualizar os clusters com novos dados:

    ```bash
    # 1. Executar clustering
    python -m src.analysis.clustering

    # 2. Carregar no dbt
    cd dbt_project
    dbt seed
    dbt run --select mart_cluster_analysis
    ```
    """)

# Footer
st.markdown("---")
st.caption(
    "Dados: IBGE, Atlas Brasil (PNUD/IPEA) | "
    "Clustering: scikit-learn K-Means | "
    "Visualizacao: Plotly + Streamlit"
)
