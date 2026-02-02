"""
Fiscal Dependency and Efficiency Analysis Page.

Analyzes municipal fiscal dependency on federal transfers and
efficiency in converting spending into social outcomes.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from data.queries import (
    get_dependency_rankings,
    get_efficiency_rankings,
    get_dependency_vs_efficiency,
    get_fiscal_comparison_by_region,
    get_transfer_breakdown,
    get_regions,
)

# Page config
st.set_page_config(
    page_title="Analise Fiscal | Municipios",
    page_icon="💰",
    layout="wide",
)

st.title("💰 Analise Fiscal Municipal")
st.markdown("""
Analise de **dependencia fiscal** (quanto cada municipio depende de transferencias federais)
e **eficiencia municipal** (quao bem os municipios convertem gastos em resultados sociais).
""")

# Region colors for consistency
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

year_options = list(range(2023, 2012, -1))
selected_year = st.sidebar.selectbox("Ano", year_options, index=0)

# =============================================================================
# KEY METRICS
# =============================================================================
st.header("📊 Visao Geral")

try:
    regional_data = get_fiscal_comparison_by_region(year=selected_year)

    if not regional_data.is_empty():
        # Summary metrics across all regions
        col1, col2, col3, col4 = st.columns(4)

        total_municipios = regional_data["num_municipios"].sum()
        avg_dependency = regional_data["avg_dependency"].mean()
        avg_efficiency = regional_data["avg_efficiency"].mean()
        avg_own_revenue = regional_data["avg_own_revenue"].mean()

        with col1:
            st.metric("Municipios Analisados", f"{total_municipios:,.0f}")
        with col2:
            st.metric(
                "Dependencia Media",
                f"{avg_dependency:.1f}%",
                help="% da receita vinda de transferencias federais"
            )
        with col3:
            st.metric(
                "Eficiencia Media",
                f"{avg_efficiency:.1f}",
                help="Indice de eficiencia (0-100)"
            )
        with col4:
            st.metric(
                "Receita Propria Media",
                f"{avg_own_revenue:.1f}%",
                help="% da receita gerada pelo proprio municipio"
            )
except Exception as e:
    st.warning("Dados fiscais nao disponiveis. Execute `dbt build` para gerar os modelos.")

# =============================================================================
# REGIONAL COMPARISON
# =============================================================================
st.header("🗺️ Comparacao Regional")

tab1, tab2 = st.tabs(["Dependencia por Regiao", "Eficiencia por Regiao"])

with tab1:
    try:
        if not regional_data.is_empty():
            df = regional_data.to_pandas()

            fig = px.bar(
                df,
                x="regiao",
                y="avg_dependency",
                color="regiao",
                color_discrete_map=REGION_COLORS,
                title=f"Dependencia Fiscal Media por Regiao ({selected_year})",
                labels={
                    "regiao": "Regiao",
                    "avg_dependency": "Dependencia (%)"
                }
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

            # Show range
            st.markdown("**Faixas de Dependencia por Regiao:**")
            cols = st.columns(len(df))
            for i, row in df.iterrows():
                with cols[i]:
                    st.markdown(f"**{row['regiao']}**")
                    st.caption(f"Min: {row['min_dependency']:.1f}% | Max: {row['max_dependency']:.1f}%")
    except Exception:
        st.info("Dados de dependencia regional nao disponiveis.")

with tab2:
    try:
        if not regional_data.is_empty():
            df = regional_data.to_pandas()

            fig = px.bar(
                df,
                x="regiao",
                y="avg_efficiency",
                color="regiao",
                color_discrete_map=REGION_COLORS,
                title=f"Eficiencia Media por Regiao ({selected_year})",
                labels={
                    "regiao": "Regiao",
                    "avg_efficiency": "Indice de Eficiencia"
                }
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

            # Show range
            st.markdown("**Faixas de Eficiencia por Regiao:**")
            cols = st.columns(len(df))
            for i, row in df.iterrows():
                with cols[i]:
                    st.markdown(f"**{row['regiao']}**")
                    st.caption(f"Min: {row['min_efficiency']:.1f} | Max: {row['max_efficiency']:.1f}")
    except Exception:
        st.info("Dados de eficiencia regional nao disponiveis.")

# =============================================================================
# DEPENDENCY VS EFFICIENCY SCATTER
# =============================================================================
st.header("📈 Dependencia vs Eficiencia")

st.markdown("""
Este grafico mostra a relacao entre **dependencia fiscal** (eixo X) e **eficiencia municipal** (eixo Y).
Municipios no quadrante superior esquerdo sao os mais desejados: baixa dependencia e alta eficiencia.
""")

try:
    scatter_data = get_dependency_vs_efficiency(year=selected_year, region=selected_region)

    if not scatter_data.is_empty():
        df = scatter_data.to_pandas()

        fig = px.scatter(
            df,
            x="dependency_ratio",
            y="efficiency_index",
            color="regiao",
            color_discrete_map=REGION_COLORS,
            size="populacao",
            size_max=30,
            hover_name="nome_municipio",
            hover_data={
                "sigla_uf": True,
                "dependency_ratio": ":.1f",
                "efficiency_index": ":.1f",
                "populacao": ":,.0f",
                "idhm": ":.3f"
            },
            title=f"Dependencia Fiscal vs Eficiencia Municipal ({selected_year})",
            labels={
                "dependency_ratio": "Dependencia Fiscal (%)",
                "efficiency_index": "Indice de Eficiencia",
                "regiao": "Regiao",
                "populacao": "Populacao"
            }
        )

        # Add quadrant lines
        fig.add_hline(y=50, line_dash="dash", line_color="gray", opacity=0.5)
        fig.add_vline(x=50, line_dash="dash", line_color="gray", opacity=0.5)

        # Add quadrant labels
        fig.add_annotation(x=25, y=80, text="Alta Eficiencia<br>Baixa Dependencia", showarrow=False, font=dict(size=10, color="green"))
        fig.add_annotation(x=75, y=80, text="Alta Eficiencia<br>Alta Dependencia", showarrow=False, font=dict(size=10, color="orange"))
        fig.add_annotation(x=25, y=20, text="Baixa Eficiencia<br>Baixa Dependencia", showarrow=False, font=dict(size=10, color="orange"))
        fig.add_annotation(x=75, y=20, text="Baixa Eficiencia<br>Alta Dependencia", showarrow=False, font=dict(size=10, color="red"))

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Nenhum dado disponivel para o filtro selecionado.")
except Exception as e:
    st.warning(f"Erro ao carregar dados: {e}")

# =============================================================================
# RANKINGS
# =============================================================================
st.header("🏆 Rankings")

tab_dep, tab_eff = st.tabs(["Ranking de Dependencia", "Ranking de Eficiencia"])

with tab_dep:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🟢 Mais Autonomos")
        try:
            least_dependent = get_dependency_rankings(
                year=selected_year,
                limit=20,
                ascending=True,
                region=selected_region
            )

            if not least_dependent.is_empty():
                df = least_dependent.to_pandas()
                st.dataframe(
                    df[["ranking", "nome_municipio", "sigla_uf", "dependency_ratio", "categoria_dependencia"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "ranking": "Pos",
                        "nome_municipio": "Municipio",
                        "sigla_uf": "UF",
                        "dependency_ratio": st.column_config.NumberColumn("Dependencia %", format="%.1f"),
                        "categoria_dependencia": "Categoria"
                    }
                )
        except Exception:
            st.info("Dados nao disponiveis.")

    with col2:
        st.subheader("🔴 Mais Dependentes")
        try:
            most_dependent = get_dependency_rankings(
                year=selected_year,
                limit=20,
                ascending=False,
                region=selected_region
            )

            if not most_dependent.is_empty():
                df = most_dependent.to_pandas()
                st.dataframe(
                    df[["ranking", "nome_municipio", "sigla_uf", "dependency_ratio", "categoria_dependencia"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "ranking": "Pos",
                        "nome_municipio": "Municipio",
                        "sigla_uf": "UF",
                        "dependency_ratio": st.column_config.NumberColumn("Dependencia %", format="%.1f"),
                        "categoria_dependencia": "Categoria"
                    }
                )
        except Exception:
            st.info("Dados nao disponiveis.")

with tab_eff:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🏆 Mais Eficientes")
        try:
            most_efficient = get_efficiency_rankings(
                year=selected_year,
                limit=20,
                region=selected_region
            )

            if not most_efficient.is_empty():
                df = most_efficient.to_pandas()
                st.dataframe(
                    df[["ranking", "nome_municipio", "sigla_uf", "efficiency_index", "social_outcome_score", "categoria_eficiencia"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "ranking": "Pos",
                        "nome_municipio": "Municipio",
                        "sigla_uf": "UF",
                        "efficiency_index": st.column_config.NumberColumn("Eficiencia", format="%.1f"),
                        "social_outcome_score": st.column_config.NumberColumn("Score Social", format="%.1f"),
                        "categoria_eficiencia": "Categoria"
                    }
                )
        except Exception:
            st.info("Dados nao disponiveis.")

    with col2:
        st.subheader("📉 Menos Eficientes")
        try:
            # Get all efficiency data and sort ascending
            all_efficiency = get_efficiency_rankings(
                year=selected_year,
                limit=1000,
                region=selected_region
            )

            if not all_efficiency.is_empty():
                df = all_efficiency.to_pandas().sort_values("efficiency_index", ascending=True).head(20)
                df["ranking"] = range(1, len(df) + 1)
                st.dataframe(
                    df[["ranking", "nome_municipio", "sigla_uf", "efficiency_index", "social_outcome_score", "categoria_eficiencia"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "ranking": "Pos",
                        "nome_municipio": "Municipio",
                        "sigla_uf": "UF",
                        "efficiency_index": st.column_config.NumberColumn("Eficiencia", format="%.1f"),
                        "social_outcome_score": st.column_config.NumberColumn("Score Social", format="%.1f"),
                        "categoria_eficiencia": "Categoria"
                    }
                )
        except Exception:
            st.info("Dados nao disponiveis.")

# =============================================================================
# TRANSFER BREAKDOWN
# =============================================================================
st.header("💵 Composicao das Transferencias Federais")

try:
    transfer_data = get_transfer_breakdown(year=selected_year, region=selected_region)

    if not transfer_data.is_empty():
        df = transfer_data.to_pandas()

        # Stacked bar chart of transfer composition
        fig = go.Figure()

        for transfer_type, label in [
            ("avg_fpm_share", "FPM"),
            ("avg_fundeb_share", "FUNDEB"),
            ("avg_sus_share", "SUS")
        ]:
            fig.add_trace(go.Bar(
                name=label,
                x=df["regiao"],
                y=df[transfer_type],
                text=df[transfer_type].apply(lambda x: f"{x:.1f}%"),
                textposition="inside"
            ))

        fig.update_layout(
            barmode="stack",
            title=f"Composicao Media das Transferencias Federais por Regiao ({selected_year})",
            xaxis_title="Regiao",
            yaxis_title="Participacao (%)",
            legend_title="Tipo de Transferencia"
        )

        st.plotly_chart(fig, use_container_width=True)

        # Summary table
        st.markdown("**Resumo por Regiao:**")
        st.dataframe(
            df[["regiao", "num_municipios", "avg_transfers_per_capita", "avg_fpm_share", "avg_fundeb_share", "avg_sus_share"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "regiao": "Regiao",
                "num_municipios": "Municipios",
                "avg_transfers_per_capita": st.column_config.NumberColumn("Transf. per Capita (R$)", format="R$ %.2f"),
                "avg_fpm_share": st.column_config.NumberColumn("FPM %", format="%.1f"),
                "avg_fundeb_share": st.column_config.NumberColumn("FUNDEB %", format="%.1f"),
                "avg_sus_share": st.column_config.NumberColumn("SUS %", format="%.1f")
            }
        )
except Exception as e:
    st.info("Dados de composicao de transferencias nao disponiveis.")

# =============================================================================
# METHODOLOGY
# =============================================================================
with st.expander("📚 Metodologia"):
    st.markdown("""
    ### Indice de Dependencia Fiscal

    **Definicao:** Percentual da receita municipal proveniente de transferencias federais.

    ```
    Dependencia = (Transferencias Federais / Receita Total) × 100
    ```

    **Categorias:**
    - Autonomo: < 20%
    - Baixa Dependencia: 20-40%
    - Moderadamente Dependente: 40-60%
    - Altamente Dependente: 60-80%
    - Extremamente Dependente: > 80%

    ---

    ### Indice de Eficiencia Municipal

    **Definicao:** Mede quao bem o municipio converte gastos publicos em resultados sociais.

    **Formula:**
    1. Normaliza indicadores por ano (min-max)
    2. `Score Social = 0.4 × IDHM + 0.3 × IVS(invertido) + 0.3 × Gini(invertido)`
    3. `Eficiencia = Score Social / (Gasto Normalizado + 0.1)`
    4. Aplica modificador de equilibrio fiscal (±10 pontos)
    5. Reescala para 0-100

    **Categorias:**
    - Alta Eficiencia: ≥ 70
    - Eficiencia Moderada: 50-70
    - Baixa Eficiencia: 30-50
    - Ineficiente: < 30

    **Limitacao:** IDHM/IVS/Gini baseados no Censo 2010 (valores mais recentes nao disponiveis).
    """)

# Footer
st.markdown("---")
st.caption("Dados: SICONFI (Tesouro Nacional), IBGE, Atlas Brasil")
