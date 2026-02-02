"""
Political Economy Analysis Dashboard Page.

Analyzes relationships between political leadership (party ideology,
continuity, transitions) and municipal fiscal/social outcomes.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import polars as pl
import numpy as np

from data.queries import (
    get_ideology_distribution_by_year,
    get_fiscal_by_ideology,
    get_party_ranking,
    get_political_stability_by_region,
    get_ideology_transitions,
    get_political_correlations_data,
    get_fiscal_trend_by_ideology,
    get_diff_in_diff_summary,
    get_electoral_cycle_effects,
    get_regions,
)

# Page config
st.set_page_config(
    page_title="Political Economy | Municipios Analytics",
    page_icon="🗳️",
    layout="wide",
)

# Color schemes
IDEOLOGY_COLORS = {
    "esquerda": "#e74c3c",      # Red for left
    "centro": "#f1c40f",        # Yellow for center
    "direita": "#3498db",       # Blue for right
}

REGION_COLORS = {
    "Norte": "#27ae60",
    "Nordeste": "#e74c3c",
    "Sudeste": "#3498db",
    "Sul": "#9b59b6",
    "Centro-Oeste": "#f39c12",
}

TRANSITION_COLORS = {
    "esquerda_para_direita": "#e74c3c",
    "centro_para_direita": "#e67e22",
    "direita_para_esquerda": "#3498db",
    "centro_para_esquerda": "#2980b9",
    "sem_mudanca_significativa": "#95a5a6",
}


def format_currency(value: float) -> str:
    """Format value as Brazilian currency."""
    if value is None:
        return "N/A"
    if abs(value) >= 1_000_000_000:
        return f"R$ {value/1_000_000_000:.1f}B"
    if abs(value) >= 1_000_000:
        return f"R$ {value/1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"R$ {value/1_000:.1f}k"
    return f"R$ {value:.0f}"


def format_number(value: float) -> str:
    """Format large numbers with abbreviations."""
    if value is None:
        return "N/A"
    if value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value/1_000:.1f}k"
    return f"{value:.0f}"


# =============================================================================
# HEADER
# =============================================================================

st.title("🗳️ Political Economy Analysis")
st.markdown("""
Analyzing relationships between **political leadership** and **municipal outcomes**.
Explore how party ideology, political continuity, and electoral cycles affect
fiscal performance across Brazil's 5,570 municipalities.
""")

st.divider()

# =============================================================================
# KEY METRICS
# =============================================================================

st.subheader("📊 Key Political-Economy Indicators")

try:
    ideology_dist = get_ideology_distribution_by_year()
    fiscal_by_ideology = get_fiscal_by_ideology()
    stability_by_region = get_political_stability_by_region()

    if not ideology_dist.is_empty() and not fiscal_by_ideology.is_empty():
        # Get latest year stats
        latest_year = ideology_dist["ano_eleicao"].max()
        latest_dist = ideology_dist.filter(pl.col("ano_eleicao") == latest_year)

        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            total_mandates = fiscal_by_ideology["num_mandatos"].sum()
            st.metric(
                label="Total Mandates Analyzed",
                value=format_number(total_mandates),
                help="Mayoral terms from 2000-2024 with fiscal data"
            )

        with col2:
            # Get left percentage for latest year
            left_pct = latest_dist.filter(pl.col("bloco_ideologico") == "esquerda")["percentage"].to_list()
            left_pct = left_pct[0] if left_pct else 0
            st.metric(
                label=f"Left-Bloc ({latest_year})",
                value=f"{left_pct:.1f}%",
                delta=None,
                help="Municipalities governed by left-leaning parties"
            )

        with col3:
            center_pct = latest_dist.filter(pl.col("bloco_ideologico") == "centro")["percentage"].to_list()
            center_pct = center_pct[0] if center_pct else 0
            st.metric(
                label=f"Center-Bloc ({latest_year})",
                value=f"{center_pct:.1f}%",
                help="Municipalities governed by centrist parties"
            )

        with col4:
            right_pct = latest_dist.filter(pl.col("bloco_ideologico") == "direita")["percentage"].to_list()
            right_pct = right_pct[0] if right_pct else 0
            st.metric(
                label=f"Right-Bloc ({latest_year})",
                value=f"{right_pct:.1f}%",
                help="Municipalities governed by right-leaning parties"
            )

        with col5:
            if not stability_by_region.is_empty():
                avg_continuity = stability_by_region["taxa_continuidade"].mean()
                st.metric(
                    label="Avg Party Continuity",
                    value=f"{avg_continuity:.1f}%",
                    help="Rate of same-party reelection across municipalities"
                )
            else:
                st.metric(label="Avg Party Continuity", value="N/A")

except Exception as e:
    st.error(f"Error loading key metrics: {e}")

st.divider()

# =============================================================================
# IDEOLOGY DISTRIBUTION OVER TIME
# =============================================================================

st.subheader("📈 Ideology Distribution Over Time")

st.markdown("""
How has the ideological composition of Brazilian mayors changed since 2000?
This chart shows the percentage of municipalities governed by each ideological bloc.
""")

try:
    ideology_dist = get_ideology_distribution_by_year()

    if not ideology_dist.is_empty():
        # Pivot for area chart
        df_pivot = ideology_dist.pivot(
            values="percentage",
            index="ano_eleicao",
            columns="bloco_ideologico"
        ).sort("ano_eleicao")

        col1, col2 = st.columns([2, 1])

        with col1:
            # Create stacked area chart
            fig = go.Figure()

            for bloc in ["esquerda", "centro", "direita"]:
                if bloc in df_pivot.columns:
                    fig.add_trace(go.Scatter(
                        x=df_pivot["ano_eleicao"].to_list(),
                        y=df_pivot[bloc].to_list(),
                        mode='lines+markers',
                        name=bloc.capitalize(),
                        line=dict(width=3, color=IDEOLOGY_COLORS.get(bloc)),
                        marker=dict(size=10),
                        fill='tonexty' if bloc != "esquerda" else None,
                    ))

            fig.update_layout(
                title="Ideology Distribution by Election Year",
                xaxis_title="Election Year",
                yaxis_title="Percentage of Municipalities (%)",
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
                height=400,
            )

            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Show data table
            st.markdown("**Detailed Distribution**")
            display_df = ideology_dist.to_pandas()
            display_df = display_df.pivot(
                index="ano_eleicao",
                columns="bloco_ideologico",
                values="percentage"
            ).round(1)
            display_df.columns = [c.capitalize() for c in display_df.columns]
            st.dataframe(display_df, use_container_width=True)

            # Key insight
            st.info("""
            **Key Insight:** Since 2012, there has been a significant
            rightward shift in Brazilian municipal politics. Left-bloc
            municipalities declined from 38% to ~15%.
            """)

except Exception as e:
    st.error(f"Error loading ideology distribution: {e}")

st.divider()

# =============================================================================
# FISCAL PERFORMANCE BY IDEOLOGY
# =============================================================================

st.subheader("💰 Fiscal Performance by Ideology Bloc")

st.markdown("""
Do mayors from different ideological blocs manage public finances differently?
Compare average fiscal metrics across left, center, and right-leaning administrations.
""")

try:
    # Region filter
    regions = ["All"] + get_regions()
    selected_region = st.selectbox(
        "Filter by Region:",
        regions,
        key="fiscal_region_filter"
    )

    fiscal_data = get_fiscal_by_ideology(selected_region)

    if not fiscal_data.is_empty():
        col1, col2 = st.columns(2)

        with col1:
            # Fiscal balance by ideology
            fig1 = px.bar(
                fiscal_data.to_pandas(),
                x="bloco_ideologico",
                y="avg_saldo_fiscal",
                color="bloco_ideologico",
                color_discrete_map=IDEOLOGY_COLORS,
                title="Average Fiscal Balance by Ideology",
                labels={
                    "bloco_ideologico": "Ideology Bloc",
                    "avg_saldo_fiscal": "Avg Fiscal Balance (R$)"
                }
            )
            fig1.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            # Per capita spending by ideology
            fig2 = px.bar(
                fiscal_data.to_pandas(),
                x="bloco_ideologico",
                y="avg_despesa_per_capita",
                color="bloco_ideologico",
                color_discrete_map=IDEOLOGY_COLORS,
                title="Average Per Capita Spending by Ideology",
                labels={
                    "bloco_ideologico": "Ideology Bloc",
                    "avg_despesa_per_capita": "Avg Spending per Capita (R$)"
                }
            )
            fig2.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig2, use_container_width=True)

        # Summary table
        st.markdown("**Detailed Fiscal Metrics by Ideology**")
        summary_df = fiscal_data.select([
            pl.col("bloco_ideologico").alias("Ideology"),
            pl.col("num_mandatos").alias("Mandates"),
            pl.col("avg_saldo_fiscal").round(0).alias("Avg Fiscal Balance (R$)"),
            pl.col("avg_despesa_per_capita").round(0).alias("Avg Spending/Capita (R$)"),
            pl.col("avg_receita_per_capita").round(0).alias("Avg Revenue/Capita (R$)"),
            pl.col("avg_taxa_execucao").round(1).alias("Budget Execution (%)"),
            pl.col("taxa_reeleicao").round(1).alias("Reelection Rate (%)")
        ])
        st.dataframe(summary_df.to_pandas(), use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error loading fiscal data: {e}")

st.divider()

# =============================================================================
# PARTY RANKINGS
# =============================================================================

st.subheader("🏆 Party Rankings")

st.markdown("""
Which parties have won the most mayoral elections since 2000?
Explore the electoral success and performance of Brazil's major political parties.
""")

try:
    party_data = get_party_ranking()

    if not party_data.is_empty():
        # Top 15 parties by mandates
        top_parties = party_data.head(15)

        col1, col2 = st.columns([1.5, 1])

        with col1:
            fig = px.bar(
                top_parties.to_pandas(),
                y="sigla_partido",
                x="num_mandatos",
                color="bloco_ideologico",
                color_discrete_map=IDEOLOGY_COLORS,
                orientation="h",
                title="Top 15 Parties by Mandates Won (2000-2024)",
                labels={
                    "sigla_partido": "Party",
                    "num_mandatos": "Number of Mandates",
                    "bloco_ideologico": "Ideology"
                }
            )
            fig.update_layout(
                height=500,
                yaxis=dict(categoryorder="total ascending"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02)
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Party Statistics**")

            # Filter options
            ideology_filter = st.multiselect(
                "Filter by Ideology:",
                ["esquerda", "centro", "direita"],
                default=["esquerda", "centro", "direita"],
                key="party_ideology_filter"
            )

            filtered_parties = party_data.filter(
                pl.col("bloco_ideologico").is_in(ideology_filter)
            )

            display_df = filtered_parties.select([
                pl.col("sigla_partido").alias("Party"),
                pl.col("bloco_ideologico").alias("Ideology"),
                pl.col("num_mandatos").alias("Mandates"),
                pl.col("avg_votacao").round(1).alias("Avg Vote %"),
                pl.col("taxa_reeleicao").round(1).alias("Reelection %")
            ]).head(20)

            st.dataframe(display_df.to_pandas(), use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error loading party data: {e}")

st.divider()

# =============================================================================
# REGIONAL POLITICAL STABILITY
# =============================================================================

st.subheader("🗺️ Regional Political Stability")

st.markdown("""
How stable is political leadership across Brazil's regions?
Compare party continuity rates and ideological volatility by region.
""")

try:
    stability_data = get_political_stability_by_region()

    if not stability_data.is_empty():
        col1, col2 = st.columns(2)

        with col1:
            # Continuity rate by region
            fig1 = px.bar(
                stability_data.to_pandas(),
                x="regiao",
                y="taxa_continuidade",
                color="regiao",
                color_discrete_map=REGION_COLORS,
                title="Party Continuity Rate by Region",
                labels={
                    "regiao": "Region",
                    "taxa_continuidade": "Continuity Rate (%)"
                }
            )
            fig1.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            # Ideological volatility by region
            fig2 = px.bar(
                stability_data.to_pandas(),
                x="regiao",
                y="volatilidade_ideologica",
                color="regiao",
                color_discrete_map=REGION_COLORS,
                title="Ideological Volatility by Region",
                labels={
                    "regiao": "Region",
                    "volatilidade_ideologica": "Avg Ideological Shift"
                }
            )
            fig2.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig2, use_container_width=True)

        # Historical profile distribution
        st.markdown("**Historical Political Profile by Region**")

        profile_data = stability_data.select([
            pl.col("regiao").alias("Region"),
            pl.col("num_municipios").alias("Municipalities"),
            pl.col("municipios_esquerda").alias("Historically Left"),
            pl.col("municipios_direita").alias("Historically Right"),
            pl.col("municipios_misto").alias("Politically Mixed"),
            pl.col("taxa_continuidade").round(1).alias("Continuity %"),
            pl.col("volatilidade_ideologica").round(2).alias("Volatility")
        ])
        st.dataframe(profile_data.to_pandas(), use_container_width=True, hide_index=True)

        st.info("""
        **Key Finding:** Southern municipalities show the highest political stability
        (33% continuity rate), while Northern municipalities show the lowest (20%),
        suggesting more volatile electoral behavior in the Amazon region.
        """)

except Exception as e:
    st.error(f"Error loading stability data: {e}")

st.divider()

# =============================================================================
# IDEOLOGY TRANSITIONS (SANKEY DIAGRAM)
# =============================================================================

st.subheader("🔄 Ideology Transitions")

st.markdown("""
How do municipalities transition between ideological blocs across elections?
This diagram shows the flow from previous mandate's ideology to current mandate's ideology.
""")

try:
    transitions = get_ideology_transitions()

    if not transitions.is_empty():
        # Prepare Sankey data
        sources = transitions["source"].to_list()
        targets = transitions["target"].to_list()
        values = transitions["count"].to_list()

        # Create node labels
        labels = ["Esquerda (Anterior)", "Centro (Anterior)", "Direita (Anterior)",
                  "Esquerda (Atual)", "Centro (Atual)", "Direita (Atual)"]

        # Map sources/targets to indices
        source_map = {"esquerda": 0, "centro": 1, "direita": 2}
        target_map = {"esquerda": 3, "centro": 4, "direita": 5}

        source_indices = [source_map.get(s, 1) for s in sources]
        target_indices = [target_map.get(t, 4) for t in targets]

        # Colors
        node_colors = [
            IDEOLOGY_COLORS["esquerda"], IDEOLOGY_COLORS["centro"], IDEOLOGY_COLORS["direita"],
            IDEOLOGY_COLORS["esquerda"], IDEOLOGY_COLORS["centro"], IDEOLOGY_COLORS["direita"]
        ]

        link_colors = []
        for s, t in zip(sources, targets):
            if s == t:
                link_colors.append("rgba(150, 150, 150, 0.4)")  # Gray for no change
            else:
                link_colors.append("rgba(231, 76, 60, 0.4)" if t == "direita" else "rgba(52, 152, 219, 0.4)")

        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=labels,
                color=node_colors
            ),
            link=dict(
                source=source_indices,
                target=target_indices,
                value=values,
                color=link_colors
            )
        )])

        fig.update_layout(
            title="Ideology Transitions Between Mandates (2004-2024)",
            height=450,
            font_size=12
        )

        st.plotly_chart(fig, use_container_width=True)

        # Transition matrix
        col1, col2 = st.columns([1, 1])

        with col1:
            st.markdown("**Transition Counts**")
            matrix = transitions.pivot(
                values="count",
                index="source",
                columns="target"
            )
            if not matrix.is_empty():
                st.dataframe(matrix.to_pandas(), use_container_width=True)

        with col2:
            st.markdown("**Interpretation**")
            st.write("""
            - **Diagonal flows** = Political stability (same bloc wins again)
            - **Off-diagonal flows** = Ideological transitions
            - Thicker flows indicate more common transitions
            """)

except Exception as e:
    st.error(f"Error loading transitions: {e}")

st.divider()

# =============================================================================
# ELECTORAL CYCLE EFFECTS
# =============================================================================

st.subheader("📆 Electoral Cycle Effects")

st.markdown("""
Do mayors spend more in election years? Explore fiscal behavior across the 4-year mandate cycle.
""")

try:
    cycle_data = get_electoral_cycle_effects()

    if not cycle_data.is_empty():
        # Average spending by mandate year
        avg_by_year = cycle_data.group_by("ano_no_mandato").agg([
            pl.col("avg_despesa_per_capita").mean().alias("despesa_per_capita"),
            pl.col("avg_crescimento_despesa").mean().alias("crescimento_despesa")
        ]).sort("ano_no_mandato")

        col1, col2 = st.columns(2)

        with col1:
            fig1 = px.bar(
                avg_by_year.to_pandas(),
                x="ano_no_mandato",
                y="despesa_per_capita",
                title="Average Per Capita Spending by Mandate Year",
                labels={
                    "ano_no_mandato": "Year in Mandate",
                    "despesa_per_capita": "Avg Spending per Capita (R$)"
                },
                color_discrete_sequence=["#3498db"]
            )
            fig1.update_layout(height=350)
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            # By ideology and mandate year
            pivot_data = cycle_data.pivot(
                values="avg_despesa_per_capita",
                index="ano_no_mandato",
                columns="bloco_ideologico"
            ).sort("ano_no_mandato")

            fig2 = go.Figure()
            for bloc in ["esquerda", "centro", "direita"]:
                if bloc in pivot_data.columns:
                    fig2.add_trace(go.Scatter(
                        x=pivot_data["ano_no_mandato"].to_list(),
                        y=pivot_data[bloc].to_list(),
                        mode='lines+markers',
                        name=bloc.capitalize(),
                        line=dict(width=3, color=IDEOLOGY_COLORS.get(bloc)),
                        marker=dict(size=10)
                    ))

            fig2.update_layout(
                title="Spending Pattern by Ideology Across Mandate",
                xaxis_title="Year in Mandate",
                yaxis_title="Avg Spending per Capita (R$)",
                height=350,
                legend=dict(orientation="h", yanchor="bottom", y=1.02)
            )
            st.plotly_chart(fig2, use_container_width=True)

        st.info("""
        **Political Budget Cycle:** Research suggests spending often increases in
        Year 4 (election year) as incumbents try to secure votes. Compare patterns
        across ideologies to see if this behavior varies.
        """)

except Exception as e:
    st.error(f"Error loading cycle data: {e}")

st.divider()

# =============================================================================
# DIFFERENCE-IN-DIFFERENCES SUMMARY
# =============================================================================

st.subheader("📐 Ideological Transition Effects (Diff-in-Diff)")

st.markdown("""
What happens to fiscal outcomes when a municipality changes ideological direction?
This analysis compares fiscal changes before and after ideological transitions.
""")

try:
    did_data = get_diff_in_diff_summary()

    if not did_data.is_empty():
        # Create labels for transitions
        transition_labels = {
            "esquerda_para_direita": "Left → Right",
            "centro_para_direita": "Center → Right",
            "direita_para_esquerda": "Right → Left",
            "centro_para_esquerda": "Center → Left",
            "sem_mudanca_significativa": "No Significant Change"
        }

        did_df = did_data.with_columns([
            pl.col("direcao_transicao").replace(transition_labels).alias("Transition")
        ])

        col1, col2 = st.columns(2)

        with col1:
            # Fiscal balance change by transition type
            fig1 = px.bar(
                did_df.to_pandas(),
                x="Transition",
                y="avg_diff_saldo",
                color="direcao_transicao",
                color_discrete_map=TRANSITION_COLORS,
                title="Average Fiscal Balance Change by Transition Type",
                labels={
                    "Transition": "Ideological Transition",
                    "avg_diff_saldo": "Avg Change in Fiscal Balance (R$)"
                }
            )
            fig1.update_layout(showlegend=False, height=350)
            fig1.add_hline(y=0, line_dash="dash", line_color="gray")
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            # Count of transitions
            fig2 = px.bar(
                did_df.to_pandas(),
                x="Transition",
                y="num_transicoes",
                color="direcao_transicao",
                color_discrete_map=TRANSITION_COLORS,
                title="Number of Transitions by Type",
                labels={
                    "Transition": "Ideological Transition",
                    "num_transicoes": "Number of Transitions"
                }
            )
            fig2.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig2, use_container_width=True)

        # Summary table
        st.markdown("**Detailed Transition Effects**")
        summary = did_df.select([
            pl.col("Transition"),
            pl.col("num_transicoes").alias("Count"),
            pl.col("avg_saldo_pre").round(0).alias("Pre-Transition Balance (R$)"),
            pl.col("avg_saldo_post").round(0).alias("Post-Transition Balance (R$)"),
            pl.col("avg_diff_saldo").round(0).alias("Change (R$)"),
        ])
        st.dataframe(summary.to_pandas(), use_container_width=True, hide_index=True)

        st.warning("""
        **Caution:** These are correlational findings, not causal estimates.
        True difference-in-differences analysis requires parallel trends
        assumptions and proper control groups.
        """)

except Exception as e:
    st.error(f"Error loading DiD data: {e}")

st.divider()

# =============================================================================
# POLITICAL VS FISCAL QUALITY CORRELATION
# =============================================================================

st.subheader("🔗 Political Quality vs Fiscal Quality")

st.markdown("""
Is there a relationship between political stability and fiscal performance?
Explore correlations between political quality scores and fiscal quality scores.
""")

try:
    corr_data = get_political_correlations_data()

    if not corr_data.is_empty():
        col1, col2 = st.columns([2, 1])

        with col1:
            # Scatter plot
            color_by = st.radio(
                "Color by:",
                ["Region", "Historical Profile", "Municipality Size"],
                horizontal=True,
                key="corr_color"
            )

            color_col = {
                "Region": "regiao",
                "Historical Profile": "perfil_ideologico_historico",
                "Municipality Size": "porte_municipio"
            }.get(color_by, "regiao")

            fig = px.scatter(
                corr_data.to_pandas(),
                x="score_qualidade_politica",
                y="score_qualidade_fiscal",
                color=color_col,
                hover_name="nome_municipio",
                hover_data=["sigla_uf", "populacao"],
                title="Political Quality vs Fiscal Quality",
                labels={
                    "score_qualidade_politica": "Political Quality Score (0-10)",
                    "score_qualidade_fiscal": "Fiscal Quality Score (0-10)",
                    color_col: color_by
                },
                color_discrete_map=REGION_COLORS if color_col == "regiao" else None,
                opacity=0.6
            )

            # Add trend line
            x_vals = corr_data["score_qualidade_politica"].to_numpy()
            y_vals = corr_data["score_qualidade_fiscal"].to_numpy()
            mask = ~(np.isnan(x_vals) | np.isnan(y_vals))
            if mask.sum() > 2:
                z = np.polyfit(x_vals[mask], y_vals[mask], 1)
                p = np.poly1d(z)
                x_line = np.linspace(x_vals[mask].min(), x_vals[mask].max(), 100)
                fig.add_trace(go.Scatter(
                    x=x_line, y=p(x_line),
                    mode='lines',
                    name='Trend',
                    line=dict(color='black', width=2, dash='dash')
                ))

                # Calculate correlation
                correlation = np.corrcoef(x_vals[mask], y_vals[mask])[0, 1]

            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Correlation Analysis**")

            if 'correlation' in dir():
                st.metric(
                    label="Pearson Correlation",
                    value=f"{correlation:.3f}",
                    help="Correlation between political and fiscal quality"
                )

                if correlation > 0.3:
                    st.success("Moderate positive correlation")
                elif correlation > 0.1:
                    st.info("Weak positive correlation")
                elif correlation > -0.1:
                    st.warning("No significant correlation")
                else:
                    st.error("Negative correlation")

            st.markdown("**Score Definitions:**")
            st.write("""
            - **Political Quality**: Based on continuity,
              low volatility, and moderate ideology
            - **Fiscal Quality**: Based on execution rate,
              positive balance, and growth trends
            """)

            # Summary stats by region
            st.markdown("**Avg Scores by Region**")
            region_summary = corr_data.group_by("regiao").agg([
                pl.col("score_qualidade_politica").mean().round(2).alias("Political"),
                pl.col("score_qualidade_fiscal").mean().round(2).alias("Fiscal")
            ]).sort("regiao")
            st.dataframe(region_summary.to_pandas(), use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error loading correlation data: {e}")

# =============================================================================
# FOOTER
# =============================================================================

st.divider()

st.markdown("""
---
**Data Sources:**
- Electoral data: TSE (Tribunal Superior Eleitoral) via Base dos Dados
- Fiscal data: SICONFI via Base dos Dados (2013-2024)
- Party ideology: Power & Zucco BPCS Surveys (2009, 2012, 2019)

**Methodology Note:** Party ideology scores are based on expert surveys of Brazilian
legislators. "Big tent" parties (MDB, PP, PSD) are flagged as having less stable
ideological positions.
""")
