{{
    config(
        materialized='table',
        tags=['mart', 'analytics', 'correlation']
    )
}}

/*
    ==========================================================================
    MART: CORRELACOES POLITICAS (Political Correlations Analysis)
    ==========================================================================

    Cross-sectional dataset aggregated at municipality level.
    Summarizes entire political history for correlation analysis.

    Grain: One row per municipality (aggregated across all mandates)

    Use cases:
    - Correlation matrices between political and socioeconomic variables
    - Scatter plots with trend lines
    - Regional comparisons
    - Portfolio-style segmentation analysis

    Example analyses:
    - "Is average ideology correlated with fiscal health?"
    - "Do municipalities with high political volatility have worse outcomes?"
    - "Is party stability associated with development?"
*/

with mandatos as (
    select * from {{ ref('fct_mandato_completo') }}
),

aggregated as (
    select
        -- =====================================================================
        -- MUNICIPALITY IDENTIFICATION
        -- =====================================================================
        id_municipio,
        max(nome_municipio) as nome_municipio,
        max(sigla_uf) as sigla_uf,
        max(nome_uf) as nome_uf,
        max(regiao) as regiao,
        max(porte_municipio) as porte_municipio,
        max(is_capital) as is_capital,
        max(is_amazonia_legal) as is_amazonia_legal,

        -- =====================================================================
        -- POLITICAL HISTORY SUMMARY
        -- =====================================================================
        count(*) as total_mandatos_analisados,
        min(ano_eleicao) as primeiro_mandato,
        max(ano_eleicao) as ultimo_mandato,

        -- Ideological profile (average over all mandates)
        round(avg(score_ideologico), 2) as media_score_ideologico,

        -- Most frequent ideology bloc
        mode() within group (order by bloco_ideologico) as bloco_predominante,

        -- Most frequent party
        mode() within group (order by partido_vencedor) as partido_predominante,

        -- =====================================================================
        -- POLITICAL STABILITY METRICS
        -- =====================================================================
        -- Party continuity rate
        round(
            sum(case when is_continuidade_partidaria then 1 else 0 end)::decimal
            / nullif(count(*) - 1, 0),  -- -1 because first mandate has no predecessor
            2
        ) as taxa_continuidade_partidaria,

        -- Ideological volatility (average absolute change)
        round(avg(abs(coalesce(delta_ideologico, 0))), 2) as volatilidade_ideologica_media,

        -- Number of different parties that governed
        count(distinct partido_vencedor) as partidos_diferentes_no_poder,

        -- Number of ideological transitions
        sum(case when tipo_transicao_ideologica like 'Guinada%' then 1 else 0 end) as guinadas_ideologicas,

        -- =====================================================================
        -- ELECTORAL COMPETITIVENESS
        -- =====================================================================
        round(avg(percentual_vencedor), 2) as media_margem_vitoria,
        round(avg(total_candidatos), 1) as media_candidatos_por_eleicao,

        -- Proportion of competitive elections
        round(
            sum(case when nivel_competicao = 'Alta' then 1 else 0 end)::decimal / count(*),
            2
        ) as proporcao_eleicoes_competitivas,

        -- =====================================================================
        -- FISCAL PERFORMANCE AVERAGES
        -- =====================================================================
        round(avg(media_saldo_fiscal), 2) as media_saldo_fiscal_historico,
        round(avg(media_taxa_execucao), 2) as media_taxa_execucao_historico,
        round(avg(receita_per_capita), 2) as media_receita_pc_historico,
        round(avg(proporcao_anos_deficit), 2) as media_proporcao_deficit,

        -- Best and worst fiscal performance
        max(media_saldo_fiscal) as melhor_saldo_fiscal,
        min(media_saldo_fiscal) as pior_saldo_fiscal,

        -- =====================================================================
        -- SOCIAL DEVELOPMENT BASELINES
        -- =====================================================================
        max(idhm_2000) as idhm_2000,
        max(idhm_2010) as idhm_2010,
        max(delta_idhm_decada) as delta_idhm_2000_2010,

        max(ivs_2000) as ivs_2000,
        max(ivs_2010) as ivs_2010,
        max(delta_ivs_decada) as delta_ivs_2000_2010,

        max(gini_2000) as gini_2000,
        max(gini_2010) as gini_2010,
        max(delta_gini_decada) as delta_gini_2000_2010,

        -- =====================================================================
        -- POPULATION DYNAMICS
        -- =====================================================================
        round(avg(crescimento_pop_pct), 2) as crescimento_pop_medio_por_mandato,
        min(pop_inicio_mandato) as pop_primeiro_mandato,
        max(pop_fim_mandato) as pop_ultimo_mandato

    from mandatos
    group by id_municipio
),

-- Add derived categories
final as (
    select
        *,

        -- =====================================================================
        -- DERIVED POLITICAL PROFILE
        -- =====================================================================
        case
            when media_score_ideologico <= -1.0 then 'Historicamente de Esquerda'
            when media_score_ideologico >= 1.0 then 'Historicamente de Direita'
            else 'Politicamente Misto'
        end as perfil_ideologico_historico,

        case
            when taxa_continuidade_partidaria >= 0.7 then 'Alta Estabilidade'
            when taxa_continuidade_partidaria >= 0.4 then 'Estabilidade Moderada'
            when taxa_continuidade_partidaria >= 0.2 then 'Baixa Estabilidade'
            else 'Alta Rotatividade'
        end as categoria_estabilidade_politica,

        case
            when volatilidade_ideologica_media >= 1.5 then 'Alta Volatilidade'
            when volatilidade_ideologica_media >= 0.8 then 'Volatilidade Moderada'
            when volatilidade_ideologica_media >= 0.3 then 'Baixa Volatilidade'
            else 'Ideologicamente Estavel'
        end as categoria_volatilidade_ideologica,

        -- =====================================================================
        -- DERIVED FISCAL PROFILE
        -- =====================================================================
        case
            when media_saldo_fiscal_historico > 0 and media_proporcao_deficit < 0.2 then 'Fiscalmente Saudavel'
            when media_saldo_fiscal_historico > 0 then 'Fiscalmente Estavel'
            when media_proporcao_deficit < 0.5 then 'Fiscalmente Fragil'
            else 'Fiscalmente Vulneravel'
        end as perfil_fiscal_historico,

        -- =====================================================================
        -- DEVELOPMENT TRAJECTORY
        -- =====================================================================
        case
            when delta_idhm_2000_2010 >= 0.15 then 'Avanco Expressivo'
            when delta_idhm_2000_2010 >= 0.10 then 'Avanco Moderado'
            when delta_idhm_2000_2010 >= 0.05 then 'Avanco Lento'
            else 'Estagnacao ou Retrocesso'
        end as trajetoria_desenvolvimento,

        -- =====================================================================
        -- COMPOSITE SCORES (for ranking)
        -- =====================================================================
        -- Political quality score (higher = more stable, competitive)
        round(
            (coalesce(taxa_continuidade_partidaria, 0.5) * 0.4 +
             (1 - coalesce(volatilidade_ideologica_media, 1) / 4) * 0.3 +
             coalesce(proporcao_eleicoes_competitivas, 0.5) * 0.3) * 10,
            2
        ) as score_qualidade_politica,

        -- Fiscal quality score (higher = healthier finances)
        case
            when media_saldo_fiscal_historico is not null then
                round(
                    ((1 - coalesce(media_proporcao_deficit, 0.5)) * 0.5 +
                     coalesce(media_taxa_execucao_historico, 50) / 100 * 0.5) * 10,
                    2
                )
        end as score_qualidade_fiscal

    from aggregated
)

select * from final
order by sigla_uf, nome_municipio
