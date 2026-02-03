{{
    config(
        materialized='table',
        tags=['mart', 'analytics', 'fiscal', 'efficiency']
    )
}}

/*
    ==========================================================================
    MART: EFICIENCIA MUNICIPAL (Municipal Efficiency Index)
    ==========================================================================

    Grain: One row per municipality per year

    Methodology (Percentile-Based Efficiency):
    1. Compute social_outcome_score = 0.4 * idhm + 0.3 * (1-ivs) + 0.3 * (1-gini)
    2. Compute outcome_percentile = percentile rank of social_outcome (0-100)
    3. Compute spending_percentile = percentile rank of spending per capita (0-100)
    4. efficiency_index = outcome_percentile - spending_percentile + 50

    Interpretation:
    - Score > 50: Getting better outcomes than spending level suggests (efficient)
    - Score = 50: Average efficiency for spending level
    - Score < 50: Getting worse outcomes than spending level suggests (inefficient)

    Fiscal Modifier (±5 points):
    - Rewards municipalities with fiscal surpluses
    - Penalizes those with deficits

    Key limitation: IDHM/IVS/Gini only available for census years (2000, 2010)
    Solution: Use 2010 values as baseline for all years 2013-2024

    Use cases:
    - Rank municipalities by efficiency in converting spending to outcomes
    - Identify high-efficiency / low-efficiency municipalities
    - Compare efficiency across regions and municipality sizes
*/

with financas as (
    select
        sk_municipio,
        id_municipio,
        ano,
        despesa_paga,
        receita_liquida,
        saldo_fiscal,
        populacao,
        despesa_total_per_capita,
        saldo_fiscal_per_capita
    from {{ ref('fct_financas_municipais') }}
    where despesa_paga > 0  -- Ensure valid spending data
),

-- Social indicators (use 2010 as baseline - latest available census)
indicadores_sociais as (
    select
        id_municipio,
        idhm,
        idhm_educacao,
        idhm_longevidade,
        idhm_renda,
        ivs,
        indice_gini
    from {{ ref('fct_indicadores_sociais') }}
    where ano = 2010
),

municipios as (
    select
        id_municipio_ibge as id_municipio,
        sk_municipio,
        nome_municipio,
        sigla_uf,
        regiao,
        porte_municipio
    from {{ ref('dim_municipio') }}
),

-- Combine base data for analysis
combined as (
    select
        -- Keys
        f.sk_municipio,
        f.id_municipio,
        f.ano,

        -- Municipality context
        m.nome_municipio,
        m.sigla_uf,
        m.regiao,
        m.porte_municipio,

        -- Raw financial values
        f.despesa_paga,
        f.despesa_total_per_capita,
        f.receita_liquida,
        f.saldo_fiscal,
        f.saldo_fiscal_per_capita,
        f.populacao,

        -- Social indicators (2010 baseline)
        s.idhm,
        s.idhm_educacao,
        s.idhm_longevidade,
        s.idhm_renda,
        s.ivs,
        s.indice_gini,

        -- ============================================================
        -- COMPOSITE SOCIAL OUTCOME SCORE (0-1 scale)
        -- ============================================================
        -- Weighted: 40% IDHM (higher=better), 30% IVS (inverted), 30% Gini (inverted)
        -- IVS and Gini are subtracted from 1 because lower values are better
        round(
            0.4 * s.idhm +
            0.3 * (1.0 - coalesce(s.ivs, 0.5)) +
            0.3 * (1.0 - coalesce(s.indice_gini, 0.5)),
            4
        ) as social_outcome_raw

    from financas f
    inner join municipios m on f.id_municipio = m.id_municipio
    inner join indicadores_sociais s on f.id_municipio = s.id_municipio
    where f.despesa_total_per_capita is not null
      and s.idhm is not null
),

-- Calculate percentile ranks for each municipality per year
percentile_calc as (
    select
        *,

        -- Outcome percentile (0-100): higher = better social outcomes
        round(
            percent_rank() over (partition by ano order by social_outcome_raw) * 100,
            2
        ) as outcome_percentile,

        -- Spending percentile (0-100): higher = higher spending per capita
        round(
            percent_rank() over (partition by ano order by despesa_total_per_capita) * 100,
            2
        ) as spending_percentile

    from combined
),

efficiency_calc as (
    select
        *,

        -- ============================================================
        -- EFFICIENCY INDEX (Percentile-Based)
        -- ============================================================
        -- Formula: outcome_percentile - spending_percentile + 50
        -- Interpretation:
        --   > 50: Better outcomes than spending level suggests
        --   = 50: Average efficiency for spending level
        --   < 50: Worse outcomes than spending level suggests
        round(
            outcome_percentile - spending_percentile + 50,
            2
        ) as efficiency_raw,

        -- Fiscal modifier: rewards/penalizes fiscal responsibility (±5 points)
        case
            when saldo_fiscal_per_capita > 300 then 5
            when saldo_fiscal_per_capita > 100 then 3
            when saldo_fiscal_per_capita > 0 then 1
            when saldo_fiscal_per_capita > -100 then -1
            when saldo_fiscal_per_capita > -300 then -3
            else -5
        end as fiscal_modifier

    from percentile_calc
),

final as (
    select
        -- Keys
        sk_municipio,
        id_municipio,
        ano,

        -- Municipality context
        nome_municipio,
        sigla_uf,
        regiao,
        porte_municipio,
        populacao,

        -- Raw financial values
        despesa_paga,
        despesa_total_per_capita,
        receita_liquida,
        saldo_fiscal,
        saldo_fiscal_per_capita,

        -- Social indicators (2010 baseline)
        idhm,
        idhm_educacao,
        idhm_longevidade,
        idhm_renda,
        ivs,
        indice_gini,

        -- ============================================================
        -- PERCENTILE SCORES (for transparency)
        -- ============================================================
        outcome_percentile,
        spending_percentile as spend_score,

        -- Composite outcome score (scaled to 0-100 for display)
        round(social_outcome_raw * 100, 2) as social_outcome_score,

        -- ============================================================
        -- EFFICIENCY INDEX (0-100 scale, centered at 50)
        -- ============================================================

        -- Raw efficiency index (before fiscal modifier)
        efficiency_raw as efficiency_index_raw,

        -- Fiscal balance modifier
        fiscal_modifier,

        -- Final efficiency index with fiscal modifier (capped 0-100)
        round(
            least(100.0, greatest(0.0, efficiency_raw + fiscal_modifier))
        , 2) as efficiency_index,

        -- ============================================================
        -- EFFICIENCY CATEGORIES
        -- ============================================================
        case
            when efficiency_raw + fiscal_modifier >= 65 then 'Alta Eficiencia'
            when efficiency_raw + fiscal_modifier >= 50 then 'Eficiencia Moderada'
            when efficiency_raw + fiscal_modifier >= 35 then 'Baixa Eficiencia'
            else 'Ineficiente'
        end as categoria_eficiencia,

        -- ============================================================
        -- RANKINGS
        -- ============================================================

        -- National ranking by efficiency (higher = better)
        rank() over (
            partition by ano
            order by (efficiency_raw + fiscal_modifier) desc nulls last
        ) as ranking_eficiencia_nacional,

        -- State ranking by efficiency
        rank() over (
            partition by ano, sigla_uf
            order by (efficiency_raw + fiscal_modifier) desc nulls last
        ) as ranking_eficiencia_uf,

        -- Regional ranking
        rank() over (
            partition by ano, regiao
            order by (efficiency_raw + fiscal_modifier) desc nulls last
        ) as ranking_eficiencia_regiao,

        -- Size category ranking
        rank() over (
            partition by ano, porte_municipio
            order by (efficiency_raw + fiscal_modifier) desc nulls last
        ) as ranking_eficiencia_porte,

        -- Percentile within year (for efficiency index)
        percent_rank() over (
            partition by ano
            order by (efficiency_raw + fiscal_modifier)
        ) as percentile_eficiencia,

        -- ============================================================
        -- YEAR-OVER-YEAR COMPARISON
        -- ============================================================

        lag(efficiency_raw + fiscal_modifier) over (
            partition by id_municipio order by ano
        ) as efficiency_index_ano_anterior,

        (efficiency_raw + fiscal_modifier) - lag(efficiency_raw + fiscal_modifier) over (
            partition by id_municipio order by ano
        ) as delta_efficiency_index,

        -- Metadata
        current_timestamp as _loaded_at

    from efficiency_calc
)

select * from final
order by ano desc, efficiency_index desc
