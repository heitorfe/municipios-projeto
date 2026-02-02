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

    Methodology:
    1. Normalize indicators (min-max per year)
    2. social_outcome_score = 0.4 * idhm_norm + 0.3 * ivs_adj + 0.3 * gini_adj
    3. efficiency_index = social_outcome_score / (spend_norm + 0.1)
    4. Rescale to 0-100
    5. Optional fiscal balance modifier (±10 points)

    Key limitation: IDHM/IVS/Gini only available for census years (2000, 2010)
    Solution: Use 2010 values as baseline for all years 2013-2024

    Future enhancement: When 2022 census data becomes available, implement
    interpolation for more accurate year-over-year efficiency tracking.

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

-- Calculate min/max per year for normalization
-- This ensures fair comparison within each year (controls for inflation)
normalization_params as (
    select
        f.ano,

        -- Spending normalization bounds
        min(f.despesa_total_per_capita) as min_despesa_pc,
        max(f.despesa_total_per_capita) as max_despesa_pc,
        percentile_cont(0.05) within group (order by f.despesa_total_per_capita) as p5_despesa_pc,
        percentile_cont(0.95) within group (order by f.despesa_total_per_capita) as p95_despesa_pc,

        -- Social indicator bounds (from 2010 census, constant across years)
        min(s.idhm) as min_idhm,
        max(s.idhm) as max_idhm,
        min(s.ivs) as min_ivs,
        max(s.ivs) as max_ivs,
        min(s.indice_gini) as min_gini,
        max(s.indice_gini) as max_gini

    from financas f
    inner join indicadores_sociais s on f.id_municipio = s.id_municipio
    where f.despesa_total_per_capita is not null
      and s.idhm is not null
    group by f.ano
),

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

        -- Normalization parameters
        np.min_despesa_pc, np.max_despesa_pc,
        np.p5_despesa_pc, np.p95_despesa_pc,
        np.min_idhm, np.max_idhm,
        np.min_ivs, np.max_ivs,
        np.min_gini, np.max_gini

    from financas f
    inner join municipios m on f.id_municipio = m.id_municipio
    inner join indicadores_sociais s on f.id_municipio = s.id_municipio
    inner join normalization_params np on f.ano = np.ano
),

normalized as (
    select
        *,

        -- ============================================================
        -- NORMALIZED SCORES (0-1 scale)
        -- ============================================================

        -- Normalized spending (0-1, using robust min-max with percentile bounds)
        -- Higher value = higher spending per capita
        case
            when p95_despesa_pc > p5_despesa_pc then
                least(1.0, greatest(0.0,
                    (despesa_total_per_capita - p5_despesa_pc) /
                    (p95_despesa_pc - p5_despesa_pc)
                ))
            else 0.5
        end as spend_norm,

        -- Normalized IDHM (0-1, higher = better)
        case
            when max_idhm > min_idhm then
                (idhm - min_idhm) / (max_idhm - min_idhm)
            else 0.5
        end as idhm_norm,

        -- Adjusted IVS (0-1, INVERTED: lower IVS = lower vulnerability = better)
        case
            when max_ivs > min_ivs then
                1.0 - ((ivs - min_ivs) / (max_ivs - min_ivs))
            else 0.5
        end as ivs_adj,

        -- Adjusted Gini (0-1, INVERTED: lower Gini = less inequality = better)
        case
            when max_gini > min_gini then
                1.0 - ((indice_gini - min_gini) / (max_gini - min_gini))
            else 0.5
        end as gini_adj

    from combined
),

efficiency_calc as (
    select
        *,

        -- ============================================================
        -- COMPOSITE SOCIAL OUTCOME SCORE
        -- ============================================================
        -- Weighted average: 40% IDHM, 30% IVS, 30% Gini
        -- This captures human development, social vulnerability, and inequality
        round(
            0.4 * idhm_norm + 0.3 * ivs_adj + 0.3 * gini_adj,
            4
        ) as social_outcome_score,

        -- ============================================================
        -- BASE EFFICIENCY RATIO
        -- ============================================================
        -- Outcome per unit of input (normalized)
        -- Adding 0.1 to denominator prevents division by zero and
        -- avoids extreme values for very low spenders
        round(
            (0.4 * idhm_norm + 0.3 * ivs_adj + 0.3 * gini_adj) /
            (spend_norm + 0.1),
            4
        ) as efficiency_ratio

    from normalized
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
        -- NORMALIZED SCORES (as percentages for readability)
        -- ============================================================
        round(spend_norm * 100, 2) as spend_score,
        round(idhm_norm * 100, 2) as idhm_score,
        round(ivs_adj * 100, 2) as ivs_score,
        round(gini_adj * 100, 2) as gini_score,

        -- Composite outcome score (0-100)
        round(social_outcome_score * 100, 2) as social_outcome_score,

        -- Base efficiency ratio
        efficiency_ratio,

        -- ============================================================
        -- EFFICIENCY INDEX (0-100 scale)
        -- ============================================================

        -- Raw efficiency index (scaled to approximately 0-100)
        round(
            least(100.0, greatest(0.0, efficiency_ratio * 50))
        , 2) as efficiency_index_raw,

        -- Fiscal balance modifier (rewards fiscal responsibility)
        case
            when saldo_fiscal_per_capita > 500 then 10
            when saldo_fiscal_per_capita > 200 then 5
            when saldo_fiscal_per_capita > 0 then 2
            when saldo_fiscal_per_capita > -200 then -2
            when saldo_fiscal_per_capita > -500 then -5
            else -10
        end as fiscal_modifier,

        -- Final efficiency index with fiscal modifier
        round(
            least(100.0, greatest(0.0,
                efficiency_ratio * 50 +
                case
                    when saldo_fiscal_per_capita > 500 then 10
                    when saldo_fiscal_per_capita > 200 then 5
                    when saldo_fiscal_per_capita > 0 then 2
                    when saldo_fiscal_per_capita > -200 then -2
                    when saldo_fiscal_per_capita > -500 then -5
                    else -10
                end
            ))
        , 2) as efficiency_index,

        -- ============================================================
        -- EFFICIENCY CATEGORIES
        -- ============================================================
        case
            when efficiency_ratio * 50 >= 70 then 'Alta Eficiencia'
            when efficiency_ratio * 50 >= 50 then 'Eficiencia Moderada'
            when efficiency_ratio * 50 >= 30 then 'Baixa Eficiencia'
            else 'Ineficiente'
        end as categoria_eficiencia,

        -- ============================================================
        -- RANKINGS
        -- ============================================================

        -- National ranking by efficiency (higher = better)
        rank() over (
            partition by ano
            order by efficiency_ratio desc nulls last
        ) as ranking_eficiencia_nacional,

        -- State ranking by efficiency
        rank() over (
            partition by ano, sigla_uf
            order by efficiency_ratio desc nulls last
        ) as ranking_eficiencia_uf,

        -- Regional ranking
        rank() over (
            partition by ano, regiao
            order by efficiency_ratio desc nulls last
        ) as ranking_eficiencia_regiao,

        -- Size category ranking
        rank() over (
            partition by ano, porte_municipio
            order by efficiency_ratio desc nulls last
        ) as ranking_eficiencia_porte,

        -- Percentile within year
        percent_rank() over (
            partition by ano
            order by efficiency_ratio
        ) as percentile_eficiencia,

        -- ============================================================
        -- YEAR-OVER-YEAR COMPARISON
        -- ============================================================

        lag(efficiency_ratio) over (
            partition by id_municipio order by ano
        ) as efficiency_ratio_ano_anterior,

        efficiency_ratio - lag(efficiency_ratio) over (
            partition by id_municipio order by ano
        ) as delta_efficiency_ratio,

        -- Metadata
        current_timestamp as _loaded_at

    from efficiency_calc
)

select * from final
order by ano desc, efficiency_index desc
