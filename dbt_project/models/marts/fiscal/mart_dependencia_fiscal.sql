{{
    config(
        materialized='table',
        tags=['mart', 'analytics', 'fiscal', 'dependency']
    )
}}

/*
    ==========================================================================
    MART: DEPENDENCIA FISCAL (Fiscal Dependency Analysis)
    ==========================================================================

    Grain: One row per municipality per year

    Key metrics:
    - dependency_ratio: Federal transfers as % of total revenue
    - own_revenue_ratio: Self-generated revenue as % of total
    - transfers_per_capita: Federal transfers normalized by population
    - revenue_effort_index: Own revenue relative to national median

    Use cases:
    - Identify municipalities highly dependent on federal transfers
    - Compare fiscal autonomy across regions
    - Track dependency trends over time
    - Rank municipalities by revenue effort

    Data sources:
    - fct_financas_municipais: Revenue and expense data
    - fct_transferencias_federais: Federal transfer breakdown
    - dim_municipio: Geographic and demographic context
*/

with financas as (
    select
        sk_municipio,
        id_municipio,
        ano,
        receita_bruta,
        receita_liquida,
        despesa_paga,
        saldo_fiscal,
        populacao,
        receita_total_per_capita,
        despesa_total_per_capita,
        saldo_fiscal_per_capita
    from {{ ref('fct_financas_municipais') }}
    where receita_liquida > 0  -- Filter out invalid revenue records
),

transferencias as (
    select
        id_municipio,
        ano,
        total_transferencias_federais,
        fpm_value,
        fundeb_value,
        sus_transfers,
        itr_value,
        transferencias_per_capita
    from {{ ref('fct_transferencias_federais') }}
),

municipios as (
    select
        sk_municipio,
        id_municipio_ibge as id_municipio,
        nome_municipio,
        sigla_uf,
        regiao,
        porte_municipio,
        idhm_2010,
        faixa_idhm
    from {{ ref('dim_municipio') }}
),

-- Calculate national medians per year for benchmarking
national_medians as (
    select
        f.ano,
        -- Median of own revenue per capita (for revenue effort index)
        percentile_cont(0.5) within group (
            order by case
                when f.populacao > 0
                then (f.receita_liquida - coalesce(t.total_transferencias_federais, 0)) / f.populacao
            end
        ) as median_receita_propria_pc_nacional,
        -- Median of transfers per capita
        percentile_cont(0.5) within group (
            order by t.transferencias_per_capita
        ) as median_transfer_pc_nacional,
        -- Median dependency ratio
        percentile_cont(0.5) within group (
            order by case
                when f.receita_liquida > 0
                then coalesce(t.total_transferencias_federais, 0) / f.receita_liquida * 100
            end
        ) as median_dependency_ratio_nacional
    from financas f
    left join transferencias t
        on f.id_municipio = t.id_municipio
        and f.ano = t.ano
    where f.receita_total_per_capita is not null
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
        m.idhm_2010,
        m.faixa_idhm,

        -- Raw financial values
        f.receita_bruta,
        f.receita_liquida,
        f.despesa_paga,
        f.saldo_fiscal,
        f.populacao,

        -- Transfer values
        coalesce(t.total_transferencias_federais, 0) as total_transferencias_federais,
        coalesce(t.fpm_value, 0) as fpm_value,
        coalesce(t.fundeb_value, 0) as fundeb_value,
        coalesce(t.sus_transfers, 0) as sus_transfers,
        coalesce(t.itr_value, 0) as itr_value,

        -- Per capita metrics
        f.receita_total_per_capita,
        f.despesa_total_per_capita,
        f.saldo_fiscal_per_capita,
        t.transferencias_per_capita,

        -- Own revenue calculation (total revenue minus federal transfers)
        f.receita_liquida - coalesce(t.total_transferencias_federais, 0) as receita_propria,

        -- National medians for benchmarking
        nm.median_receita_propria_pc_nacional,
        nm.median_transfer_pc_nacional,
        nm.median_dependency_ratio_nacional

    from financas f
    inner join municipios m on f.id_municipio = m.id_municipio
    left join transferencias t
        on f.id_municipio = t.id_municipio
        and f.ano = t.ano
    left join national_medians nm on f.ano = nm.ano
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
        idhm_2010,
        faixa_idhm,

        -- Raw values
        receita_bruta,
        receita_liquida,
        despesa_paga,
        saldo_fiscal,
        populacao,

        -- Transfer breakdown
        total_transferencias_federais,
        fpm_value,
        fundeb_value,
        sus_transfers,
        itr_value,

        -- Own revenue
        receita_propria,

        -- Per capita metrics
        receita_total_per_capita,
        despesa_total_per_capita,
        saldo_fiscal_per_capita,
        transferencias_per_capita,

        -- Own revenue per capita
        case
            when populacao > 0
            then round(receita_propria / populacao, 2)
        end as receita_propria_per_capita,

        -- ============================================================
        -- KEY DEPENDENCY METRICS
        -- ============================================================

        -- Dependency ratio: % of revenue from federal transfers
        case
            when receita_liquida > 0
            then round(total_transferencias_federais / receita_liquida * 100, 2)
        end as dependency_ratio,

        -- Own revenue ratio: % of self-generated revenue
        case
            when receita_liquida > 0
            then round(receita_propria / receita_liquida * 100, 2)
        end as own_revenue_ratio,

        -- Revenue Effort Index: own revenue pc / national median
        -- > 1.0 means above-average effort to generate own revenue
        case
            when median_receita_propria_pc_nacional > 0 and populacao > 0
            then round((receita_propria / populacao) / median_receita_propria_pc_nacional, 3)
        end as revenue_effort_index,

        -- Transfer dependency relative to national median
        case
            when median_dependency_ratio_nacional > 0 and receita_liquida > 0
            then round(
                (total_transferencias_federais / receita_liquida * 100) / median_dependency_ratio_nacional,
                3
            )
        end as relative_dependency_index,

        -- National benchmarks for reference
        median_receita_propria_pc_nacional,
        median_transfer_pc_nacional,
        median_dependency_ratio_nacional,

        -- ============================================================
        -- DEPENDENCY CATEGORIES
        -- ============================================================

        case
            when receita_liquida > 0 then
                case
                    when (total_transferencias_federais / receita_liquida * 100) >= 80
                        then 'Extremamente Dependente (80%+)'
                    when (total_transferencias_federais / receita_liquida * 100) >= 60
                        then 'Altamente Dependente (60-80%)'
                    when (total_transferencias_federais / receita_liquida * 100) >= 40
                        then 'Moderadamente Dependente (40-60%)'
                    when (total_transferencias_federais / receita_liquida * 100) >= 20
                        then 'Baixa Dependencia (20-40%)'
                    else 'Autonomo (<20%)'
                end
        end as categoria_dependencia,

        -- Revenue effort category
        case
            when median_receita_propria_pc_nacional > 0 and populacao > 0 then
                case
                    when (receita_propria / populacao) / median_receita_propria_pc_nacional >= 1.5
                        then 'Alto Esforco Arrecadatorio'
                    when (receita_propria / populacao) / median_receita_propria_pc_nacional >= 1.0
                        then 'Esforco Acima da Media'
                    when (receita_propria / populacao) / median_receita_propria_pc_nacional >= 0.5
                        then 'Esforco Abaixo da Media'
                    else 'Baixo Esforco Arrecadatorio'
                end
        end as categoria_esforco_arrecadatorio,

        -- ============================================================
        -- YEAR-OVER-YEAR ANALYSIS
        -- ============================================================

        -- Previous year dependency ratio
        lag(
            case when receita_liquida > 0
                then round(total_transferencias_federais / receita_liquida * 100, 2)
            end
        ) over (partition by id_municipio order by ano) as dependency_ratio_ano_anterior,

        -- Change in dependency ratio
        case when receita_liquida > 0
            then round(total_transferencias_federais / receita_liquida * 100, 2)
        end
        - lag(
            case when receita_liquida > 0
                then round(total_transferencias_federais / receita_liquida * 100, 2)
            end
        ) over (partition by id_municipio order by ano) as delta_dependency_ratio,

        -- ============================================================
        -- RANKINGS
        -- ============================================================

        -- National ranking by dependency (lower = less dependent)
        rank() over (
            partition by ano
            order by case when receita_liquida > 0
                then total_transferencias_federais / receita_liquida
            end
        ) as ranking_dependency_nacional,

        -- State ranking by dependency
        rank() over (
            partition by ano, sigla_uf
            order by case when receita_liquida > 0
                then total_transferencias_federais / receita_liquida
            end
        ) as ranking_dependency_uf,

        -- National ranking by revenue effort (higher = better)
        rank() over (
            partition by ano
            order by case when populacao > 0 and median_receita_propria_pc_nacional > 0
                then (receita_propria / populacao) / median_receita_propria_pc_nacional
            end desc nulls last
        ) as ranking_esforco_nacional,

        -- Metadata
        current_timestamp as _loaded_at

    from combined
)

select * from final
where dependency_ratio <= 100
order by ano desc, id_municipio
