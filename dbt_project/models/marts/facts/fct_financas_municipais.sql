{{
    config(
        materialized='table',
        tags=['fact', 'financial', 'gold']
    )
}}

/*
    Municipal Finances Fact Table

    Consolidated financial data combining expenses and revenues by municipality/year.
    Aggregated at the municipality-year level for easier analysis.

    Grain: One row per municipality per year

    Note: Values are in BRL (Brazilian Reais) at nominal prices.
    Consider inflation adjustment for time-series analysis.

    Per-capita metrics included for cross-municipality comparison:
    - despesa_total_per_capita: Average spending per inhabitant
    - receita_total_per_capita: Total revenue per inhabitant
    - receita_propria_per_capita: Own revenue (non-transfer) per inhabitant
    - saldo_fiscal_per_capita: Fiscal balance per inhabitant
*/

with despesas as (
    select
        id_municipio,
        ano,
        -- Aggregate by execution stage
        sum(case when tipo_estagio = 'empenhado' then valor else 0 end) as despesa_empenhada,
        sum(case when tipo_estagio = 'liquidado' then valor else 0 end) as despesa_liquidada,
        sum(case when tipo_estagio = 'pago' then valor else 0 end) as despesa_paga
    from {{ ref('stg_despesas') }}
    group by id_municipio, ano
),

receitas as (
    select
        id_municipio,
        ano,
        -- Aggregate by revenue type
        sum(case when tipo_receita = 'receita_bruta' then valor else 0 end) as receita_bruta,
        sum(case when is_deducao = true then valor else 0 end) as deducoes,
        sum(case when tipo_receita = 'deducao_fundeb' then valor else 0 end) as deducao_fundeb
    from {{ ref('stg_receitas') }}
    group by id_municipio, ano
),

municipios as (
    select
        sk_municipio,
        id_municipio_ibge
    from {{ ref('dim_municipio') }}
),

calendario as (
    select
        sk_ano,
        ano
    from {{ ref('dim_calendario') }}
),

populacao as (
    select
        id_municipio,
        ano,
        populacao
    from {{ ref('stg_populacao') }}
),

-- Get latest population as fallback for municipalities without year-specific data
populacao_fallback as (
    select
        id_municipio_ibge as id_municipio,
        populacao
    from {{ ref('dim_municipio') }}
),

combined as (
    select
        coalesce(d.id_municipio, r.id_municipio) as id_municipio,
        coalesce(d.ano, r.ano) as ano,

        -- Expenses
        coalesce(d.despesa_empenhada, 0) as despesa_empenhada,
        coalesce(d.despesa_liquidada, 0) as despesa_liquidada,
        coalesce(d.despesa_paga, 0) as despesa_paga,

        -- Revenues
        coalesce(r.receita_bruta, 0) as receita_bruta,
        coalesce(r.deducoes, 0) as deducoes,
        coalesce(r.deducao_fundeb, 0) as deducao_fundeb

    from despesas d
    full outer join receitas r
        on d.id_municipio = r.id_municipio
        and d.ano = r.ano
),

final as (
    select
        -- Foreign keys
        m.sk_municipio,
        c.sk_ano,

        -- Natural keys (for debugging)
        cb.id_municipio,
        cb.ano,

        -- Expense metrics
        cb.despesa_empenhada,
        cb.despesa_liquidada,
        cb.despesa_paga,

        -- Revenue metrics
        cb.receita_bruta,
        cb.deducoes,
        cb.deducao_fundeb,

        -- Calculated metrics
        cb.receita_bruta - cb.deducoes as receita_liquida,

        -- Fiscal balance (simplified)
        cb.receita_bruta - cb.deducoes - cb.despesa_paga as saldo_fiscal,

        -- Execution rate (how much of committed was actually paid)
        case
            when cb.despesa_empenhada > 0
            then round(cb.despesa_paga / cb.despesa_empenhada * 100, 2)
            else null
        end as taxa_execucao_percentual,

        -- Population (use year-specific or fallback to latest)
        coalesce(p.populacao, pf.populacao) as populacao,

        -- ============================================================
        -- PER-CAPITA METRICS (for cross-municipality comparison)
        -- ============================================================

        -- Total spending per capita (average of execution stages)
        case
            when coalesce(p.populacao, pf.populacao) > 0
            then round(cb.despesa_paga / coalesce(p.populacao, pf.populacao), 2)
        end as despesa_total_per_capita,

        -- Total revenue per capita (net of deductions)
        case
            when coalesce(p.populacao, pf.populacao) > 0
            then round((cb.receita_bruta - cb.deducoes) / coalesce(p.populacao, pf.populacao), 2)
        end as receita_total_per_capita,

        -- Fiscal balance per capita
        case
            when coalesce(p.populacao, pf.populacao) > 0
            then round((cb.receita_bruta - cb.deducoes - cb.despesa_paga) / coalesce(p.populacao, pf.populacao), 2)
        end as saldo_fiscal_per_capita,

        -- Metadata
        current_timestamp as _loaded_at

    from combined cb
    inner join municipios m on cb.id_municipio = m.id_municipio_ibge
    inner join calendario c on cb.ano = c.ano
    left join populacao p on cb.id_municipio = p.id_municipio and cb.ano = p.ano
    left join populacao_fallback pf on cb.id_municipio = pf.id_municipio
)

select * from final
