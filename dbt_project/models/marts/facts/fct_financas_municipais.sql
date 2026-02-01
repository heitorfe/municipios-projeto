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

        -- Metadata
        current_timestamp as _loaded_at

    from combined cb
    inner join municipios m on cb.id_municipio = m.id_municipio_ibge
    inner join calendario c on cb.ano = c.ano
)

select * from final
