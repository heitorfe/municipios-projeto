{{
    config(
        materialized='table',
        tags=['fact', 'financial', 'transfers', 'gold']
    )
}}

/*
    ==========================================================================
    FACT: TRANSFERENCIAS FEDERAIS (Federal Transfers Fact Table)
    ==========================================================================

    Grain: One row per municipality per year

    Contains breakdown of federal transfer types:
    - FPM: Fundo de Participação dos Municípios (constitutional revenue sharing)
    - FUNDEB: Education fund transfers
    - SUS: Health system transfers (Sistema Único de Saúde)
    - ITR: Rural Land Tax (federal share)
    - IPI-Exportação: Export compensation (Lei Kandir)
    - Other federal transfers

    Data availability: 2013-2024 (SICONFI coverage)

    Use cases:
    - Analyze federal transfer composition by municipality
    - Calculate fiscal dependency ratios
    - Compare transfer structures across regions
*/

with transfers_raw as (
    select * from {{ ref('stg_transferencias') }}
),

-- Pivot to wide format (one row per municipality-year)
transfers_pivoted as (
    select
        id_municipio,
        ano,
        any_value(sigla_uf) as sigla_uf,

        -- Main constitutional transfers
        coalesce(sum(case when tipo_transferencia = 'FPM' then valor_total end), 0) as fpm_value,
        coalesce(sum(case when tipo_transferencia = 'FUNDEB' then valor_total end), 0) as fundeb_value,
        coalesce(sum(case when tipo_transferencia = 'SUS' then valor_total end), 0) as sus_transfers,

        -- Other federal transfers
        coalesce(sum(case when tipo_transferencia = 'ITR' then valor_total end), 0) as itr_value,
        coalesce(sum(case when tipo_transferencia = 'IPI_EXPORTACAO' then valor_total end), 0) as ipi_exportacao_value,
        coalesce(sum(case when tipo_transferencia = 'OUTRAS_FEDERAIS' then valor_total end), 0) as outras_federais,

        -- Total federal transfers
        sum(valor_total) as total_transferencias_federais,

        -- Counts for data quality monitoring
        sum(num_contas) as total_contas_agregadas

    from transfers_raw
    group by id_municipio, ano
),

municipios as (
    select
        sk_municipio,
        id_municipio_ibge,
        populacao
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

final as (
    select
        -- Foreign keys (for star schema joins)
        m.sk_municipio,
        c.sk_ano,

        -- Natural keys
        t.id_municipio,
        t.ano,
        t.sigla_uf,

        -- Main constitutional transfer components
        t.fpm_value,
        t.fundeb_value,
        t.sus_transfers,
        t.itr_value,
        t.ipi_exportacao_value,
        t.outras_federais,

        -- Total
        t.total_transferencias_federais,

        -- Transfer composition shares (%)
        case
            when t.total_transferencias_federais > 0
            then round(t.fpm_value / t.total_transferencias_federais * 100, 2)
        end as fpm_share_pct,

        case
            when t.total_transferencias_federais > 0
            then round(t.fundeb_value / t.total_transferencias_federais * 100, 2)
        end as fundeb_share_pct,

        case
            when t.total_transferencias_federais > 0
            then round(t.sus_transfers / t.total_transferencias_federais * 100, 2)
        end as sus_share_pct,

        -- Per capita metrics (using annual population)
        case
            when coalesce(p.populacao, m.populacao) > 0
            then round(t.total_transferencias_federais / coalesce(p.populacao, m.populacao), 2)
        end as transferencias_per_capita,

        case
            when coalesce(p.populacao, m.populacao) > 0
            then round(t.fpm_value / coalesce(p.populacao, m.populacao), 2)
        end as fpm_per_capita,

        case
            when coalesce(p.populacao, m.populacao) > 0
            then round(t.fundeb_value / coalesce(p.populacao, m.populacao), 2)
        end as fundeb_per_capita,

        case
            when coalesce(p.populacao, m.populacao) > 0
            then round(t.sus_transfers / coalesce(p.populacao, m.populacao), 2)
        end as sus_per_capita,

        -- Population reference
        coalesce(p.populacao, m.populacao) as populacao,

        -- Data quality indicator
        t.total_contas_agregadas,

        -- Metadata
        current_timestamp as _loaded_at

    from transfers_pivoted t
    inner join municipios m on t.id_municipio = m.id_municipio_ibge
    inner join calendario c on t.ano = c.ano
    left join populacao p on t.id_municipio = p.id_municipio and t.ano = p.ano
)

select * from final
