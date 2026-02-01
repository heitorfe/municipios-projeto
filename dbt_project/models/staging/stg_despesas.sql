{{
    config(
        materialized='view',
        tags=['staging', 'financial', 'expenses']
    )
}}

/*
    Staging model for municipal expenses.

    This model cleans and standardizes municipal expense data from SICONFI.
    Contains budget execution data by function/account.

    Source: Base dos Dados - br_me_siconfi.municipio_despesas_funcao
    Grain: One row per municipality per year per account per execution stage

    Key concepts:
    - Empenhadas: Committed (reserved budget)
    - Liquidadas: Verified/accrued (services delivered)
    - Pagas: Paid (actual disbursement)
    - Restos a Pagar: Carryover payables from previous years
*/

with source as (
    select * from read_parquet('../data/raw/municipio_despesas_funcao.parquet')
),

cleaned as (
    select
        -- Time dimension
        cast(ano as integer) as ano,

        -- Geographic keys
        cast(id_municipio as varchar(7)) as id_municipio,
        upper(sigla_uf) as sigla_uf,

        -- Budget execution stage (standardized)
        estagio as estagio_original,
        coalesce(estagio_bd, estagio) as estagio,

        -- Account classification
        portaria,
        conta as conta_original,
        id_conta_bd,
        conta_bd as conta,

        -- Financial value (in BRL)
        cast(valor as decimal(18, 2)) as valor,

        -- Derived: Execution stage classification
        case
            when coalesce(estagio_bd, estagio) ilike '%empenhadas%' then 'empenhado'
            when coalesce(estagio_bd, estagio) ilike '%liquidadas%' then 'liquidado'
            when coalesce(estagio_bd, estagio) ilike '%pagas%' then 'pago'
            when coalesce(estagio_bd, estagio) ilike '%restos%processados%' then 'restos_a_pagar'
            else 'outro'
        end as tipo_estagio,

        -- Metadata
        current_timestamp as _loaded_at

    from source
    where
        id_municipio is not null
        and ano is not null
        and valor is not null
)

select * from cleaned
