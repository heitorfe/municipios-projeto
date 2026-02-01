{{
    config(
        materialized='view',
        tags=['staging', 'financial', 'revenue']
    )
}}

/*
    Staging model for municipal revenues.

    This model cleans and standardizes municipal revenue data from SICONFI.
    Contains budget revenue data by source/account.

    Source: Base dos Dados - br_me_siconfi.municipio_receitas_orcamentarias
    Grain: One row per municipality per year per account per stage

    Key concepts:
    - Receitas Brutas Realizadas: Gross revenues collected
    - Deduções - FUNDEB: Deductions for education fund
    - Deduções - Transferências Constitucionais: Constitutional transfer deductions
    - Outras Deduções: Other revenue deductions
*/

with source as (
    select * from read_parquet('../data/raw/municipio_receitas_orcamentarias.parquet')
),

cleaned as (
    select
        -- Time dimension
        cast(ano as integer) as ano,

        -- Geographic keys
        cast(id_municipio as varchar(7)) as id_municipio,
        upper(sigla_uf) as sigla_uf,

        -- Revenue stage (standardized)
        estagio as estagio_original,
        coalesce(estagio_bd, estagio) as estagio,

        -- Account classification
        portaria,
        conta as conta_original,
        id_conta_bd,
        conta_bd as conta,

        -- Financial value (in BRL)
        cast(valor as decimal(18, 2)) as valor,

        -- Derived: Revenue type classification
        case
            when coalesce(estagio_bd, estagio) ilike '%brutas%realizadas%' then 'receita_bruta'
            when coalesce(estagio_bd, estagio) ilike '%fundeb%' then 'deducao_fundeb'
            when coalesce(estagio_bd, estagio) ilike '%transfer%ncias%constitucionais%' then 'deducao_transferencias'
            when coalesce(estagio_bd, estagio) ilike '%dedu%' then 'outras_deducoes'
            else 'outro'
        end as tipo_receita,

        -- Flag for deductions (negative impact on net revenue)
        case
            when coalesce(estagio_bd, estagio) ilike '%dedu%' then true
            else false
        end as is_deducao,

        -- Metadata
        current_timestamp as _loaded_at

    from source
    where
        id_municipio is not null
        and ano is not null
        and valor is not null
)

select * from cleaned
