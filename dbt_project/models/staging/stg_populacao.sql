{{
    config(
        materialized='view',
        tags=['staging', 'population', 'demographics']
    )
}}

/*
    Staging model for population data.

    This model cleans and standardizes population estimates from IBGE.
    Contains annual population estimates from 1991 to 2025.

    Source: Base dos Dados - br_ibge_populacao.municipio
    Grain: One row per municipality per year
*/

with source as (
    select * from read_parquet('../data/raw/populacao.parquet')
),

cleaned as (
    select
        -- Keys
        cast(id_municipio as varchar(7)) as id_municipio,
        cast(ano as integer) as ano,
        upper(sigla_uf) as sigla_uf,

        -- Population
        cast(populacao as bigint) as populacao,

        -- Metadata
        current_timestamp as _loaded_at

    from source
    where
        id_municipio is not null
        and ano is not null
        and populacao is not null
)

select * from cleaned
