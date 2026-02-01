{{
    config(
        materialized='view',
        tags=['staging', 'municipality']
    )
}}

/*
    Staging model for municipality directory.

    This model cleans and standardizes the raw municipality data from IBGE.
    It serves as the foundation for the dim_municipio dimension table.

    Source: Base dos Dados - br_bd_diretorios_brasil.municipio
*/

with source as (
    select * from read_parquet('../data/raw/municipio.parquet')
),

cleaned as (
    select
        -- Primary keys
        cast(id_municipio as varchar(7)) as id_municipio_ibge,
        cast(id_municipio_tse as varchar(5)) as id_municipio_tse,

        -- Names and codes
        trim(nome) as nome_municipio,
        upper(sigla_uf) as sigla_uf,
        trim(nome_uf) as nome_uf,
        trim(nome_regiao) as regiao,
        trim(nome_mesorregiao) as mesorregiao,
        trim(nome_microrregiao) as microrregiao,

        -- Additional identifiers
        id_municipio_6,
        id_municipio_rf,
        id_municipio_bcb,
        ddd,
        id_uf,

        -- Region hierarchies
        id_regiao_imediata,
        nome_regiao_imediata,
        id_regiao_intermediaria,
        nome_regiao_intermediaria,
        id_regiao_metropolitana,
        nome_regiao_metropolitana,

        -- Flags
        case when capital_uf = 1 then true else false end as is_capital,
        case when amazonia_legal = 1 then true else false end as is_amazonia_legal,

        -- Geographic (centroid is stored as string/geometry)
        centroide,

        -- Metadata
        current_timestamp as _loaded_at

    from source
    where id_municipio is not null
)

select * from cleaned
