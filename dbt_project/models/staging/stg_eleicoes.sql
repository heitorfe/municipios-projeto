{{
    config(
        materialized='view',
        tags=['staging', 'electoral']
    )
}}

/*
    Staging model for electoral results.

    This model cleans and standardizes municipal election data from TSE.
    Contains candidate-level results for mayoral elections.

    Source: Base dos Dados - br_tse_eleicoes.resultados_candidato_municipio
    Grain: One row per candidate per municipality per election
*/

with source as (
    select * from read_parquet('../data/raw/resultados_candidato_municipio.parquet')
),

cleaned as (
    select
        -- Time dimension
        cast(ano as integer) as ano,
        cast(turno as integer) as turno,

        -- Election identifiers
        id_eleicao,
        tipo_eleicao,
        cast(data_eleicao as date) as data_eleicao,

        -- Geographic keys
        cast(id_municipio as varchar(7)) as id_municipio,
        cast(id_municipio_tse as varchar(5)) as id_municipio_tse,
        upper(sigla_uf) as sigla_uf,

        -- Political party
        cast(numero_partido as integer) as numero_partido,
        upper(trim(sigla_partido)) as sigla_partido,

        -- Candidate info
        cargo,
        titulo_eleitoral_candidato,
        sequencial_candidato,
        cast(numero_candidato as integer) as numero_candidato,

        -- Results
        lower(trim(resultado)) as resultado,
        cast(votos as bigint) as votos,

        -- Derived flags
        case
            when lower(trim(resultado)) in ('eleito', 'eleito por média', 'eleito por qp')
            then true else false
        end as is_eleito,

        case
            when turno = 2 then true else false
        end as is_segundo_turno,

        -- Metadata
        current_timestamp as _loaded_at

    from source
    where
        id_municipio is not null
        and ano is not null
)

select * from cleaned
