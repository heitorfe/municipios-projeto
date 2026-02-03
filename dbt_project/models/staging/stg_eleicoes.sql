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

with 
candidatos as 
(
    select *
    from read_parquet('../data/raw/candidatos.parquet')
),
source as (
    select * from read_parquet('../data/raw/resultados_candidato_municipio.parquet')
),

cleaned as (
    select
        -- Time dimension
        cast(s.ano as integer) as ano,
        cast(s.turno as integer) as turno,

        -- Election identifiers
        s.id_eleicao,
        s.tipo_eleicao,
        cast(s.data_eleicao as date) as data_eleicao,

        -- Geographic keys
        cast(s.id_municipio as varchar(7)) as id_municipio,
        cast(s.id_municipio_tse as varchar(5)) as id_municipio_tse,
        upper(s.sigla_uf) as sigla_uf,

        -- Political party
        cast(s.numero_partido as integer) as numero_partido,
        upper(trim(s.sigla_partido)) as sigla_partido,

        -- Candidate info
        s.cargo,
        s.titulo_eleitoral_candidato,
        s.sequencial_candidato,
        cast(s.numero_candidato as integer) as numero_candidato,
        upper(trim(c.nome)) as nome_candidato,
        upper(trim(c.nome_urna)) as nome_urna_candidato,
        -- Results
        lower(trim(s.resultado)) as resultado,
        cast(s.votos as bigint) as votos,

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

    from source s
    LEFT JOIN 
        candidatos c
    ON s.titulo_eleitoral_candidato = c.titulo_eleitoral

    where
        s.id_municipio is not null
        and s.ano is not null
)

select * from cleaned
