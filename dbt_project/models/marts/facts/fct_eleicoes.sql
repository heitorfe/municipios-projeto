{{
    config(
        materialized='table',
        tags=['fact', 'electoral', 'gold']
    )
}}

/*
    Electoral Results Fact Table

    Fact table containing mayoral election results by municipality.
    Aggregated to show winning party and vote distribution per election.

    Grain: One row per municipality per election year per turno
*/

with eleicoes as (
    select * from {{ ref('stg_eleicoes') }}
    where cargo = 'prefeito'  -- Focus on mayoral elections
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

-- Get the winner for each municipality/year/turno
vencedores as (
    select
        id_municipio,
        ano,
        turno,
        sigla_partido as partido_vencedor,
        votos as votos_vencedor,
        numero_candidato
    from eleicoes
    where is_eleito = true
),

-- Aggregate votes by municipality/year/turno
agregado as (
    select
        e.id_municipio,
        e.ano,
        e.turno,

        -- Total votes
        sum(e.votos) as total_votos,
        count(distinct e.sequencial_candidato) as total_candidatos,
        count(distinct e.sigla_partido) as total_partidos,

        -- Vote distribution
        max(e.votos) as votos_primeiro_colocado,
        sum(case when e.resultado = 'eleito' or e.resultado like 'eleito%' then e.votos else 0 end) as votos_eleito

    from eleicoes e
    group by e.id_municipio, e.ano, e.turno
),

final as (
    select
        -- Foreign keys
        m.sk_municipio,
        c.sk_ano,

        -- Natural keys (for debugging)
        a.id_municipio,
        a.ano,
        a.turno,

        -- Election metrics
        a.total_votos,
        a.total_candidatos,
        a.total_partidos,

        -- Winner info
        v.partido_vencedor,
        v.votos_vencedor,

        -- Calculated metrics
        case
            when a.total_votos > 0
            then round(cast(v.votos_vencedor as decimal(18,4)) / a.total_votos * 100, 2)
            else null
        end as percentual_vencedor,

        -- Competition index (inverse HHI proxy)
        a.total_candidatos as indice_competicao,

        -- Metadata
        current_timestamp as _loaded_at

    from agregado a
    inner join municipios m on a.id_municipio = m.id_municipio_ibge
    inner join calendario c on a.ano = c.ano
    left join vencedores v on a.id_municipio = v.id_municipio
        and a.ano = v.ano
        and a.turno = v.turno
)

select * from final
