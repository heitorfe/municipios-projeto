{{
    config(
        materialized='table',
        tags=['dimension', 'political', 'gold']
    )
}}

/*
    ==========================================================================
    DIMENSION: MANDATO PREFEITO (Mayoral Mandate)
    ==========================================================================

    Dimension table tracking mayoral terms (mandates) at the municipality level.
    Links municipalities to election results and party ideology.

    Grain: One row per municipality per mandate term

    Term Mapping (election year -> mandate period):
    - 2000 election -> 2001-2004 mandate
    - 2004 election -> 2005-2008 mandate
    - 2008 election -> 2009-2012 mandate
    - 2012 election -> 2013-2016 mandate
    - 2016 election -> 2017-2020 mandate
    - 2020 election -> 2021-2024 mandate

    Key Features:
    - Political continuity tracking (same party reelection)
    - Ideological transition analysis (shift direction and magnitude)
    - Mandate sequence counting
    - Competition level classification
*/

-- Get election winners (prefer 2nd round if exists)
with eleicoes_raw as (
    select * from {{ ref('stg_eleicoes') }}
    where cargo = 'prefeito'
),

eleicoes_vencedores as (
    select
        id_municipio,
        ano as ano_eleicao,
        sigla_partido as partido_vencedor,
        votos as votos_vencedor,
        turno,
        row_number() over (
            partition by id_municipio, ano
            order by turno desc, votos desc
        ) as rn
    from eleicoes_raw
    where is_eleito = true
),

eleicoes as (
    select * from eleicoes_vencedores where rn = 1
),

-- Election-level statistics
eleicoes_stats as (
    select
        id_municipio,
        ano as ano_eleicao,
        max(turno) as turno_final,
        sum(votos) as total_votos,
        count(distinct sequencial_candidato) as total_candidatos,
        count(distinct sigla_partido) as total_partidos
    from eleicoes_raw
    group by id_municipio, ano
),

-- Build mandates with party info
mandatos_base as (
    select
        e.id_municipio,
        e.ano_eleicao,
        e.ano_eleicao + 1 as ano_inicio_mandato,
        e.ano_eleicao + 4 as ano_fim_mandato,
        concat(
            cast(e.ano_eleicao + 1 as varchar), '-',
            cast(e.ano_eleicao + 4 as varchar)
        ) as periodo_mandato,
        e.partido_vencedor,
        e.votos_vencedor,
        e.turno as turno_eleicao,
        es.total_votos,
        es.total_candidatos,
        es.total_partidos
    from eleicoes e
    inner join eleicoes_stats es
        on e.id_municipio = es.id_municipio
        and e.ano_eleicao = es.ano_eleicao
),

-- Add historical context (previous mandate)
mandatos_com_historico as (
    select
        m.*,
        lag(m.partido_vencedor) over (
            partition by m.id_municipio order by m.ano_eleicao
        ) as partido_mandato_anterior,
        lag(m.ano_eleicao) over (
            partition by m.id_municipio order by m.ano_eleicao
        ) as eleicao_anterior,
        row_number() over (
            partition by m.id_municipio order by m.ano_eleicao
        ) as sequencia_mandato_municipio
    from mandatos_base m
),

-- Join with party dimension for ideology
final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['m.id_municipio', 'm.ano_eleicao']) }} as sk_mandato,

        -- Foreign Keys
        {{ dbt_utils.generate_surrogate_key(['m.id_municipio']) }} as sk_municipio,
        p.sk_partido,
        p_ant.sk_partido as sk_partido_anterior,

        -- Natural Keys
        m.id_municipio,
        m.ano_eleicao,

        -- Mandate Period
        m.periodo_mandato,
        m.ano_inicio_mandato,
        m.ano_fim_mandato,
        m.sequencia_mandato_municipio,

        -- Electoral Results
        m.partido_vencedor,
        m.votos_vencedor,
        m.total_votos,
        m.turno_eleicao,
        m.total_candidatos,
        m.total_partidos,

        -- Vote Share
        case
            when m.total_votos > 0
            then round(cast(m.votos_vencedor as decimal(18,4)) / m.total_votos * 100, 2)
        end as percentual_vencedor,

        -- Competition Level
        case
            when m.total_candidatos = 1 then 'Sem Competicao'
            when m.total_candidatos = 2 then 'Bipolar'
            when m.total_candidatos <= 4 then 'Moderada'
            else 'Alta'
        end as nivel_competicao,

        -- Party Ideology (current mandate)
        p.nome_partido,
        p.espectro_ideologico,
        p.bloco_ideologico,
        p.score_ideologico,
        p.is_big_tent as is_partido_big_tent,
        p.intensidade_ideologica,

        -- Previous Mandate Info
        m.partido_mandato_anterior,
        p_ant.espectro_ideologico as espectro_anterior,
        p_ant.bloco_ideologico as bloco_anterior,
        p_ant.score_ideologico as score_anterior,

        -- Political Continuity
        case
            when m.partido_vencedor = m.partido_mandato_anterior
                and m.eleicao_anterior = m.ano_eleicao - 4
            then true
            else false
        end as is_continuidade_partidaria,

        case
            when p.bloco_ideologico = p_ant.bloco_ideologico then true
            else false
        end as is_continuidade_bloco,

        -- Ideological Change
        coalesce(p.score_ideologico, 0) - coalesce(p_ant.score_ideologico, 0) as delta_ideologico,

        case
            when p.score_ideologico is null or p_ant.score_ideologico is null
                then 'Primeiro Mandato ou Dados Incompletos'
            when p.score_ideologico - p_ant.score_ideologico >= 2
                then 'Guinada para Direita'
            when p.score_ideologico - p_ant.score_ideologico <= -2
                then 'Guinada para Esquerda'
            when p.score_ideologico - p_ant.score_ideologico >= 1
                then 'Mudanca Moderada para Direita'
            when p.score_ideologico - p_ant.score_ideologico <= -1
                then 'Mudanca Moderada para Esquerda'
            else 'Estabilidade Ideologica'
        end as tipo_transicao_ideologica,

        case
            when p.score_ideologico is null or p_ant.score_ideologico is null then null
            when p.score_ideologico > p_ant.score_ideologico then 'direita'
            when p.score_ideologico < p_ant.score_ideologico then 'esquerda'
            else 'neutro'
        end as direcao_mudanca_ideologica,

        -- Party Sequence (consecutive mandates for same party)
        sum(case when m.partido_vencedor = m.partido_mandato_anterior then 0 else 1 end) over (
            partition by m.id_municipio
            order by m.ano_eleicao
            rows unbounded preceding
        ) as sequencia_partido_municipio,

        -- Metadata
        current_timestamp as _loaded_at

    from mandatos_com_historico m

    -- Current party ideology
    left join {{ ref('dim_partido') }} p
        on m.partido_vencedor = p.sigla_partido

    -- Previous party ideology
    left join {{ ref('dim_partido') }} p_ant
        on m.partido_mandato_anterior = p_ant.sigla_partido
)

select * from final
order by id_municipio, ano_eleicao
