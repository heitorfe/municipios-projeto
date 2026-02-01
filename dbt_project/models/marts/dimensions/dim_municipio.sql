{{
    config(
        materialized='table',
        tags=['dimension', 'municipality', 'gold']
    )
}}

/*
    Municipality Dimension Table (Conformed Dimension)

    This is the central dimension in our star schema, linking all fact tables.
    It contains geographic, demographic, and social development information
    for all 5,570+ Brazilian municipalities.

    Grain: One row per municipality
*/

with municipios as (
    select * from {{ ref('stg_municipios') }}
),

-- Get latest population data (2024 or latest available)
populacao_latest as (
    select
        id_municipio,
        populacao,
        ano as ano_populacao
    from {{ ref('stg_populacao') }}
    where ano = (select max(ano) from {{ ref('stg_populacao') }})
),

-- Get latest IDHM data (2010 - latest census with IDHM)
idhm_latest as (
    select
        id_municipio,
        idhm,
        idhm_educacao,
        idhm_longevidade,
        idhm_renda,
        ivs,
        indice_gini,
        renda_per_capita,
        esperanca_vida
    from {{ ref('stg_idhm') }}
    where ano = 2010  -- Latest available census with IDHM
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['m.id_municipio_ibge']) }} as sk_municipio,

        -- Natural keys
        m.id_municipio_ibge,
        m.id_municipio_tse,

        -- Additional identifiers (for joining with other systems)
        m.id_municipio_6,
        m.id_municipio_rf,
        m.id_municipio_bcb,

        -- Descriptive attributes
        m.nome_municipio,
        m.sigla_uf,
        m.nome_uf,
        m.regiao,
        m.mesorregiao,
        m.microrregiao,

        -- Region hierarchies
        m.id_regiao_imediata,
        m.nome_regiao_imediata,
        m.id_regiao_intermediaria,
        m.nome_regiao_intermediaria,
        m.id_regiao_metropolitana,
        m.nome_regiao_metropolitana,

        -- Flags
        m.is_capital,
        m.is_amazonia_legal,

        -- Geographic
        m.centroide,
        m.ddd,

        -- Population (latest snapshot)
        p.populacao,
        p.ano_populacao,

        -- Population size classification
        case
            when p.populacao < 5000 then 'Micro (< 5k)'
            when p.populacao < 20000 then 'Pequeno (5k-20k)'
            when p.populacao < 100000 then 'Médio (20k-100k)'
            when p.populacao < 500000 then 'Grande (100k-500k)'
            else 'Metrópole (500k+)'
        end as porte_municipio,

        -- IDHM snapshot (2010)
        i.idhm as idhm_2010,
        i.idhm_educacao,
        i.idhm_longevidade,
        i.idhm_renda,
        case
            when i.idhm >= 0.800 then 'Muito Alto'
            when i.idhm >= 0.700 then 'Alto'
            when i.idhm >= 0.600 then 'Médio'
            when i.idhm >= 0.500 then 'Baixo'
            else 'Muito Baixo'
        end as faixa_idhm,

        -- Social Vulnerability Index
        i.ivs as ivs_2010,
        case
            when i.ivs <= 0.200 then 'Muito Baixa'
            when i.ivs <= 0.300 then 'Baixa'
            when i.ivs <= 0.400 then 'Média'
            when i.ivs <= 0.500 then 'Alta'
            else 'Muito Alta'
        end as faixa_ivs,

        -- Key indicators (for quick filtering)
        i.indice_gini as gini_2010,
        i.renda_per_capita as renda_per_capita_2010,
        i.esperanca_vida as esperanca_vida_2010,

        -- Metadata
        current_timestamp as _loaded_at

    from municipios m
    left join populacao_latest p on m.id_municipio_ibge = p.id_municipio
    left join idhm_latest i on m.id_municipio_ibge = i.id_municipio
)

select * from final
