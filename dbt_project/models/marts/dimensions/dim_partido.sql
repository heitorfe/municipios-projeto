{{
    config(
        materialized='table',
        tags=['dimension', 'political', 'gold']
    )
}}

/*
    ==========================================================================
    DIMENSION: PARTIDO (Political Party)
    ==========================================================================

    Conformed dimension for Brazilian political parties with ideology classification.
    Based on academic research from Power & Zucco BPCS surveys (2012, 2019).

    Grain: One row per political party

    Key Features:
    - Surrogate key for dimensional modeling
    - Ideology spectrum (5-point categorical)
    - Ideology block (3-point simplified: left/center/right)
    - Numeric ideology score (-2 to +2) for regression analysis
    - Big tent flag for pragmatic/catch-all parties
    - Historical status (active, extinct, merged)

    Sources:
    - TSE (Tribunal Superior Eleitoral) - Official party registry
    - Power & Zucco (2009, 2012) - Legislative surveys
    - Tarouco & Madeira (2013) - Party system classification
    - Bolognesi, Ribeiro & Codato (2023) - Updated classifications
*/

with source as (
    select * from {{ ref('seed_partidos') }}
),

enriched as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['sigla_partido']) }} as sk_partido,

        -- Natural Keys
        numero_partido,
        sigla_partido,

        -- Descriptive Attributes
        nome_partido,

        -- Ideology Classification
        espectro_ideologico,
        bloco_ideologico,
        score_ideologico,

        -- Party Characteristics
        is_big_tent,
        ano_fundacao,
        ano_extincao,

        -- Derived Attributes
        case
            when ano_extincao is null then true
            else false
        end as is_ativo,

        case
            when ano_extincao is not null then 'Extinto/Fundido'
            when ano_fundacao >= 2015 then 'Partido Novo'
            when ano_fundacao >= 2000 then 'Pos-Redemocratizacao Recente'
            when ano_fundacao >= 1985 then 'Redemocratizacao'
            else 'Historico'
        end as era_fundacao,

        -- Ideology Intensity (distance from center)
        abs(score_ideologico) as intensidade_ideologica,

        case
            when abs(score_ideologico) >= 2 then 'Forte'
            when abs(score_ideologico) >= 1 then 'Moderado'
            else 'Fraco/Pragmatico'
        end as faixa_intensidade_ideologica,

        -- Notes
        notas,

        -- Metadata
        current_timestamp as _loaded_at

    from source
)

select * from enriched
order by numero_partido
