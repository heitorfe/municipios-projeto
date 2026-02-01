{{
    config(
        materialized='table',
        tags=['dimension', 'municipality', 'gold']
    )
}}

/*
    Municipality Dimension Table (Conformed Dimension)

    This is the central dimension in our star schema, linking all fact tables.
    It contains geographic and administrative information for all 5,570+
    Brazilian municipalities.

    Grain: One row per municipality

    Note: Population and IDHM data can be added later when those files are extracted.
*/

with municipios as (
    select * from {{ ref('stg_municipios') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['id_municipio_ibge']) }} as sk_municipio,

        -- Natural keys
        id_municipio_ibge,
        id_municipio_tse,

        -- Additional identifiers (for joining with other systems)
        id_municipio_6,
        id_municipio_rf,
        id_municipio_bcb,

        -- Descriptive attributes
        nome_municipio,
        sigla_uf,
        nome_uf,
        regiao,
        mesorregiao,
        microrregiao,

        -- Region hierarchies
        id_regiao_imediata,
        nome_regiao_imediata,
        id_regiao_intermediaria,
        nome_regiao_intermediaria,
        id_regiao_metropolitana,
        nome_regiao_metropolitana,

        -- Flags
        is_capital,
        is_amazonia_legal,

        -- Geographic
        centroide,

        -- Phone area code (useful for regional analysis)
        ddd,

        -- Metadata
        current_timestamp as _loaded_at

    from municipios
)

select * from final
