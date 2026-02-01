{{
    config(
        materialized='table',
        tags=['fact', 'social', 'gold']
    )
}}

/*
    Social Indicators Fact Table

    Periodic snapshot fact table containing socio-economic indicators
    for Brazilian municipalities across census years.

    Grain: One row per municipality per census year (2000, 2010)

    Note: Data from IPEA AVS (Atlas da Vulnerabilidade Social)
*/

with idhm as (
    select * from {{ ref('stg_idhm') }}
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

final as (
    select
        -- Foreign keys
        m.sk_municipio,
        c.sk_ano,

        -- Natural keys (for debugging)
        i.id_municipio,
        i.ano,

        -- IDHM Components
        i.idhm,
        i.idhm_educacao,
        i.idhm_longevidade,
        i.idhm_renda,

        -- Education sub-indices
        i.idhm_subescolaridade,
        i.idhm_subfrequencia,

        -- Demographics
        i.esperanca_vida,
        i.taxa_envelhecimento,

        -- Education metrics
        i.taxa_analfabetismo_18_mais,
        i.taxa_fundamental_completo,
        i.taxa_medio_completo,

        -- Income metrics
        i.renda_per_capita,
        i.indice_gini,
        i.taxa_pobreza,

        -- Vulnerability indices (IVS)
        i.ivs,
        i.ivs_infraestrutura,
        i.ivs_capital_humano,
        i.ivs_renda_trabalho,

        -- Labor market
        i.taxa_desemprego,
        i.taxa_informalidade,

        -- Infrastructure
        i.taxa_energia_eletrica,
        i.taxa_sem_saneamento,

        -- Calculated metrics
        round(i.idhm * 100, 2) as idhm_percentual,
        round(i.ivs * 100, 2) as ivs_percentual,

        -- Metadata
        current_timestamp as _loaded_at

    from idhm i
    inner join municipios m on i.id_municipio = m.id_municipio_ibge
    inner join calendario c on i.ano = c.ano
)

select * from final
