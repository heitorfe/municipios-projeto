{{
    config(
        materialized='view',
        tags=['staging', 'idhm', 'social']
    )
}}

/*
    Staging model for Human Development Index (IDHM).

    This model cleans and standardizes the IDHM data from IPEA AVS (Atlas da Vulnerabilidade Social).
    IDHM is calculated for census years: 2000, 2010.

    Source: Base dos Dados - br_ipea_avs.municipio

    Note: The source data has UDH (Human Development Units) which are sub-municipal areas.
    This model aggregates to municipality level by taking the mean across UDHs.
*/

with source as (
    select * from read_parquet('../data/raw/idhm.parquet')
),

-- Filter to total population (not broken down by demographics)
filtered as (
    select *
    from source
    where
        raca_cor = 'total'
        and sexo = 'total'
        and localizacao = 'total'
        and id_municipio is not null
        and ano is not null
        and idhm is not null
),

-- Aggregate to municipality level (average across UDHs)
aggregated as (
    select
        cast(id_municipio as varchar(7)) as id_municipio,
        cast(ano as integer) as ano,
        any_value(upper(sigla_uf)) as sigla_uf,

        -- IDHM components (average across UDHs)
        cast(avg(idhm) as decimal(5, 4)) as idhm,
        cast(avg(idhm_e) as decimal(5, 4)) as idhm_educacao,
        cast(avg(idhm_l) as decimal(5, 4)) as idhm_longevidade,
        cast(avg(idhm_r) as decimal(5, 4)) as idhm_renda,

        -- Education sub-indices
        cast(avg(idhm_subescolaridade) as decimal(5, 4)) as idhm_subescolaridade,
        cast(avg(idhm_subfrequencia) as decimal(5, 4)) as idhm_subfrequencia,

        -- Demographics
        cast(avg(expectativa_vida) as decimal(5, 2)) as esperanca_vida,
        cast(avg(taxa_envelhecimento) as decimal(5, 2)) as taxa_envelhecimento,

        -- Education indicators
        cast(avg(proporcao_analfabetismo_18_mais) as decimal(5, 2)) as taxa_analfabetismo_18_mais,
        cast(avg(proporcao_fundamental_completo_18_mais) as decimal(5, 2)) as taxa_fundamental_completo,
        cast(avg(proporcao_medio_completo_18_20) as decimal(5, 2)) as taxa_medio_completo,

        -- Income indicators
        cast(avg(renda_per_capita) as decimal(12, 2)) as renda_per_capita,
        cast(avg(indice_gini) as decimal(5, 4)) as indice_gini,
        cast(avg(proporcao_vulneravel) as decimal(5, 2)) as taxa_pobreza,

        -- Vulnerability indicators (IVS)
        cast(avg(ivs) as decimal(5, 4)) as ivs,
        cast(avg(ivs_infraestrutura_urbana) as decimal(5, 4)) as ivs_infraestrutura,
        cast(avg(ivs_capital_humano) as decimal(5, 4)) as ivs_capital_humano,
        cast(avg(ivs_renda_trabalho) as decimal(5, 4)) as ivs_renda_trabalho,

        -- Labor market
        cast(avg(proporcao_desocupado_18_mais) as decimal(5, 2)) as taxa_desemprego,
        cast(avg(proporcao_ocupados_informal_18_mais) as decimal(5, 2)) as taxa_informalidade,

        -- Infrastructure
        cast(avg(propocao_energia_eletrica) as decimal(5, 2)) as taxa_energia_eletrica,
        cast(avg(proporcao_sem_agua_esgoto) as decimal(5, 2)) as taxa_sem_saneamento,

        -- Count of UDHs for reference
        count(*) as udh_count,

        -- Metadata
        current_timestamp as _loaded_at

    from filtered
    group by
        cast(id_municipio as varchar(7)),
        cast(ano as integer)
)

select * from aggregated
