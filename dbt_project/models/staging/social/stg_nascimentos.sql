{{
    config(
        materialized='view',
        tags=['staging', 'health']
    )
}}

/*
    Live Births Staging

    Data from SINASC (Sistema de Informacao sobre Nascidos Vivos).
    Used for calculating mortality rates.

    Source: br_ms_sinasc.municipio (pre-aggregated)
    Grain: One row per municipality per year
*/

select
    -- Keys
    cast(id_municipio as varchar) as id_municipio,
    cast(ano as integer) as ano,

    -- Birth counts
    cast(nascidos_vivos as integer) as nascidos_vivos

from read_parquet('../data/raw/nascimentos_municipio.parquet')
where ano >= 2000
