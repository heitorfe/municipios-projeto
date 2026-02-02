{# Disabled: br_ms_sim query needs column name fix #}
{{
    config(
        materialized='view',
        tags=['staging', 'health'],
        enabled=false
    )
}}

/*
    Mortality Indicators Staging

    Aggregated mortality data from SIM (Sistema de Informacao sobre Mortalidade).
    Pre-aggregated in extraction query to reduce data volume.

    Key metrics:
    - Infant mortality (< 1 year)
    - Child mortality (< 5 years)
    - Total deaths by cause

    Source: br_ms_sim.municipio_causa (pre-aggregated)
    Grain: One row per municipality per year per cause category
*/

select
    -- Keys
    cast(id_municipio as varchar) as id_municipio,
    cast(ano as integer) as ano,

    -- Cause of death (ICD-10 chapter)
    causa_basica_categoria,

    -- Death counts
    cast(obitos as integer) as total_obitos,
    cast(obitos_infantis as integer) as obitos_infantis,  -- < 1 year
    cast(obitos_menores_5 as integer) as obitos_menores_5  -- < 5 years

from {{ source('raw', 'mortalidade_municipio') }}
where ano >= 2000
