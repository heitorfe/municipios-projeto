{# Disabled: BigQuery access permissions needed for this dataset #}
{{
    config(
        materialized='view',
        tags=['staging', 'social'],
        enabled=false
    )
}}

/*
    Bolsa Familia Social Transfer Staging

    Brazil's main conditional cash transfer program.
    Provides monthly stipends to low-income families
    conditional on school attendance and health checkups.

    Source: br_mds_bolsa_familia.municipio
    Grain: One row per municipality per year (or month)
*/

select
    -- Keys
    cast(id_municipio as varchar) as id_municipio,
    cast(ano as integer) as ano,

    -- Beneficiary counts
    cast(quantidade_beneficiarios_bolsa_familia as integer) as familias_beneficiarias,

    -- Transfer amounts
    cast(valor_total_bolsa_familia as decimal(18,2)) as valor_total_transferido

from {{ source('raw', 'bolsa_familia_municipio') }}
where ano >= 2004
