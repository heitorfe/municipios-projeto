{{
    config(
        materialized='view',
        tags=['staging', 'financial', 'transfers']
    )
}}

/*
    Staging model for federal transfer classification.

    Extracts and classifies federal transfers (FPM, FUNDEB, SUS) from SICONFI
    revenue accounts. Uses pattern matching on account descriptions.

    Source: Base dos Dados - br_me_siconfi.municipio_receitas_orcamentarias
    Grain: One row per municipality per year per transfer type

    Transfer Types:
    - FPM: Fundo de Participação dos Municípios (constitutional revenue sharing)
    - FUNDEB: Education fund transfers and União complementation
    - SUS: Health system transfers (various programs)
    - OUTRAS_FEDERAIS: Other federal constitutional/legal transfers

    Note: Classification based on conta_bd and id_conta_bd patterns.
    Only includes realized revenues (Receitas Brutas Realizadas).
*/

with source as (
    select * from read_parquet('../data/raw/municipio_receitas_orcamentarias.parquet')
),

-- Classify transfer types based on account descriptions and codes
classified as (
    select
        -- Keys
        cast(id_municipio as varchar(7)) as id_municipio,
        cast(ano as integer) as ano,
        upper(sigla_uf) as sigla_uf,

        -- Transfer type classification
        -- Priority order matters: more specific patterns first
        case
            -- FPM: Fundo de Participação dos Municípios
            when conta_bd ilike '%Fundo de Participa%o dos Munic%pios%FPM%'
                or conta_bd ilike '%Cota-Parte do Fundo de Participa%o dos Munic%pios%'
                or id_conta_bd like '1.1.7.1.1.51%'
            then 'FPM'

            -- FUNDEB: Education fund transfers
            when conta_bd ilike '%FUNDEB%'
                or conta_bd ilike '%Fundo de Manuten%o e Desenvolvimento da Educa%o B%sica%'
                or conta_bd ilike '%Fundo de Manuten%o e Desenvolvimento do Ensino Fundamental%'
                or id_conta_bd like '1.1.7.2.4%'
            then 'FUNDEB'

            -- SUS: Health system transfers
            when conta_bd ilike '%Sistema%nico de Sa%de%SUS%'
                or conta_bd ilike '%Recursos do SUS%'
                or conta_bd ilike '%Transfer%ncia%SUS%'
                or id_conta_bd like '1.1.7.1.3.3%'
            then 'SUS'

            -- ITR: Rural Land Tax (federal share to municipalities)
            when conta_bd ilike '%ITR%'
                or conta_bd ilike '%Imposto sobre a Propriedade Territorial Rural%'
                or id_conta_bd like '1.1.7.1.1.53%'
            then 'ITR'

            -- IPI-Exportação: Export compensation
            when conta_bd ilike '%IPI%Export%'
                or conta_bd ilike '%Ressarcimento%Lei Kandir%'
                or id_conta_bd like '1.1.7.1.1.52%'
            then 'IPI_EXPORTACAO'

            -- Other federal transfers (broader category)
            when conta_bd ilike '%Transfer%ncias da Uni%o%'
                or conta_bd ilike '%Transfer%ncias%Governo Federal%'
                or id_conta_bd like '1.1.7.1%'
                or id_conta_bd like '1.1.7.2%'
            then 'OUTRAS_FEDERAIS'

            else null
        end as tipo_transferencia,

        -- Raw values
        cast(valor as decimal(18, 2)) as valor,
        estagio_bd,
        id_conta_bd,
        conta_bd

    from source
    where
        -- Only include realized revenues (not estimates or deductions)
        (
            estagio_bd ilike '%Brutas%Realizadas%'
            or estagio_bd is null  -- Some records may lack stage classification
        )
        -- Basic data quality filters
        and id_municipio is not null
        and ano is not null
        and valor is not null
        and valor > 0
),

-- Aggregate by municipality, year, and transfer type
aggregated as (
    select
        id_municipio,
        ano,
        any_value(sigla_uf) as sigla_uf,
        tipo_transferencia,
        sum(valor) as valor_total,
        count(*) as num_contas  -- Number of account records aggregated

    from classified
    where tipo_transferencia is not null
    group by id_municipio, ano, tipo_transferencia
)

select
    *,
    current_timestamp as _loaded_at
from aggregated
