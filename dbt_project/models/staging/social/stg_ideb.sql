{{
    config(
        materialized='view',
        tags=['staging', 'education']
    )
}}

/*
    IDEB - Education Development Index Staging

    Biennial education quality indicator combining:
    - Flow rate (approval/retention)
    - Learning performance (standardized tests)

    Scale: 0-10 (higher is better)
    Target: Brazil aims for IDEB 6.0 by 2022 (OECD average)

    Source: br_inep_ideb.municipio
    Grain: One row per municipality per year per school level
*/

select
    -- Keys
    cast(id_municipio as varchar) as id_municipio,
    cast(ano as integer) as ano,

    -- School level (anos_iniciais, anos_finais, ensino_medio)
    rede as rede_ensino,  -- publica, privada, total

    -- IDEB components
    cast(ideb as decimal(4,2)) as ideb,
    cast(taxa_aprovacao as decimal(5,2)) as taxa_aprovacao,
    cast(indicador_rendimento as decimal(4,2)) as indicador_rendimento,
    cast(nota_saeb_matematica as decimal(5,2)) as nota_saeb_matematica,
    cast(nota_saeb_lingua_portuguesa as decimal(5,2)) as nota_saeb_lingua_portuguesa,
    cast(nota_saeb_media_padronizada as decimal(4,2)) as nota_saeb_media_padronizada,

    -- Targets and projections
    cast(projecao as decimal(4,2)) as meta_ideb,

    -- Achievement flag
    case
        when ideb >= projecao then true
        else false
    end as atingiu_meta,

    -- Quality classification
    case
        when ideb >= 6.0 then 'Desenvolvido'
        when ideb >= 5.0 then 'Medio-Alto'
        when ideb >= 4.0 then 'Medio'
        when ideb >= 3.0 then 'Medio-Baixo'
        else 'Baixo'
    end as faixa_ideb

from read_parquet('../data/raw/ideb_municipio.parquet')
where rede = 'publica'  -- Focus on public schools for policy analysis
  and anos_escolares in ('iniciais', 'finais')  -- Elementary school
