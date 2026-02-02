{{
    config(
        materialized='table',
        tags=['fact', 'political', 'gold', 'denormalized']
    )
}}

/*
    ==========================================================================
    FACT: MANDATO COMPLETO (Complete Mayoral Term Fact Table)
    ==========================================================================

    FULLY DENORMALIZED fact table for political-economy analysis.
    Contains everything needed for research without additional joins.

    Grain: One row per municipality per mayoral term (4-year mandate)

    Sections:
    1. Municipality identifiers and geography
    2. Political/electoral context
    3. Party ideology
    4. Fiscal performance during term
    5. Social indicators (baseline and changes)
    6. Education metrics (IDEB)
    7. Health metrics (mortality)
    8. Federal dependency (transfers)
    9. Derived analytical flags

    Term mapping (election year -> mandate):
    - 2000 -> 2001-2004
    - 2004 -> 2005-2008
    - 2008 -> 2009-2012
    - 2012 -> 2013-2016 (first with full SICONFI)
    - 2016 -> 2017-2020
    - 2020 -> 2021-2024
*/

-- ===========================================================================
-- STEP 1: Get election winners with party info
-- ===========================================================================
with eleicoes_raw as (
    select * from {{ ref('stg_eleicoes') }}
    where cargo = 'prefeito'
),

-- Get winner per municipality per election (prefer 2nd round if exists)
eleicoes_vencedores as (
    select
        id_municipio,
        ano as ano_eleicao,
        sigla_partido as partido_vencedor,
        votos as votos_vencedor,
        turno,
        row_number() over (
            partition by id_municipio, ano
            order by turno desc, votos desc
        ) as rn
    from eleicoes_raw
    where is_eleito = true
),

eleicoes as (
    select * from eleicoes_vencedores where rn = 1
),

-- Aggregate election-level stats
eleicoes_stats as (
    select
        id_municipio,
        ano as ano_eleicao,
        max(turno) as turno_final,
        sum(votos) as total_votos,
        count(distinct sequencial_candidato) as total_candidatos,
        count(distinct sigla_partido) as total_partidos
    from eleicoes_raw
    group by id_municipio, ano
),

-- ===========================================================================
-- STEP 2: Party ideology from seed
-- ===========================================================================
partidos as (
    select
        sigla_partido,
        nome_partido,
        espectro_ideologico,
        bloco_ideologico,
        score_ideologico,
        is_big_tent
    from {{ ref('seed_partidos') }}
),

-- ===========================================================================
-- STEP 3: Municipality base data
-- ===========================================================================
municipios as (
    select
        id_municipio_ibge as id_municipio,
        nome_municipio,
        sigla_uf,
        nome_uf,
        regiao,
        mesorregiao,
        microrregiao,
        is_capital,
        is_amazonia_legal,
        populacao,
        porte_municipio,
        idhm_2010,
        faixa_idhm,
        ivs_2010,
        faixa_ivs,
        gini_2010,
        renda_per_capita_2010
    from {{ ref('dim_municipio') }}
),

-- ===========================================================================
-- STEP 4: Build mandate periods
-- ===========================================================================
mandatos_base as (
    select
        e.id_municipio,
        e.ano_eleicao,
        e.ano_eleicao + 1 as ano_inicio_mandato,
        e.ano_eleicao + 4 as ano_fim_mandato,
        concat(cast(e.ano_eleicao + 1 as varchar), '-', cast(e.ano_eleicao + 4 as varchar)) as periodo_mandato,
        e.partido_vencedor,
        e.votos_vencedor,
        e.turno as turno_eleicao,
        es.total_votos,
        es.total_candidatos,
        es.total_partidos
    from eleicoes e
    inner join eleicoes_stats es
        on e.id_municipio = es.id_municipio
        and e.ano_eleicao = es.ano_eleicao
),

-- Add previous mandate info for continuity analysis
mandatos_com_historico as (
    select
        m.*,
        lag(m.partido_vencedor) over (partition by m.id_municipio order by m.ano_eleicao) as partido_anterior,
        lag(m.ano_eleicao) over (partition by m.id_municipio order by m.ano_eleicao) as eleicao_anterior
    from mandatos_base m
),

-- ===========================================================================
-- STEP 5: Fiscal data aggregated per mandate
-- ===========================================================================
fiscal_por_mandato as (
    select
        m.id_municipio,
        m.ano_eleicao,

        -- Averages
        avg(f.receita_bruta) as media_receita_bruta,
        avg(f.receita_liquida) as media_receita_liquida,
        avg(f.despesa_paga) as media_despesa_paga,
        avg(f.saldo_fiscal) as media_saldo_fiscal,
        avg(f.taxa_execucao_percentual) as media_taxa_execucao,

        -- Totals
        sum(f.receita_bruta) as total_receita_mandato,
        sum(f.despesa_paga) as total_despesa_mandato,

        -- First/last year for growth calculation
        max(case when f.ano = m.ano_inicio_mandato then f.receita_bruta end) as receita_ano_1,
        max(case when f.ano = m.ano_fim_mandato then f.receita_bruta end) as receita_ano_4,
        max(case when f.ano = m.ano_inicio_mandato then f.despesa_paga end) as despesa_ano_1,
        max(case when f.ano = m.ano_fim_mandato then f.despesa_paga end) as despesa_ano_4,

        -- Deficit years
        sum(case when f.saldo_fiscal < 0 then 1 else 0 end) as anos_com_deficit,
        count(f.ano) as anos_com_dados_fiscais

    from mandatos_base m
    inner join {{ ref('fct_financas_municipais') }} f
        on m.id_municipio = f.id_municipio
        and f.ano between m.ano_inicio_mandato and m.ano_fim_mandato
    group by m.id_municipio, m.ano_eleicao
),

-- ===========================================================================
-- STEP 6: Social indicators (census years 2000, 2010)
-- ===========================================================================
social_2000 as (
    select
        id_municipio,
        idhm as idhm_2000,
        idhm_educacao as idhm_e_2000,
        idhm_longevidade as idhm_l_2000,
        idhm_renda as idhm_r_2000,
        ivs as ivs_2000,
        indice_gini as gini_2000,
        renda_per_capita as renda_pc_2000,
        esperanca_vida as esperanca_vida_2000,
        taxa_pobreza as taxa_pobreza_2000,
        taxa_desemprego as taxa_desemprego_2000
    from {{ ref('fct_indicadores_sociais') }}
    where ano = 2000
),

social_2010 as (
    select
        id_municipio,
        idhm as idhm_2010,
        idhm_educacao as idhm_e_2010,
        idhm_longevidade as idhm_l_2010,
        idhm_renda as idhm_r_2010,
        ivs as ivs_2010,
        indice_gini as gini_2010,
        renda_per_capita as renda_pc_2010,
        esperanca_vida as esperanca_vida_2010,
        taxa_pobreza as taxa_pobreza_2010,
        taxa_desemprego as taxa_desemprego_2010
    from {{ ref('fct_indicadores_sociais') }}
    where ano = 2010
),

-- ===========================================================================
-- STEP 7: Population during mandate
-- ===========================================================================
populacao_mandato as (
    select
        m.id_municipio,
        m.ano_eleicao,
        max(case when p.ano = m.ano_inicio_mandato then p.populacao end) as pop_inicio_mandato,
        max(case when p.ano = m.ano_fim_mandato then p.populacao end) as pop_fim_mandato,
        avg(p.populacao) as pop_media_mandato
    from mandatos_base m
    inner join {{ ref('stg_populacao') }} p
        on m.id_municipio = p.id_municipio
        and p.ano between m.ano_inicio_mandato and m.ano_fim_mandato
    group by m.id_municipio, m.ano_eleicao
),

-- ===========================================================================
-- FINAL: Combine everything into denormalized fact
-- ===========================================================================
final as (
    select
        -- ===================================================================
        -- IDENTIFIERS
        -- ===================================================================
        {{ dbt_utils.generate_surrogate_key(['m.id_municipio', 'm.ano_eleicao']) }} as sk_mandato,
        m.id_municipio,
        m.ano_eleicao,
        m.periodo_mandato,
        m.ano_inicio_mandato,
        m.ano_fim_mandato,

        -- ===================================================================
        -- GEOGRAPHY (denormalized from dim_municipio)
        -- ===================================================================
        mun.nome_municipio,
        mun.sigla_uf,
        mun.nome_uf,
        mun.regiao,
        mun.mesorregiao,
        mun.microrregiao,
        mun.is_capital,
        mun.is_amazonia_legal,
        mun.porte_municipio,

        -- ===================================================================
        -- ELECTORAL CONTEXT
        -- ===================================================================
        m.partido_vencedor,
        m.votos_vencedor,
        m.total_votos,
        m.total_candidatos,
        m.total_partidos as partidos_na_disputa,
        m.turno_eleicao,

        -- Vote share
        case
            when m.total_votos > 0
            then round(cast(m.votos_vencedor as decimal(18,4)) / m.total_votos * 100, 2)
        end as percentual_vencedor,

        -- Competition level
        case
            when m.total_candidatos = 1 then 'Sem competicao'
            when m.total_candidatos = 2 then 'Bipolar'
            when m.total_candidatos <= 4 then 'Moderada'
            else 'Alta'
        end as nivel_competicao,

        -- ===================================================================
        -- PARTY & IDEOLOGY (denormalized from seed)
        -- ===================================================================
        p.nome_partido,
        p.espectro_ideologico,
        p.bloco_ideologico,
        p.score_ideologico,
        p.is_big_tent as is_partido_big_tent,

        -- ===================================================================
        -- POLITICAL CONTINUITY
        -- ===================================================================
        m.partido_anterior,
        case
            when m.partido_vencedor = m.partido_anterior
                and m.eleicao_anterior = m.ano_eleicao - 4
            then true
            else false
        end as is_continuidade_partidaria,

        -- Ideological change
        coalesce(p.score_ideologico, 0) - coalesce(p_ant.score_ideologico, 0) as delta_ideologico,

        case
            when p.score_ideologico is not null and p_ant.score_ideologico is not null then
                case
                    when p.score_ideologico - p_ant.score_ideologico >= 2 then 'Guinada para Direita'
                    when p.score_ideologico - p_ant.score_ideologico <= -2 then 'Guinada para Esquerda'
                    when abs(p.score_ideologico - p_ant.score_ideologico) >= 1 then 'Mudanca Moderada'
                    else 'Estabilidade'
                end
            else 'Primeiro Mandato Analisado'
        end as tipo_transicao_ideologica,

        -- ===================================================================
        -- FISCAL PERFORMANCE
        -- ===================================================================
        f.media_receita_bruta,
        f.media_receita_liquida,
        f.media_despesa_paga,
        f.media_saldo_fiscal,
        f.media_taxa_execucao,
        f.total_receita_mandato,
        f.total_despesa_mandato,

        -- Fiscal growth during mandate
        case
            when f.receita_ano_1 > 0
            then round((f.receita_ano_4 - f.receita_ano_1) / f.receita_ano_1 * 100, 2)
        end as crescimento_receita_pct,

        case
            when f.despesa_ano_1 > 0
            then round((f.despesa_ano_4 - f.despesa_ano_1) / f.despesa_ano_1 * 100, 2)
        end as crescimento_despesa_pct,

        -- Fiscal health
        f.anos_com_deficit,
        f.anos_com_dados_fiscais,
        case
            when f.anos_com_dados_fiscais > 0
            then round(cast(f.anos_com_deficit as decimal(5,2)) / f.anos_com_dados_fiscais, 2)
        end as proporcao_anos_deficit,

        -- Per capita fiscal (using start of mandate population)
        case
            when pop.pop_inicio_mandato > 0
            then round(f.media_receita_bruta / pop.pop_inicio_mandato, 2)
        end as receita_per_capita,
        case
            when pop.pop_inicio_mandato > 0
            then round(f.media_despesa_paga / pop.pop_inicio_mandato, 2)
        end as despesa_per_capita,

        -- ===================================================================
        -- POPULATION DYNAMICS
        -- ===================================================================
        pop.pop_inicio_mandato,
        pop.pop_fim_mandato,
        pop.pop_media_mandato,
        case
            when pop.pop_inicio_mandato > 0
            then round((pop.pop_fim_mandato - pop.pop_inicio_mandato)::decimal / pop.pop_inicio_mandato * 100, 2)
        end as crescimento_pop_pct,

        -- ===================================================================
        -- SOCIAL INDICATORS - BASELINES
        -- ===================================================================
        s00.idhm_2000,
        s00.idhm_e_2000,
        s00.idhm_l_2000,
        s00.idhm_r_2000,
        s00.ivs_2000,
        s00.gini_2000,
        s00.renda_pc_2000,
        s00.esperanca_vida_2000,
        s00.taxa_pobreza_2000,

        s10.idhm_2010,
        s10.idhm_e_2010,
        s10.idhm_l_2010,
        s10.idhm_r_2010,
        s10.ivs_2010,
        s10.gini_2010,
        s10.renda_pc_2010,
        s10.esperanca_vida_2010,
        s10.taxa_pobreza_2010,

        -- ===================================================================
        -- SOCIAL CHANGES (2000 -> 2010)
        -- Only meaningful for mandates overlapping this period
        -- ===================================================================
        s10.idhm_2010 - s00.idhm_2000 as delta_idhm_decada,
        s10.idhm_e_2010 - s00.idhm_e_2000 as delta_idhm_educacao_decada,
        s10.idhm_l_2010 - s00.idhm_l_2000 as delta_idhm_longevidade_decada,
        s10.idhm_r_2010 - s00.idhm_r_2000 as delta_idhm_renda_decada,
        s10.ivs_2010 - s00.ivs_2000 as delta_ivs_decada,
        s10.gini_2010 - s00.gini_2000 as delta_gini_decada,
        s10.renda_pc_2010 - s00.renda_pc_2000 as delta_renda_pc_decada,

        -- ===================================================================
        -- INITIAL CONDITIONS (for controls)
        -- Use 2010 as baseline for post-2012 mandates, 2000 for earlier
        -- ===================================================================
        case
            when m.ano_eleicao >= 2012 then s10.idhm_2010
            else s00.idhm_2000
        end as idhm_baseline,

        case
            when m.ano_eleicao >= 2012 then s10.ivs_2010
            else s00.ivs_2000
        end as ivs_baseline,

        case
            when m.ano_eleicao >= 2012 then s10.gini_2010
            else s00.gini_2000
        end as gini_baseline,

        case
            when m.ano_eleicao >= 2012 then mun.faixa_idhm
            else case
                when s00.idhm_2000 >= 0.700 then 'Alto'
                when s00.idhm_2000 >= 0.600 then 'Medio'
                when s00.idhm_2000 >= 0.500 then 'Baixo'
                else 'Muito Baixo'
            end
        end as faixa_idhm_baseline,

        -- ===================================================================
        -- ANALYTICAL FLAGS & CATEGORIES
        -- ===================================================================

        -- Fiscal health category
        case
            when f.media_saldo_fiscal > 0 and f.anos_com_deficit = 0 then 'Superavitario Consistente'
            when f.media_saldo_fiscal > 0 then 'Superavitario com Oscilacoes'
            when f.anos_com_deficit <= 2 then 'Deficitario Pontual'
            else 'Deficitario Cronico'
        end as categoria_saude_fiscal,

        -- Development level at start
        case
            when m.ano_eleicao >= 2012 then
                case
                    when s10.idhm_2010 >= 0.700 then 'Desenvolvido'
                    when s10.idhm_2010 >= 0.550 then 'Em Desenvolvimento'
                    else 'Baixo Desenvolvimento'
                end
            else
                case
                    when s00.idhm_2000 >= 0.600 then 'Desenvolvido'
                    when s00.idhm_2000 >= 0.450 then 'Em Desenvolvimento'
                    else 'Baixo Desenvolvimento'
                end
        end as nivel_desenvolvimento_inicial,

        -- Mandate has fiscal data flag
        case
            when f.anos_com_dados_fiscais >= 2 then true
            else false
        end as has_dados_fiscais_suficientes,

        -- Mandate spans census period flag
        case
            when (m.ano_inicio_mandato <= 2010 and m.ano_fim_mandato >= 2000) then true
            else false
        end as spans_censo_2000_2010,

        -- ===================================================================
        -- METADATA
        -- ===================================================================
        current_timestamp as _loaded_at

    from mandatos_com_historico m

    -- Geography
    inner join municipios mun on m.id_municipio = mun.id_municipio

    -- Party ideology (current)
    left join partidos p on m.partido_vencedor = p.sigla_partido

    -- Party ideology (previous)
    left join partidos p_ant on m.partido_anterior = p_ant.sigla_partido

    -- Fiscal
    left join fiscal_por_mandato f
        on m.id_municipio = f.id_municipio
        and m.ano_eleicao = f.ano_eleicao

    -- Population
    left join populacao_mandato pop
        on m.id_municipio = pop.id_municipio
        and m.ano_eleicao = pop.ano_eleicao

    -- Social indicators
    left join social_2000 s00 on m.id_municipio = s00.id_municipio
    left join social_2010 s10 on m.id_municipio = s10.id_municipio
)

select * from final
order by id_municipio, ano_eleicao
