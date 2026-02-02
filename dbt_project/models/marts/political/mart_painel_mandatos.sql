{{
    config(
        materialized='table',
        tags=['mart', 'analytics', 'panel']
    )
}}

/*
    ==========================================================================
    MART: PAINEL MANDATOS (Panel Data for Econometric Analysis)
    ==========================================================================

    Ready-to-use panel dataset for:
    - Fixed effects regression (municipality and time FE)
    - Difference-in-differences
    - Panel data models (Stata, R, Python statsmodels)

    Grain: One row per municipality per mandate
    Panel ID: id_municipio
    Time ID: ano_eleicao

    Optimized columns:
    - All variables are numeric or categorical (no complex objects)
    - Missing values are explicit (null)
    - Ready for export to .csv/.dta/.parquet

    Usage in Stata:
        xtset id_municipio ano_eleicao
        xtreg saldo_fiscal score_ideologico i.regiao, fe

    Usage in Python:
        from linearmodels import PanelOLS
        mod = PanelOLS.from_formula('saldo_fiscal ~ score_ideologico + EntityEffects', data=df)
*/

select
    -- =========================================================================
    -- PANEL IDENTIFIERS (required for panel data)
    -- =========================================================================
    id_municipio,                    -- Cross-sectional unit
    ano_eleicao,                     -- Time unit
    periodo_mandato,                 -- Human-readable period

    -- =========================================================================
    -- TREATMENT VARIABLES (political characteristics)
    -- =========================================================================
    partido_vencedor,                -- Party abbreviation
    bloco_ideologico,                -- Left/Center/Right (categorical)
    score_ideologico,                -- Numeric ideology (-2 to +2)
    is_partido_big_tent,             -- Big tent party flag

    is_continuidade_partidaria,      -- Same party as previous term
    delta_ideologico,                -- Change in ideology score
    tipo_transicao_ideologica,       -- Type of ideological transition

    -- Electoral context
    percentual_vencedor,             -- Vote share
    total_candidatos,                -- Number of candidates
    nivel_competicao,                -- Competition level (categorical)

    -- =========================================================================
    -- OUTCOME VARIABLES - FISCAL
    -- =========================================================================
    media_saldo_fiscal as saldo_fiscal,
    media_taxa_execucao as taxa_execucao,
    media_receita_bruta as receita_bruta,
    media_despesa_paga as despesa_paga,
    receita_per_capita,
    despesa_per_capita,

    crescimento_receita_pct,
    crescimento_despesa_pct,
    proporcao_anos_deficit,

    categoria_saude_fiscal,          -- Fiscal health category

    -- =========================================================================
    -- OUTCOME VARIABLES - SOCIAL
    -- =========================================================================
    -- Inter-census changes (2000->2010)
    delta_idhm_decada,
    delta_idhm_educacao_decada,
    delta_idhm_renda_decada,
    delta_ivs_decada,
    delta_gini_decada,
    delta_renda_pc_decada,

    -- Population dynamics
    crescimento_pop_pct,

    -- =========================================================================
    -- CONTROL VARIABLES - TIME-INVARIANT
    -- =========================================================================
    regiao,                          -- Geographic region
    sigla_uf,                        -- State
    nome_municipio,
    porte_municipio,                 -- Population size class
    is_capital,
    is_amazonia_legal,

    -- Initial conditions (for controlling baseline)
    idhm_baseline,
    ivs_baseline,
    gini_baseline,
    faixa_idhm_baseline,
    nivel_desenvolvimento_inicial,

    pop_inicio_mandato as pop_baseline,

    -- =========================================================================
    -- FIXED EFFECT GROUPS (for FE regression)
    -- =========================================================================
    id_municipio as fe_municipio,    -- Municipality FE
    ano_eleicao as fe_tempo,         -- Time FE
    sigla_uf as fe_estado,           -- State FE
    regiao as fe_regiao,             -- Region FE
    bloco_ideologico as fe_ideologia -- Ideology FE

    -- =========================================================================
    -- DATA QUALITY FLAGS
    -- =========================================================================
    , has_dados_fiscais_suficientes
    , spans_censo_2000_2010
    , anos_com_dados_fiscais

from {{ ref('fct_mandato_completo') }}

-- Filter for quality
where has_dados_fiscais_suficientes = true
  or ano_eleicao < 2012  -- Keep earlier mandates even without fiscal data

order by id_municipio, ano_eleicao
