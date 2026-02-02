{{
    config(
        materialized='table',
        tags=['mart', 'analytics', 'causal']
    )
}}

/*
    ==========================================================================
    MART: DIFF-IN-DIFF (Difference-in-Differences Analysis)
    ==========================================================================

    Structured dataset for causal inference using:
    - Difference-in-differences (DiD)
    - Event studies
    - Before-after comparisons

    Design:
    - Treatment: Political change (party change, ideological shift)
    - Control: Political continuity
    - Pre-period: Previous mandate outcomes
    - Post-period: Current mandate outcomes

    Grain: One row per municipality per mandate (with lag variables)

    Example research questions:
    - "Does party change affect fiscal policy?" (DiD)
    - "What happens after a left-to-right ideological shift?" (Event study)
    - "Are outcomes different under continuity vs change?" (Before-after)
*/

with mandatos as (
    select * from {{ ref('fct_mandato_completo') }}
),

-- Add lagged variables from previous mandate
with_lags as (
    select
        m.*,

        -- =====================================================================
        -- LAGGED OUTCOMES (pre-treatment period)
        -- =====================================================================
        lag(m.media_saldo_fiscal) over w as saldo_fiscal_pre,
        lag(m.media_taxa_execucao) over w as taxa_execucao_pre,
        lag(m.media_receita_bruta) over w as receita_bruta_pre,
        lag(m.media_despesa_paga) over w as despesa_paga_pre,
        lag(m.receita_per_capita) over w as receita_pc_pre,
        lag(m.despesa_per_capita) over w as despesa_pc_pre,
        lag(m.crescimento_receita_pct) over w as crescimento_receita_pre,
        lag(m.crescimento_pop_pct) over w as crescimento_pop_pre,

        -- Previous political context
        lag(m.score_ideologico) over w as score_ideologico_pre,
        lag(m.bloco_ideologico) over w as bloco_ideologico_pre,
        lag(m.percentual_vencedor) over w as margem_vitoria_pre,

        -- Mandate sequence number (for event study)
        row_number() over w as mandato_numero

    from mandatos m
    window w as (partition by m.id_municipio order by m.ano_eleicao)
)

select
    -- =========================================================================
    -- IDENTIFIERS
    -- =========================================================================
    id_municipio,
    ano_eleicao,
    periodo_mandato,
    nome_municipio,
    sigla_uf,
    regiao,
    porte_municipio,
    mandato_numero,

    -- =========================================================================
    -- TREATMENT DEFINITIONS
    -- =========================================================================

    -- Treatment 1: Party changed
    case
        when is_continuidade_partidaria = false then 1
        else 0
    end as tratamento_mudanca_partido,

    -- Treatment 2: Significant ideological shift
    case
        when abs(coalesce(delta_ideologico, 0)) >= 2 then 1
        else 0
    end as tratamento_mudanca_ideologia_forte,

    case
        when abs(coalesce(delta_ideologico, 0)) >= 1 then 1
        else 0
    end as tratamento_mudanca_ideologia_moderada,

    -- Treatment 3: Direction of ideological shift
    case
        when delta_ideologico >= 2 then 'esquerda_para_direita'
        when delta_ideologico >= 1 then 'centro_para_direita'
        when delta_ideologico <= -2 then 'direita_para_esquerda'
        when delta_ideologico <= -1 then 'centro_para_esquerda'
        else 'sem_mudanca_significativa'
    end as direcao_transicao,

    -- Treatment 4: New party entered (not just alternance between 2)
    case
        when partido_vencedor != partido_anterior
            and partido_vencedor != lag(partido_anterior) over (
                partition by id_municipio order by ano_eleicao
            )
        then 1
        else 0
    end as tratamento_partido_novo,

    -- =========================================================================
    -- POLITICAL CONTEXT
    -- =========================================================================
    partido_vencedor,
    partido_anterior,
    bloco_ideologico,
    bloco_ideologico_pre,
    score_ideologico,
    score_ideologico_pre,
    delta_ideologico,
    tipo_transicao_ideologica,

    percentual_vencedor as margem_vitoria_post,
    margem_vitoria_pre,
    nivel_competicao,

    -- =========================================================================
    -- OUTCOME VARIABLES - POST (current mandate)
    -- =========================================================================
    media_saldo_fiscal as saldo_fiscal_post,
    media_taxa_execucao as taxa_execucao_post,
    media_receita_bruta as receita_bruta_post,
    media_despesa_paga as despesa_paga_post,
    receita_per_capita as receita_pc_post,
    despesa_per_capita as despesa_pc_post,
    crescimento_receita_pct as crescimento_receita_post,
    crescimento_pop_pct as crescimento_pop_post,
    proporcao_anos_deficit as prop_deficit_post,

    -- =========================================================================
    -- OUTCOME VARIABLES - PRE (previous mandate)
    -- =========================================================================
    saldo_fiscal_pre,
    taxa_execucao_pre,
    receita_bruta_pre,
    despesa_paga_pre,
    receita_pc_pre,
    despesa_pc_pre,
    crescimento_receita_pre,
    crescimento_pop_pre,

    -- =========================================================================
    -- DIFFERENCE (POST - PRE) - The "second difference" in DiD
    -- =========================================================================
    media_saldo_fiscal - coalesce(saldo_fiscal_pre, 0) as diff_saldo_fiscal,
    media_taxa_execucao - coalesce(taxa_execucao_pre, 0) as diff_taxa_execucao,
    receita_per_capita - coalesce(receita_pc_pre, 0) as diff_receita_pc,
    despesa_per_capita - coalesce(despesa_pc_pre, 0) as diff_despesa_pc,

    -- Percentage change from pre to post
    case
        when saldo_fiscal_pre != 0
        then round((media_saldo_fiscal - saldo_fiscal_pre) / abs(saldo_fiscal_pre) * 100, 2)
    end as pct_change_saldo_fiscal,

    case
        when receita_pc_pre > 0
        then round((receita_per_capita - receita_pc_pre) / receita_pc_pre * 100, 2)
    end as pct_change_receita_pc,

    -- =========================================================================
    -- CONTROL VARIABLES
    -- =========================================================================
    idhm_baseline,
    ivs_baseline,
    gini_baseline,
    faixa_idhm_baseline,
    nivel_desenvolvimento_inicial,
    pop_inicio_mandato as pop_baseline,
    is_capital,
    is_amazonia_legal,

    -- =========================================================================
    -- DATA QUALITY FLAGS
    -- =========================================================================
    has_dados_fiscais_suficientes,
    anos_com_dados_fiscais,

    -- Has pre-period data (required for DiD)
    case
        when saldo_fiscal_pre is not null then true
        else false
    end as has_pre_period_data,

    -- Valid for DiD analysis
    case
        when saldo_fiscal_pre is not null
            and media_saldo_fiscal is not null
            and mandato_numero >= 2
        then true
        else false
    end as valid_for_did

from with_lags

-- Keep only mandates from 2004+ (need previous mandate for DiD)
where ano_eleicao >= 2004

order by id_municipio, ano_eleicao
