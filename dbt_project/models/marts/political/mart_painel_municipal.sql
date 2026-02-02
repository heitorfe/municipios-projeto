{{
    config(
        materialized='table',
        tags=['mart', 'analytics', 'panel', 'annual']
    )
}}

/*
    ==========================================================================
    MART: PAINEL MUNICIPAL (Annual Municipal Panel)
    ==========================================================================

    Long-format panel data for longitudinal econometric analysis.
    One row per municipality per year - suitable for panel methods.

    Grain: Municipality × Year

    Designed for:
    - Panel data regression (fixed effects, random effects)
    - Time series analysis at municipal level
    - Year-over-year change detection
    - Interrupted time series designs
    - Event studies around elections

    Variables included:
    - Municipality identifiers and fixed effects groupings
    - Current mandate's political context
    - Annual fiscal metrics (2013+)
    - Population for per-capita calculations
    - Lagged variables for dynamic analysis
    - First differences for stationarity
    - Mandate year indicator (1-4 within term)

    Compatible with: Stata, R plm package, Python linearmodels
*/

-- Fiscal data by year
with fiscal as (
    select
        id_municipio,
        ano,
        receita_bruta,
        receita_liquida,
        despesa_paga,
        despesa_empenhada,
        saldo_fiscal,
        taxa_execucao_percentual
    from {{ ref('fct_financas_municipais') }}
),

-- Population by year
populacao as (
    select
        id_municipio,
        ano,
        populacao
    from {{ ref('stg_populacao') }}
),

-- Municipality dimension for fixed effects grouping
municipios as (
    select
        id_municipio_ibge as id_municipio,
        nome_municipio,
        sigla_uf,
        regiao,
        is_capital,
        is_amazonia_legal,
        porte_municipio,
        idhm_2010,
        faixa_idhm
    from {{ ref('dim_municipio') }}
),

-- Mandates for political context
mandatos as (
    select
        id_municipio,
        ano_eleicao,
        ano_inicio_mandato,
        ano_fim_mandato,
        partido_vencedor,
        bloco_ideologico,
        score_ideologico,
        is_partido_big_tent,
        is_continuidade_partidaria,
        delta_ideologico,
        percentual_vencedor
    from {{ ref('dim_mandato_prefeito') }}
),

-- Generate year spine (2000-2024)
year_spine as (
    select ano from {{ ref('dim_calendario') }}
    where ano between 2000 and 2024
),

-- Cross join to create all municipality-year combinations
panel_base as (
    select
        m.id_municipio,
        m.nome_municipio,
        m.sigla_uf,
        m.regiao,
        m.is_capital,
        m.is_amazonia_legal,
        m.porte_municipio,
        m.idhm_2010,
        m.faixa_idhm,
        y.ano
    from municipios m
    cross join year_spine y
),

-- Add fiscal data
panel_with_fiscal as (
    select
        pb.*,
        f.receita_bruta,
        f.receita_liquida,
        f.despesa_paga,
        f.despesa_empenhada,
        f.saldo_fiscal,
        f.taxa_execucao_percentual
    from panel_base pb
    left join fiscal f
        on pb.id_municipio = f.id_municipio
        and pb.ano = f.ano
),

-- Add population
panel_with_pop as (
    select
        pf.*,
        p.populacao,
        -- Per capita metrics
        case when p.populacao > 0 then pf.receita_bruta / p.populacao end as receita_per_capita,
        case when p.populacao > 0 then pf.despesa_paga / p.populacao end as despesa_per_capita
    from panel_with_fiscal pf
    left join populacao p
        on pf.id_municipio = p.id_municipio
        and pf.ano = p.ano
),

-- Add current mandate context
panel_with_mandate as (
    select
        pp.*,

        -- Current mandate (find which mandate covers this year)
        man.ano_eleicao,
        man.partido_vencedor,
        man.bloco_ideologico,
        man.score_ideologico,
        man.is_partido_big_tent,
        man.is_continuidade_partidaria,
        man.delta_ideologico,
        man.percentual_vencedor,

        -- Year within mandate (1-4)
        pp.ano - man.ano_eleicao as ano_no_mandato,

        -- First/last year flags
        case when pp.ano = man.ano_inicio_mandato then true else false end as is_primeiro_ano_mandato,
        case when pp.ano = man.ano_fim_mandato then true else false end as is_ultimo_ano_mandato,

        -- Election year flag
        case when pp.ano in (2000, 2004, 2008, 2012, 2016, 2020, 2024) then true else false end as is_ano_eleitoral

    from panel_with_pop pp
    left join mandatos man
        on pp.id_municipio = man.id_municipio
        and pp.ano between man.ano_inicio_mandato and man.ano_fim_mandato
),

-- Add lagged variables for dynamic models
panel_with_lags as (
    select
        pm.*,

        -- Lagged fiscal (t-1)
        lag(pm.receita_bruta) over (partition by pm.id_municipio order by pm.ano) as receita_bruta_lag1,
        lag(pm.despesa_paga) over (partition by pm.id_municipio order by pm.ano) as despesa_paga_lag1,
        lag(pm.saldo_fiscal) over (partition by pm.id_municipio order by pm.ano) as saldo_fiscal_lag1,
        lag(pm.populacao) over (partition by pm.id_municipio order by pm.ano) as populacao_lag1,

        -- Lagged political (previous year)
        lag(pm.partido_vencedor) over (partition by pm.id_municipio order by pm.ano) as partido_ano_anterior,
        lag(pm.score_ideologico) over (partition by pm.id_municipio order by pm.ano) as score_ideologico_lag1,

        -- Lagged fiscal (t-2) for second-order dynamics
        lag(pm.receita_bruta, 2) over (partition by pm.id_municipio order by pm.ano) as receita_bruta_lag2,
        lag(pm.despesa_paga, 2) over (partition by pm.id_municipio order by pm.ano) as despesa_paga_lag2

    from panel_with_mandate pm
),

-- Calculate first differences (for stationarity)
final as (
    select
        -- Identifiers
        pl.id_municipio,
        pl.ano,
        pl.nome_municipio,

        -- Geography / Fixed Effects Groupings
        pl.sigla_uf,
        pl.regiao,
        pl.is_capital,
        pl.is_amazonia_legal,
        pl.porte_municipio,
        pl.idhm_2010,
        pl.faixa_idhm,

        -- Fixed effect IDs
        pl.id_municipio as fe_municipio,
        pl.ano as fe_ano,
        pl.sigla_uf as fe_uf,

        -- Current Fiscal Metrics
        pl.receita_bruta,
        pl.receita_liquida,
        pl.despesa_paga,
        pl.despesa_empenhada,
        pl.saldo_fiscal,
        pl.taxa_execucao_percentual,
        pl.populacao,
        pl.receita_per_capita,
        pl.despesa_per_capita,

        -- Political Context (current mandate)
        pl.ano_eleicao,
        pl.partido_vencedor,
        pl.bloco_ideologico,
        pl.score_ideologico,
        pl.is_partido_big_tent,
        pl.is_continuidade_partidaria,
        pl.delta_ideologico,
        pl.percentual_vencedor,
        pl.ano_no_mandato,
        pl.is_primeiro_ano_mandato,
        pl.is_ultimo_ano_mandato,
        pl.is_ano_eleitoral,

        -- Lagged Variables
        pl.receita_bruta_lag1,
        pl.despesa_paga_lag1,
        pl.saldo_fiscal_lag1,
        pl.populacao_lag1,
        pl.partido_ano_anterior,
        pl.score_ideologico_lag1,
        pl.receita_bruta_lag2,
        pl.despesa_paga_lag2,

        -- First Differences (delta = current - previous)
        pl.receita_bruta - pl.receita_bruta_lag1 as delta_receita,
        pl.despesa_paga - pl.despesa_paga_lag1 as delta_despesa,
        pl.saldo_fiscal - pl.saldo_fiscal_lag1 as delta_saldo_fiscal,
        pl.populacao - pl.populacao_lag1 as delta_populacao,

        -- Growth Rates (percentage change)
        case
            when pl.receita_bruta_lag1 > 0
            then (pl.receita_bruta - pl.receita_bruta_lag1) / pl.receita_bruta_lag1 * 100
        end as crescimento_receita_pct,
        case
            when pl.despesa_paga_lag1 > 0
            then (pl.despesa_paga - pl.despesa_paga_lag1) / pl.despesa_paga_lag1 * 100
        end as crescimento_despesa_pct,
        case
            when pl.populacao_lag1 > 0
            then (pl.populacao - pl.populacao_lag1) / pl.populacao_lag1 * 100
        end as crescimento_pop_pct,

        -- Party Change Indicator
        case
            when pl.partido_vencedor != pl.partido_ano_anterior then true
            else false
        end as mudou_partido_ano,

        -- Data Quality Flags
        case when pl.receita_bruta is not null then true else false end as has_dados_fiscais,
        case when pl.populacao is not null then true else false end as has_dados_populacao,
        case when pl.ano_eleicao is not null then true else false end as has_dados_politicos,

        -- Metadata
        current_timestamp as _loaded_at

    from panel_with_lags pl
)

select * from final
order by id_municipio, ano
