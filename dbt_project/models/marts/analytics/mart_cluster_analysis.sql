{{
    config(
        materialized='table',
        tags=['mart', 'analytics', 'clustering']
    )
}}

/*
    ==========================================================================
    MART: CLUSTER ANALYSIS (Municipality Development Tiers)
    ==========================================================================

    Grain: One row per municipality

    Combines cluster assignments with municipality dimensions and
    fiscal/efficiency metrics for comprehensive cluster analysis.

    Cluster Tiers (ordered from most to least developed):
    - 0: Polos de Desenvolvimento (Development Poles)
    - 1: Desenvolvimento Avancado (Advanced Development)
    - 2: Em Desenvolvimento (Developing)
    - 3: Vulneraveis (Vulnerable)
    - 4: Criticos (Critical)

    Use cases:
    - Analyze development patterns across municipalities
    - Compare clusters by region and state
    - Identify municipalities near cluster boundaries
    - Policy targeting by development tier

    Data sources:
    - seed_cluster_assignments: K-Means cluster assignments
    - dim_municipio: Geographic, demographic, and social indicators
    - mart_dependencia_fiscal: Fiscal dependency metrics (latest year)
    - mart_eficiencia_municipal: Efficiency metrics (latest year)
*/

with clusters as (
    select
        id_municipio_ibge,
        cluster_id,
        cluster_label
    from {{ ref('seed_cluster_assignments') }}
),

municipios as (
    select
        id_municipio_ibge,
        nome_municipio,
        sigla_uf,
        nome_uf,
        regiao,
        populacao,
        porte_municipio,
        is_capital,
        is_amazonia_legal,
        idhm_2010,
        idhm_educacao,
        idhm_longevidade,
        idhm_renda,
        faixa_idhm,
        ivs_2010,
        faixa_ivs,
        gini_2010,
        renda_per_capita_2010,
        esperanca_vida_2010
    from {{ ref('dim_municipio') }}
),

-- Get latest year with fiscal data
latest_fiscal_year as (
    select max(ano) as ano from {{ ref('mart_dependencia_fiscal') }}
),

fiscal_latest as (
    select
        id_municipio,
        dependency_ratio,
        own_revenue_ratio,
        categoria_dependencia,
        receita_propria_per_capita,
        transferencias_per_capita,
        revenue_effort_index
    from {{ ref('mart_dependencia_fiscal') }}
    where ano = (select ano from latest_fiscal_year)
),

-- Get latest year with efficiency data
latest_efficiency_year as (
    select max(ano) as ano from {{ ref('mart_eficiencia_municipal') }}
),

efficiency_latest as (
    select
        id_municipio,
        efficiency_index,
        social_outcome_score,
        spend_score,
        categoria_eficiencia
    from {{ ref('mart_eficiencia_municipal') }}
    where ano = (select ano from latest_efficiency_year)
),

-- Calculate cluster statistics
cluster_stats as (
    select
        c.cluster_id,
        count(*) as cluster_size,
        avg(m.idhm_2010) as cluster_avg_idhm,
        avg(m.ivs_2010) as cluster_avg_ivs,
        min(m.idhm_2010) as cluster_min_idhm,
        max(m.idhm_2010) as cluster_max_idhm
    from clusters c
    join municipios m on c.id_municipio_ibge = m.id_municipio_ibge
    group by c.cluster_id
),

final as (
    select
        -- Keys
        m.id_municipio_ibge,

        -- Cluster assignment
        c.cluster_id,
        c.cluster_label,

        -- Municipality context
        m.nome_municipio,
        m.sigla_uf,
        m.nome_uf,
        m.regiao,
        m.populacao,
        m.porte_municipio,
        m.is_capital,
        m.is_amazonia_legal,

        -- Development indicators (clustering features)
        m.idhm_2010,
        m.idhm_educacao,
        m.idhm_longevidade,
        m.idhm_renda,
        m.faixa_idhm,
        m.ivs_2010,
        m.faixa_ivs,
        m.gini_2010,
        m.renda_per_capita_2010,
        m.esperanca_vida_2010,

        -- Fiscal metrics (latest year)
        f.dependency_ratio,
        f.own_revenue_ratio,
        f.categoria_dependencia,
        f.receita_propria_per_capita,
        f.transferencias_per_capita,
        f.revenue_effort_index,

        -- Efficiency metrics (latest year)
        e.efficiency_index,
        e.social_outcome_score,
        e.spend_score,
        e.categoria_eficiencia,

        -- Cluster statistics
        cs.cluster_size,
        cs.cluster_avg_idhm,
        cs.cluster_avg_ivs,

        -- Deviation from cluster mean (for transition analysis)
        m.idhm_2010 - cs.cluster_avg_idhm as idhm_vs_cluster,

        -- Distance to cluster boundaries
        case
            when c.cluster_id < 4 then cs.cluster_max_idhm - m.idhm_2010
            else null
        end as distance_to_upper_boundary,

        case
            when c.cluster_id > 0 then m.idhm_2010 - cs.cluster_min_idhm
            else null
        end as distance_to_lower_boundary,

        -- Transition potential flag
        case
            when m.idhm_2010 - cs.cluster_avg_idhm > 0.02 then 'Potencial Promocao'
            when m.idhm_2010 - cs.cluster_avg_idhm < -0.02 then 'Risco Rebaixamento'
            else 'Estavel'
        end as status_transicao,

        -- Metadata
        (select ano from latest_fiscal_year) as ano_dados_fiscais,
        (select ano from latest_efficiency_year) as ano_dados_eficiencia,
        current_timestamp as _loaded_at

    from municipios m
    inner join clusters c on m.id_municipio_ibge = c.id_municipio_ibge
    inner join cluster_stats cs on c.cluster_id = cs.cluster_id
    left join fiscal_latest f on m.id_municipio_ibge = f.id_municipio
    left join efficiency_latest e on m.id_municipio_ibge = e.id_municipio
)

select * from final
order by cluster_id, idhm_2010 desc
