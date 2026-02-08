/*
    ==========================================================================
    UTILITY MACROS FOR MUNICIPIOS ANALYTICS
    ==========================================================================

    Helper functions and utilities for common data transformations
    specific to Brazilian municipal data.
*/


-- ==========================================================================
-- MACRO: Classify IDHM into tiers
-- ==========================================================================
-- Returns Portuguese classification tier based on IDHM value

{% macro classify_idhm(column_name) %}
    case
        when {{ column_name }} >= 0.800 then 'Muito Alto'
        when {{ column_name }} >= 0.700 then 'Alto'
        when {{ column_name }} >= 0.600 then 'Médio'
        when {{ column_name }} >= 0.500 then 'Baixo'
        when {{ column_name }} is not null then 'Muito Baixo'
        else null
    end
{% endmacro %}


-- ==========================================================================
-- MACRO: Classify IVS (Social Vulnerability) into tiers
-- ==========================================================================
-- Returns Portuguese classification tier based on IVS value
-- Note: Lower IVS is better (inverse to IDHM)

{% macro classify_ivs(column_name) %}
    case
        when {{ column_name }} <= 0.200 then 'Muito Baixa'
        when {{ column_name }} <= 0.300 then 'Baixa'
        when {{ column_name }} <= 0.400 then 'Média'
        when {{ column_name }} <= 0.500 then 'Alta'
        when {{ column_name }} is not null then 'Muito Alta'
        else null
    end
{% endmacro %}


-- ==========================================================================
-- MACRO: Classify municipality size by population
-- ==========================================================================
-- Brazilian municipal size classification

{% macro classify_porte_municipio(population_column) %}
    case
        when {{ population_column }} < 5000 then 'Micro (< 5k)'
        when {{ population_column }} < 20000 then 'Pequeno (5k-20k)'
        when {{ population_column }} < 100000 then 'Médio (20k-100k)'
        when {{ population_column }} < 500000 then 'Grande (100k-500k)'
        when {{ population_column }} is not null then 'Metrópole (500k+)'
        else null
    end
{% endmacro %}


-- ==========================================================================
-- MACRO: Classify IDEB (Education Quality) into tiers
-- ==========================================================================
-- IDEB is on a 0-10 scale

{% macro classify_ideb(column_name) %}
    case
        when {{ column_name }} >= 6.0 then 'Desenvolvido'
        when {{ column_name }} >= 5.0 then 'Medio-Alto'
        when {{ column_name }} >= 4.0 then 'Medio'
        when {{ column_name }} >= 3.0 then 'Medio-Baixo'
        when {{ column_name }} is not null then 'Baixo'
        else null
    end
{% endmacro %}


-- ==========================================================================
-- MACRO: Classify fiscal dependency ratio
-- ==========================================================================
-- Based on federal transfer dependency percentage

{% macro classify_dependency(ratio_column) %}
    case
        when {{ ratio_column }} >= 80 then 'Extremamente Dependente'
        when {{ ratio_column }} >= 60 then 'Muito Dependente'
        when {{ ratio_column }} >= 40 then 'Dependente'
        when {{ ratio_column }} >= 20 then 'Parcialmente Dependente'
        when {{ ratio_column }} is not null then 'Autonomo'
        else null
    end
{% endmacro %}


-- ==========================================================================
-- MACRO: Classify electoral competition level
-- ==========================================================================
-- Based on number of candidates

{% macro classify_competition(num_candidates_column) %}
    case
        when {{ num_candidates_column }} = 1 then 'Sem Competicao'
        when {{ num_candidates_column }} = 2 then 'Bipolar'
        when {{ num_candidates_column }} <= 4 then 'Moderada'
        when {{ num_candidates_column }} is not null then 'Alta'
        else null
    end
{% endmacro %}


-- ==========================================================================
-- MACRO: Calculate per-capita metric safely
-- ==========================================================================
-- Handles division by zero and null population

{% macro per_capita(value_column, population_column, decimal_places=2) %}
    case
        when {{ population_column }} > 0
        then round({{ value_column }} / {{ population_column }}, {{ decimal_places }})
        else null
    end
{% endmacro %}


-- ==========================================================================
-- MACRO: Calculate percentage safely
-- ==========================================================================
-- Handles division by zero

{% macro safe_percentage(numerator, denominator, decimal_places=2) %}
    case
        when {{ denominator }} > 0
        then round(cast({{ numerator }} as decimal(18,4)) / {{ denominator }} * 100, {{ decimal_places }})
        else null
    end
{% endmacro %}


-- ==========================================================================
-- MACRO: Get election mandate period from election year
-- ==========================================================================
-- Municipal mandates are 4 years starting January 1st after election

{% macro mandate_period(election_year_column) %}
    concat(
        cast({{ election_year_column }} + 1 as varchar), '-',
        cast({{ election_year_column }} + 4 as varchar)
    )
{% endmacro %}


-- ==========================================================================
-- MACRO: Generate date spine for Brazilian fiscal calendar
-- ==========================================================================
-- Creates year-level calendar with relevant flags

{% macro brazilian_calendar_spine(start_year, end_year) %}
    with years as (
        select unnest(generate_series({{ start_year }}, {{ end_year }})) as ano
    )
    select
        ano,
        ano as sk_ano,
        (ano / 10 * 10)::integer as decada,
        case when ano in (1991, 2000, 2010, 2022) then true else false end as is_ano_censo,
        case when ano % 4 = 0 then true else false end as is_ano_eleitoral_municipal,
        case when ano in (1991, 2000, 2010) then true else false end as has_idhm_data,
        case when ano >= 2013 then true else false end as has_siconfi_data
    from years
{% endmacro %}


-- ==========================================================================
-- MACRO: Standardize Brazilian UF codes
-- ==========================================================================
-- Ensures consistent uppercase 2-letter state codes

{% macro standardize_sigla_uf(column_name) %}
    upper(trim({{ column_name }}))
{% endmacro %}


-- ==========================================================================
-- MACRO: Standardize IBGE municipality code
-- ==========================================================================
-- Ensures consistent 7-character string format

{% macro standardize_ibge_code(column_name) %}
    cast({{ column_name }} as varchar(7))
{% endmacro %}
