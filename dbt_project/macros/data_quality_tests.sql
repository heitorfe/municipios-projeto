/*
    ==========================================================================
    CUSTOM DATA QUALITY TESTS FOR MUNICIPIOS ANALYTICS
    ==========================================================================

    These macros provide reusable data quality tests specific to Brazilian
    municipal data analysis.

    Usage: Add tests in schema.yml files using:
        data_tests:
          - test_name:
              param1: value1
*/


-- ==========================================================================
-- TEST: Valid IBGE Municipality Code
-- ==========================================================================
-- IBGE codes are 7 digits: 2 for state (11-53) + 5 for municipality
-- Example: 3550308 = São Paulo (state 35, municipality 50308)

{% test valid_ibge_code(model, column_name) %}

with validation as (
    select
        {{ column_name }} as ibge_code
    from {{ model }}
    where {{ column_name }} is not null
),

invalid_codes as (
    select ibge_code
    from validation
    where
        -- Must be exactly 7 characters
        length(cast(ibge_code as varchar)) != 7
        -- First 2 digits must be valid state code (11-53)
        or cast(substring(cast(ibge_code as varchar), 1, 2) as integer) not between 11 and 53
        -- Cannot be all zeros
        or ibge_code = '0000000'
)

select * from invalid_codes

{% endtest %}


-- ==========================================================================
-- TEST: Valid Brazilian State Abbreviation
-- ==========================================================================
-- Validates that sigla_uf contains only valid 2-letter state codes

{% test valid_sigla_uf(model, column_name) %}

with valid_states as (
    select unnest(array[
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO',
        'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI',
        'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    ]) as sigla_uf
),

invalid_records as (
    select {{ column_name }} as sigla_uf
    from {{ model }}
    where {{ column_name }} is not null
      and upper({{ column_name }}) not in (select sigla_uf from valid_states)
)

select * from invalid_records

{% endtest %}


-- ==========================================================================
-- TEST: Valid Percentage (0-100)
-- ==========================================================================
-- Validates that percentage values are within valid range

{% test valid_percentage(model, column_name, allow_null=true) %}

with invalid_records as (
    select
        {{ column_name }} as pct_value
    from {{ model }}
    where
        {% if allow_null %}
        {{ column_name }} is not null and
        {% endif %}
        ({{ column_name }} < 0 or {{ column_name }} > 100)
)

select * from invalid_records

{% endtest %}


-- ==========================================================================
-- TEST: Valid Index (0-1 scale)
-- ==========================================================================
-- Validates indices like IDHM, IVS, Gini that use 0-1 scale

{% test valid_index_0_1(model, column_name, allow_null=true) %}

with invalid_records as (
    select
        {{ column_name }} as index_value
    from {{ model }}
    where
        {% if allow_null %}
        {{ column_name }} is not null and
        {% endif %}
        ({{ column_name }} < 0 or {{ column_name }} > 1)
)

select * from invalid_records

{% endtest %}


-- ==========================================================================
-- TEST: Positive Value (Financial Amounts)
-- ==========================================================================
-- Validates that financial values are non-negative

{% test positive_value(model, column_name, allow_null=true, allow_zero=true) %}

with invalid_records as (
    select
        {{ column_name }} as value
    from {{ model }}
    where
        {% if allow_null %}
        {{ column_name }} is not null and
        {% endif %}
        {% if allow_zero %}
        {{ column_name }} < 0
        {% else %}
        {{ column_name }} <= 0
        {% endif %}
)

select * from invalid_records

{% endtest %}


-- ==========================================================================
-- TEST: Election Year Validity
-- ==========================================================================
-- Municipal elections occur every 4 years starting from 2000

{% test valid_election_year(model, column_name) %}

with valid_years as (
    select unnest(array[2000, 2004, 2008, 2012, 2016, 2020, 2024, 2028]) as ano
),

invalid_records as (
    select {{ column_name }} as ano
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} not in (select ano from valid_years)
)

select * from invalid_records

{% endtest %}


-- ==========================================================================
-- TEST: Census Year Validity
-- ==========================================================================
-- IBGE conducts census in specific years (1991, 2000, 2010, 2022)

{% test valid_census_year(model, column_name) %}

with valid_years as (
    select unnest(array[1991, 2000, 2010, 2022]) as ano
),

invalid_records as (
    select {{ column_name }} as ano
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} not in (select ano from valid_years)
)

select * from invalid_records

{% endtest %}


-- ==========================================================================
-- TEST: SICONFI Year Range
-- ==========================================================================
-- SICONFI data is only available from 2013 onwards

{% test valid_siconfi_year(model, column_name) %}

with invalid_records as (
    select {{ column_name }} as ano
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} < 2013
)

select * from invalid_records

{% endtest %}


-- ==========================================================================
-- TEST: Consistent Foreign Key (Municipality)
-- ==========================================================================
-- Validates that municipality FK references exist in dimension

{% test valid_municipality_fk(model, column_name) %}

with dim_municipios as (
    select sk_municipio from {{ ref('dim_municipio') }}
),

orphan_records as (
    select {{ column_name }} as fk_value
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} not in (select sk_municipio from dim_municipios)
)

select * from orphan_records

{% endtest %}


-- ==========================================================================
-- TEST: Row Count Within Expected Range
-- ==========================================================================
-- Validates table has expected number of rows (useful for dimension tables)

{% test row_count_range(model, min_rows, max_rows) %}

with row_count as (
    select count(*) as cnt from {{ model }}
)

select cnt
from row_count
where cnt < {{ min_rows }} or cnt > {{ max_rows }}

{% endtest %}


-- ==========================================================================
-- TEST: No Duplicate Composite Key
-- ==========================================================================
-- Validates uniqueness across multiple columns (composite key)

{% test unique_combination(model, columns) %}

with duplicates as (
    select
        {{ columns | join(', ') }},
        count(*) as cnt
    from {{ model }}
    group by {{ columns | join(', ') }}
    having count(*) > 1
)

select * from duplicates

{% endtest %}
