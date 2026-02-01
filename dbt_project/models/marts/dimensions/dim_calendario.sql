{{
    config(
        materialized='table',
        tags=['dimension', 'calendar', 'gold']
    )
}}

/*
    Calendar Dimension Table

    Time dimension for joining with fact tables.
    Includes flags for census years, election years, and fiscal periods.

    Grain: One row per year (1990-2030)
*/

-- Using DuckDB's native generate_series for better compatibility
with years as (
    select unnest(generate_series(1990, 2030)) as ano
),

final as (
    select
        -- Surrogate key (year as integer)
        ano as sk_ano,

        -- Year attributes
        ano,

        -- Decade
        (ano / 10 * 10)::integer as decada,

        -- Census flags (IBGE conducts census every 10 years)
        case
            when ano in (1991, 2000, 2010, 2022)
            then true else false
        end as is_ano_censo,

        -- Election flags (municipal elections every 4 years)
        case
            when ano % 4 = 0
            then true else false
        end as is_ano_eleitoral_municipal,

        -- IDHM data availability (only census years have IDHM)
        case
            when ano in (1991, 2000, 2010)
            then true else false
        end as has_idhm_data,

        -- SICONFI data availability (from 2013)
        case
            when ano >= 2013
            then true else false
        end as has_siconfi_data,

        -- Metadata
        current_timestamp as _loaded_at

    from years
)

select * from final
where ano between 1990 and 2030
order by ano
