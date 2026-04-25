-- ==============================================================================
-- [INTENT — DO NOT REMOVE] base_ski_field_snowfall_modeled
-- Casts and standardises daily snowfall from multiple NWP model columns.
-- snowfall_model_columns Jinja list drives both the SELECT and the CAST loop,
--   keeping the column list in one place — add new model columns to the list
--   only, the SQL body updates automatically.
-- COALESCE(column, 0.0) converts NULLs to 0 so cumulative sums in analysis
--   models don't silently drop days. Raw NULLs would need special handling
--   if you ever need to distinguish "no forecast" from "zero snowfall".
-- snowfall_era5_cm and snowfall_bom_access_global_cm are outside the Jinja
--   loop — they are not included in multi-model ensemble logic.
-- ==============================================================================

{% set snowfall_model_columns = [
    'snowfall_ecmwf_ifs_cm',
    'snowfall_ukmo_seamless_cm',
    'snowfall_icon_seamless_cm',
    'snowfall_gem_seamless_cm',
    'snowfall_cma_grapes_global_cm',
    'snowfall_jma_seamless_cm'
] %}

with source_snowfall as (
    select
        cast(date as date) as date
        , location as ski_field
        , country
        , cast(resort_elevation as double) as resort_elevation_m
        , cast(grid_elevation as double) as grid_elevation_m
        {% for column_name in snowfall_model_columns %}
        , cast(coalesce({{ column_name }}, 0.0) as double) as {{ column_name }}
        {% endfor %}
    from {{ source('snowfall', 'ski_field_snowfall') }}
)

select
    date
    , ski_field
    , country
    , resort_elevation_m
    , grid_elevation_m
    {% for column_name in snowfall_model_columns %}
    , {{ column_name }}
    {% endfor %}
from source_snowfall
order by ski_field, date