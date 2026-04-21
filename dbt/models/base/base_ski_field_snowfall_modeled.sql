{% set snowfall_model_columns = [
    'snowfall_era5_cm',
    'snowfall_ecmwf_ifs_cm',
    'snowfall_ukmo_seamless_cm',
    'snowfall_icon_seamless_cm',
    'snowfall_gem_seamless_cm',
    'snowfall_cma_grapes_global_cm',
    'snowfall_gfs_seamless_cm',
    'snowfall_meteofrance_seamless_cm',
    'snowfall_jma_seamless_cm',
    'snowfall_bom_access_global_cm'
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