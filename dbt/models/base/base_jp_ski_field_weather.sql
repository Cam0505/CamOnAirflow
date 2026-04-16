
with raw_weather as (
    select
        cast(date as date) as weather_date
        , location
        , country
        , timezone
        , source_name
        , station_name
        , station_prec_no
        , station_block_no
        , cast(station_distance_km as double) as station_distance_km
        , cast(station_elevation as double) as station_elevation_m
        , cast(resort_elevation as double) as resort_elevation_m
        , cast(precipitation_sum as double) as precipitation_mm
        , cast(temperature_mean as double) as temperature_mean_c
        , cast(temperature_min as double) as temperature_min_c
        , cast(temperature_max as double) as temperature_max_c
        , cast(relative_humidity_mean as double) as relative_humidity_mean_pct
        , cast(snowfall as double) as station_snowfall_cm
        , cast(snow_depth as double) as station_snow_depth_cm
        , last_updated
    from {{ source('weather_jp', 'jp_ski_field_weather') }}
)

, season_enriched as (
    select
        *
        , case when extract(month from weather_date) in (11, 12, 1, 2, 3, 4) then true else false end as is_ski_season
        , case
            when extract(month from weather_date) in (11, 12) then extract(year from weather_date) + 1
            else extract(year from weather_date)
        end as season_year
        , greatest((resort_elevation_m - station_elevation_m) / 1000.0, 0.0) as elevation_delta_km
    from raw_weather
)

, adjusted as (
    select
        *
        , temperature_mean_c - 6.5 * elevation_delta_km as adjusted_temperature_mean_c
    from season_enriched
    where is_ski_season
)

, modeled as (
    select
        *
        , case
            when adjusted_temperature_mean_c <= -1.4 then 1.0
            when adjusted_temperature_mean_c >= 3.1 then 0.0
            else greatest(least((3.1 - adjusted_temperature_mean_c) / 4.5, 1.0), 0.0)
        end as snow_fraction
        , case
            when adjusted_temperature_mean_c <= -8.0 then 22.0
            when adjusted_temperature_mean_c <= -5.0 then 19.0
            when adjusted_temperature_mean_c <= -2.0 then 14.0
            when adjusted_temperature_mean_c <= 0.0 then 12.0
            when adjusted_temperature_mean_c <= 1.5 then 10.0
            else 8.0
        end as snow_liquid_ratio
    from adjusted
)

select
    weather_date as date
    , location
    , country
    , timezone
    , source_name
    , station_name
    , station_prec_no
    , station_block_no
    , station_distance_km
    , station_elevation_m
    , resort_elevation_m
    , elevation_delta_km
    , season_year
    , precipitation_mm
    , temperature_mean_c
    , temperature_min_c
    , temperature_max_c
    , relative_humidity_mean_pct
    , coalesce(station_snowfall_cm, 0.0) as station_snowfall_cm
    , coalesce(station_snow_depth_cm, 0.0) as station_snow_depth_cm
    , coalesce(station_snow_depth_cm, 0.0) / 100.0 as station_snow_depth_m
    , adjusted_temperature_mean_c
    , coalesce(precipitation_mm, 0.0) * snow_fraction * snow_liquid_ratio / 9.7 as elev_adjusted_snowfall_cm
    , snow_fraction
    , snow_liquid_ratio
    , last_updated
from modeled
