
with resort_microclimates as (
    select 'Kiroro Resort' as location, 1.80 as base_mod, 0.15 as wind_factor
    union all select 'Niseko United', 1.20, 0.10
    union all select 'Rusutsu Resort Ski Area', 1.30, 0.08
    union all select 'Furano Ski Resort', 1.05, 0.04
    union all select 'Appi Kogen Ski Resort', 1.50, 0.12
)

, raw_weather as (
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
        , cast(wind_speed_mean as double) as wind_speed_mean_ms
        , cast(wind_direction_dominant as varchar) as wind_direction_dominant
        , cast(snowfall as double) as station_snowfall_cm
        , cast(snow_depth as double) as station_snow_depth_cm
        , last_updated
    from {{ source('weather_jp', 'jp_ski_field_weather') }}
)

, season_enriched as (
    select
        r.*
        , coalesce(m.base_mod, 1.0) as base_mod
        , coalesce(m.wind_factor, 0.05) as wind_factor
        , case when extract(month from r.weather_date) in (11, 12, 1, 2, 3, 4) then true else false end as is_ski_season
        , case
            when extract(month from r.weather_date) in (11, 12) then extract(year from r.weather_date) + 1
            else extract(year from r.weather_date)
        end as season_year
        , greatest((r.resort_elevation_m - r.station_elevation_m) / 1000.0, 0.0) as elevation_delta_km
    from raw_weather r
    left join resort_microclimates m on r.location = m.location
)

, orographic_and_lapse as (
    select
        *
        , temperature_mean_c - (5.0 * elevation_delta_km) as adj_temp_mean_c
        , temperature_max_c - (5.0 * elevation_delta_km) as adj_temp_max_c
        , temperature_min_c - (5.0 * elevation_delta_km) as adj_temp_min_c
        , case
            when wind_direction_dominant in ('Northwest', 'West-Northwest', 'North-Northwest', '北西', '西北西', '北北西')
            then precipitation_mm * (base_mod + (coalesce(wind_speed_mean_ms, 0.0) * wind_factor))
            else precipitation_mm * base_mod
        end as uplifted_precip_mm
    from season_enriched
    where is_ski_season
)

, wet_bulb_bounds as (
    select
        *
        , adj_temp_min_c * atan(0.151977 * pow(relative_humidity_mean_pct + 8.313659, 0.5))
          + atan(adj_temp_min_c + relative_humidity_mean_pct)
          - atan(relative_humidity_mean_pct - 1.676331)
          + 0.00391838 * pow(relative_humidity_mean_pct, 1.5) * atan(0.023101 * relative_humidity_mean_pct)
          - 4.686035 as wb_min_c
        , adj_temp_max_c * atan(0.151977 * pow(relative_humidity_mean_pct + 8.313659, 0.5))
          + atan(adj_temp_max_c + relative_humidity_mean_pct)
          - atan(relative_humidity_mean_pct - 1.676331)
          + 0.00391838 * pow(relative_humidity_mean_pct, 1.5) * atan(0.023101 * relative_humidity_mean_pct)
          - 4.686035 as wb_max_c
    from orographic_and_lapse
)

, modeled as (
    select
        *
        , case
            when wb_min_c <= 0.0 then 1.0
            when wb_min_c >= 2.0 then 0.0
            else greatest(least((2.0 - wb_min_c) / 2.0, 1.0), 0.0)
        end as snow_frac_min
        , case
            when wb_max_c <= 0.0 then 1.0
            when wb_max_c >= 2.0 then 0.0
            else greatest(least((2.0 - wb_max_c) / 2.0, 1.0), 0.0)
        end as snow_frac_max
        , case
            when adj_temp_mean_c <= -8.0 then 22.0
            when adj_temp_mean_c <= -5.0 then 19.0
            when adj_temp_mean_c <= -2.0 then 14.0
            when adj_temp_mean_c <= 0.0 then 12.0
            when adj_temp_mean_c <= 1.5 then 10.0
            else 8.0
        end as snow_liquid_ratio
    from wet_bulb_bounds
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
    , base_mod as microclimate_base_mod
    , wind_factor as microclimate_wind_factor
    , precipitation_mm
    , temperature_mean_c
    , temperature_min_c
    , temperature_max_c
    , relative_humidity_mean_pct
    , wind_speed_mean_ms
    , wind_direction_dominant
    , coalesce(station_snowfall_cm, 0.0) as station_snowfall_cm
    , coalesce(station_snow_depth_cm, 0.0) as station_snow_depth_cm
    , coalesce(station_snow_depth_cm, 0.0) / 100.0 as station_snow_depth_m
    , adj_temp_mean_c as adjusted_temperature_mean_c
    , uplifted_precip_mm
    , wb_min_c
    , wb_max_c
    , (snow_frac_min + snow_frac_max) / 2.0 as daily_snow_fraction
    , snow_liquid_ratio
    , coalesce(uplifted_precip_mm, 0.0) * ((snow_frac_min + snow_frac_max) / 2.0) * snow_liquid_ratio / 9.7 as elev_adjusted_snowfall_cm
    , last_updated
from modeled
