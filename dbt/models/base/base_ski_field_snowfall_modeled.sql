with raw_weather as (
    select
        cast(date as date) as weather_date
        , location
        , country
        , cast(resort_elevation as double) as resort_elevation_m
        , cast(grid_elevation as double) as grid_elevation_m
        , cast(precipitation_sum as double) as precipitation_mm
        , cast(temperature_mean as double) as temperature_mean_c
        , cast(temperature_min as double) as temperature_min_c
        , cast(temperature_max as double) as temperature_max_c
        , cast(relative_humidity_mean as double) as relative_humidity_mean_pct
        , cast(wind_speed_max as double) as wind_speed_max_ms
        , cast(wind_direction_dominant as double) as wind_direction_dominant_deg
        , cast(snowfall as double) as om_direct_snowfall_cm
        , cast(snow_depth as double) as snow_depth_cm
    from {{ source('snowfall', 'ski_field_snowfall') }}
)

, resort_microclimates as (
    select 'Kiroro Resort' as location_match, 1.80 as base_mod, 0.15 as wind_factor, 270 as wind_dir_min_deg, 340 as wind_dir_max_deg
    union all select 'Niseko United', 1.15, 0.10, 270, 340
    union all select 'Appi Kogen Ski Resort', 1.15, 0.10, 270, 340
    union all select 'Furano Ski Resort', 1.40, 0.05, 270, 340
    union all select 'Hakodate Nanae Snowpark', 1.10, 0.05, 270, 340
    union all select 'Sahoro', 1.20, 0.05, 270, 340
    union all select 'Zao Onsen Ski Resort', 1.00, 0.05, 270, 340
    union all select 'Takasu Snow Park', 1.00, 0.05, 270, 340
    union all select 'Remarkables', 1.00, 0.05, 200, 280
    union all select 'Cardrona', 1.00, 0.05, 200, 280
    union all select 'Treble Cone', 0.80, 0.05, 200, 280
    union all select 'Mount Hutt', 1.10, 0.05, 150, 220
    union all select 'Ohau', 1.00, 0.05, 200, 280
    union all select 'Coronet Peak', 1.00, 0.05, 200, 280
    union all select 'Whakapapa', 1.00, 0.05, 200, 280
    union all select 'Turoa', 1.00, 0.05, 200, 280
    union all select 'Thredbo', 1.00, 0.05, 240, 310
    union all select 'Perisher', 1.00, 0.05, 240, 310
    union all select 'Mt Buller', 1.00, 0.05, 240, 310
    union all select 'Falls Creek', 1.00, 0.05, 240, 310
    union all select 'Mt Hotham', 1.00, 0.05, 240, 310
)

, season_enriched as (
    select
        r.*
        -- Map the microclimate variables directly from the CTE
        , coalesce(m.base_mod, 1.0) as base_mod
        , coalesce(m.wind_factor, 0.05) as wind_factor
        , coalesce(m.wind_dir_min_deg, 0.0) as wind_dir_min_deg
        , coalesce(m.wind_dir_max_deg, 360.0) as wind_dir_max_deg
        -- Calculate elevation delta from ERA5 grid
        , greatest((r.resort_elevation_m - r.grid_elevation_m) / 1000.0, 0.0) as elevation_delta_km
    from raw_weather r
    left join resort_microclimates m on r.location = m.location_match
)

, orographic_and_lapse as (
    select
        *
        -- Apply Saturated Adiabatic Lapse Rate (5.0 C/km) to ERA5 grid
        , temperature_mean_c - (5.0 * elevation_delta_km) as adj_temp_mean_c
        , temperature_max_c - (5.0 * elevation_delta_km) as adj_temp_max_c
        , temperature_min_c - (5.0 * elevation_delta_km) as adj_temp_min_c
        -- Apply Wind-Sector Uplift
        , case
            when wind_direction_dominant_deg between wind_dir_min_deg and wind_dir_max_deg
            then precipitation_mm * (base_mod + (coalesce(wind_speed_max_ms, 0.0) * wind_factor))
            else precipitation_mm * base_mod
        end as uplifted_precip_mm
    from season_enriched
)

, wet_bulb_bounds as (
    select
        *
        -- Wet-Bulb for Minimum Temp
        , adj_temp_min_c * atan(0.151977 * pow(relative_humidity_mean_pct + 8.313659, 0.5))
          + atan(adj_temp_min_c + relative_humidity_mean_pct)
          - atan(relative_humidity_mean_pct - 1.676331)
          + 0.00391838 * pow(relative_humidity_mean_pct, 1.5) * atan(0.023101 * relative_humidity_mean_pct)
          - 4.686035 as wb_min_c

        -- Wet-Bulb for Maximum Temp
        , adj_temp_max_c * atan(0.151977 * pow(relative_humidity_mean_pct + 8.313659, 0.5))
          + atan(adj_temp_max_c + relative_humidity_mean_pct)
          - atan(relative_humidity_mean_pct - 1.676331)
          + 0.00391838 * pow(relative_humidity_mean_pct, 1.5) * atan(0.023101 * relative_humidity_mean_pct)
          - 4.686035 as wb_max_c
    from orographic_and_lapse
)

, modeled_snow as (
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

, base_metrics as (
    select
        *
        , (snow_frac_min + snow_frac_max) / 2.0 as daily_snow_fraction
        -- Depth delta for the depth/precip blend model
        , greatest(coalesce(snow_depth_cm, 0.0) - lag(coalesce(snow_depth_cm, 0.0), 1, 0.0) over (partition by location order by weather_date), 0.0) as daily_depth_change_cm
    from modeled_snow
)

select
    weather_date as date
    , location as ski_field
    , country
    , resort_elevation_m
    , grid_elevation_m
    , elevation_delta_km
    , base_mod as microclimate_base_mod
    , wind_factor as microclimate_wind_factor
    , precipitation_mm
    , temperature_mean_c
    , temperature_min_c
    , temperature_max_c
    , relative_humidity_mean_pct
    , wind_speed_max_ms
    , wind_direction_dominant_deg
    , snow_depth_cm
    , daily_depth_change_cm
    , uplifted_precip_mm
    , adj_temp_mean_c
    , wb_min_c
    , wb_max_c
    , daily_snow_fraction as snow_fraction
    , snow_liquid_ratio
    
    -- Plotting lines
    , om_direct_snowfall_cm
    , greatest((om_direct_snowfall_cm * 1.10) + (daily_depth_change_cm * 0.30), 0.0) as depth_precip_blend_cm

from base_metrics
order by location, weather_date