{{ config(materialized='table') }}

/*
Daily weather feature table for ice-formation analysis.

This model aggregates hourly weather into daily/location grain and engineers
derived columns commonly useful for ice-formation modeling and downstream
fusion with spectral satellite features.
*/

with hourly_base as (
    select
        cast(datetime as timestamp) as weather_ts
        , cast(cast(datetime as timestamp) as date) as weather_date
        , location
        , country
        , timezone
        , temperature_2m
        , relative_humidity_2m
        , dew_point_2m
        , rain
        , snowfall
        , snow_depth
        , weather_code
        , surface_pressure
        , wind_speed_10m
        , wind_direction_10m
        , soil_temperature_0_to_7cm
        , soil_moisture_0_to_7cm
        , is_day
        , sunshine_duration
        , cloud_cover_low
        , shortwave_radiation
        , precipitation
        , cloudcover
        , wind_gusts_10m
        , temp_freezing
        , temp_below_minus5
        , total_precipitation
        , precip_type
        , wind_u
        , wind_v
    from {{ source('weather_analysis', 'weather_hourly_enriched') }}
)

, hourly_enriched as (
    select
        *
        , coalesce(precipitation, total_precipitation, coalesce(rain, 0.0) + coalesce(snowfall, 0.0)) as precip_total_mm
        , case when coalesce(temperature_2m, 999.0) <= 0 then 1 else 0 end as freezing_hour_flag
        , case when coalesce(temperature_2m, 999.0) <= -2 then 1 else 0 end as hard_freeze_hour_flag
        , case when coalesce(temperature_2m, 999.0) > 0 and coalesce(temperature_2m, -999.0) <= 2 then 1 else 0 end as near_freeze_thaw_hour_flag
        , case when coalesce(temperature_2m, 999.0) > 0 then 1 else 0 end as thaw_hour_flag
        , case
            when coalesce(temperature_2m, 999.0) <= 0
                and coalesce(precipitation, total_precipitation, coalesce(rain, 0.0) + coalesce(snowfall, 0.0)) > 0
                then 1
            else 0
        end as wet_freeze_hour_flag
        , case
            when coalesce(temperature_2m, 999.0) <= 1
                and coalesce(temperature_2m, -999.0) >= -8
                and coalesce(relative_humidity_2m, 0.0) >= 85
                then 1
            else 0
        end as supercooled_risk_hour_flag
        , case
            when coalesce(temperature_2m, 999.0) <= 1
                and coalesce(temperature_2m, -999.0) >= -2
                and coalesce(rain, 0.0) > 0
                then 1
            else 0
        end as rain_on_cold_surface_hour_flag
        , case
            when temperature_2m is not null and wind_speed_10m is not null
                and temperature_2m <= 10 and wind_speed_10m > 4.8
                then
                    13.12
                    + (0.6215 * temperature_2m)
                    - (11.37 * power(wind_speed_10m, 0.16))
                    + (0.3965 * temperature_2m * power(wind_speed_10m, 0.16))
            else temperature_2m
        end as wind_chill_c
        , case
            when temperature_2m is not null and dew_point_2m is not null
                then 0.6108 * exp((17.27 * temperature_2m) / (temperature_2m + 237.3))
                     - 0.6108 * exp((17.27 * dew_point_2m) / (dew_point_2m + 237.3))
        end as vapor_pressure_deficit_kpa
        , case when precip_type = 'snow' then 1 else 0 end as snow_precip_hour_flag
        , case when precip_type = 'rain' then 1 else 0 end as rain_precip_hour_flag
        , case when precip_type = 'mixed' then 1 else 0 end as mixed_precip_hour_flag
    from hourly_base
)

, hourly_with_prev as (
    select
        *
        , lag(temperature_2m) over (
            partition by location, weather_date
            order by weather_ts
        ) as prev_hour_temp
    from hourly_enriched
)

, daily_agg as (
    select
        weather_date
        , location
        , country
        , timezone

        -- Hourly coverage
        , count(*) as hourly_obs_count
        , sum(case when temperature_2m is null then 1 else 0 end) as temp_null_hours
        , sum(coalesce(is_day, 0)) as daylight_hours
        , sum(case when coalesce(is_day, 0) = 0 then 1 else 0 end) as night_hours

        -- Temperature
        , avg(temperature_2m) as temperature_mean_c
        , min(temperature_2m) as temperature_min_c
        , max(temperature_2m) as temperature_max_c
        , stddev_pop(temperature_2m) as temperature_stddev_c
        , max(temperature_2m) - min(temperature_2m) as temperature_range_c
        , avg(case when coalesce(is_day, 0) = 1 then temperature_2m end) as daytime_temperature_mean_c
        , avg(case when coalesce(is_day, 0) = 0 then temperature_2m end) as nighttime_temperature_mean_c

        -- Humidity and dewpoint
        , avg(relative_humidity_2m) as humidity_mean_pct
        , min(relative_humidity_2m) as humidity_min_pct
        , max(relative_humidity_2m) as humidity_max_pct
        , stddev_pop(relative_humidity_2m) as humidity_stddev_pct
        , avg(dew_point_2m) as dew_point_mean_c
        , min(dew_point_2m) as dew_point_min_c
        , max(dew_point_2m) as dew_point_max_c
        , avg(temperature_2m - dew_point_2m) as dewpoint_depression_mean_c

        -- Precipitation and snow
        , sum(coalesce(rain, 0.0)) as rain_total_mm
        , sum(coalesce(snowfall, 0.0)) as snowfall_total_cm
        , sum(coalesce(precip_total_mm, 0.0)) as precipitation_total_mm
        , max(coalesce(precip_total_mm, 0.0)) as precipitation_hourly_peak_mm
        , avg(coalesce(snow_depth, 0.0)) as snow_depth_mean_m
        , min(snow_depth) as snow_depth_min_m
        , max(snow_depth) as snow_depth_max_m
        , max(snow_depth) - min(snow_depth) as snow_depth_change_m

        -- Wind and pressure
        , avg(wind_speed_10m) as wind_speed_mean_kmh
        , min(wind_speed_10m) as wind_speed_min_kmh
        , max(wind_speed_10m) as wind_speed_max_kmh
        , stddev_pop(wind_speed_10m) as wind_speed_stddev_kmh
        , avg(wind_gusts_10m) as wind_gust_mean_kmh
        , max(wind_gusts_10m) as wind_gust_max_kmh
        , avg(surface_pressure) as surface_pressure_mean_hpa
        , min(surface_pressure) as surface_pressure_min_hpa
        , max(surface_pressure) as surface_pressure_max_hpa
        , stddev_pop(surface_pressure) as surface_pressure_stddev_hpa

        -- Wind direction summary (circular mean)
        , avg(sin(radians(wind_direction_10m))) as wind_dir_sin_avg
        , avg(cos(radians(wind_direction_10m))) as wind_dir_cos_avg

        -- Radiation and cloud cover
        , avg(shortwave_radiation) as shortwave_radiation_mean_wm2
        , max(shortwave_radiation) as shortwave_radiation_max_wm2
        , sum(coalesce(shortwave_radiation, 0.0)) * 3600.0 / 1000000.0 as shortwave_energy_mj_m2
        , avg(cloudcover) as cloudcover_mean_pct
        , min(cloudcover) as cloudcover_min_pct
        , max(cloudcover) as cloudcover_max_pct
        , avg(cloud_cover_low) as cloud_cover_low_mean_pct
        , min(cloud_cover_low) as cloud_cover_low_min_pct
        , max(cloud_cover_low) as cloud_cover_low_max_pct
        , sum(coalesce(sunshine_duration, 0.0)) / 3600.0 as sunshine_duration_hours

        -- Soil conditions
        , avg(soil_temperature_0_to_7cm) as soil_temperature_0_7cm_mean_c
        , min(soil_temperature_0_to_7cm) as soil_temperature_0_7cm_min_c
        , max(soil_temperature_0_to_7cm) as soil_temperature_0_7cm_max_c
        , avg(soil_moisture_0_to_7cm) as soil_moisture_0_7cm_mean
        , min(soil_moisture_0_to_7cm) as soil_moisture_0_7cm_min
        , max(soil_moisture_0_to_7cm) as soil_moisture_0_7cm_max

        -- Weather code behavior
        , count(distinct weather_code) as weather_code_distinct_count
        , sum(case when weather_code in (71, 73, 75, 77, 85, 86) then 1 else 0 end) as snow_weather_hours
        , sum(case when weather_code in (61, 63, 65, 80, 81, 82) then 1 else 0 end) as rain_weather_hours
        , sum(case when weather_code in (45, 48) then 1 else 0 end) as fog_weather_hours

        -- Ice-formation oriented features
        , sum(freezing_hour_flag) as freezing_hours
        , sum(hard_freeze_hour_flag) as hard_freeze_hours
        , sum(near_freeze_thaw_hour_flag) as near_freeze_thaw_hours
        , sum(thaw_hour_flag) as thaw_hours
        , sum(wet_freeze_hour_flag) as wet_freeze_hours
        , sum(supercooled_risk_hour_flag) as supercooled_risk_hours
        , sum(rain_on_cold_surface_hour_flag) as rain_on_cold_surface_hours
        , sum(snow_precip_hour_flag) as snow_precip_hours
        , sum(rain_precip_hour_flag) as rain_precip_hours
        , sum(mixed_precip_hour_flag) as mixed_precip_hours
        , sum(
            case
                when prev_hour_temp is null then 0
                when (prev_hour_temp <= 0 and temperature_2m > 0)
                    or (prev_hour_temp > 0 and temperature_2m <= 0)
                    then 1
                else 0
            end
        ) as freeze_thaw_cycles
        , avg(wind_chill_c) as wind_chill_mean_c
        , min(wind_chill_c) as wind_chill_min_c
        , avg(vapor_pressure_deficit_kpa) as vapor_pressure_deficit_mean_kpa

        -- Ratios/normalized features
        , cast(sum(freezing_hour_flag) as double) / nullif(count(*), 0) as freezing_hour_ratio
        , cast(sum(wet_freeze_hour_flag) as double) / nullif(count(*), 0) as wet_freeze_hour_ratio
        , cast(sum(supercooled_risk_hour_flag) as double) / nullif(count(*), 0) as supercooled_risk_hour_ratio
    from hourly_with_prev
    group by weather_date, location, country, timezone
)

, with_terrain as (
    select
        d.*
        , lm.elevation as site_elevation_m
        , lm.slope as site_slope
    from daily_agg as d
    left join {{ source('weather_analysis', 'location_metadata') }} as lm
        on d.location = lm.name
)

, final as (
    select
        weather_date
        , location
        , country
        , timezone
        , site_elevation_m
        , site_slope
        , extract(year from weather_date) as year_col
        , extract(month from weather_date) as month_col
        , extract(day from weather_date) as day_col
        , extract(doy from weather_date) as day_of_year

        , hourly_obs_count
        , temp_null_hours
        , daylight_hours
        , night_hours

        , temperature_mean_c
        , temperature_min_c
        , temperature_max_c
        , temperature_stddev_c
        , temperature_range_c
        , daytime_temperature_mean_c
        , nighttime_temperature_mean_c
        , temperature_mean_c - lag(temperature_mean_c) over (partition by location order by weather_date) as temperature_change_vs_prev_day_c

        , humidity_mean_pct
        , humidity_min_pct
        , humidity_max_pct
        , humidity_stddev_pct
        , dew_point_mean_c
        , dew_point_min_c
        , dew_point_max_c
        , dewpoint_depression_mean_c

        , rain_total_mm
        , snowfall_total_cm
        , precipitation_total_mm
        , precipitation_hourly_peak_mm
        , snow_depth_mean_m
        , snow_depth_min_m
        , snow_depth_max_m
        , snow_depth_change_m
        , precipitation_total_mm - lag(precipitation_total_mm) over (partition by location order by weather_date) as precipitation_change_vs_prev_day_mm

        , wind_speed_mean_kmh
        , wind_speed_min_kmh
        , wind_speed_max_kmh
        , wind_speed_stddev_kmh
        , wind_gust_mean_kmh
        , wind_gust_max_kmh
        , surface_pressure_mean_hpa
        , surface_pressure_min_hpa
        , surface_pressure_max_hpa
        , surface_pressure_stddev_hpa
        , mod(degrees(atan2(wind_dir_sin_avg, wind_dir_cos_avg)) + 360, 360) as wind_direction_mean_deg

        , shortwave_radiation_mean_wm2
        , shortwave_radiation_max_wm2
        , shortwave_energy_mj_m2
        , sunshine_duration_hours
        , cloudcover_mean_pct
        , cloudcover_min_pct
        , cloudcover_max_pct
        , cloud_cover_low_mean_pct
        , cloud_cover_low_min_pct
        , cloud_cover_low_max_pct

        , soil_temperature_0_7cm_mean_c
        , soil_temperature_0_7cm_min_c
        , soil_temperature_0_7cm_max_c
        , soil_moisture_0_7cm_mean
        , soil_moisture_0_7cm_min
        , soil_moisture_0_7cm_max

        , weather_code_distinct_count
        , snow_weather_hours
        , rain_weather_hours
        , fog_weather_hours

        , freezing_hours
        , hard_freeze_hours
        , near_freeze_thaw_hours
        , thaw_hours
        , wet_freeze_hours
        , supercooled_risk_hours
        , rain_on_cold_surface_hours
        , snow_precip_hours
        , rain_precip_hours
        , mixed_precip_hours
        , freeze_thaw_cycles
        , wind_chill_mean_c
        , wind_chill_min_c
        , vapor_pressure_deficit_mean_kpa
        , freezing_hour_ratio
        , wet_freeze_hour_ratio
        , supercooled_risk_hour_ratio

        -- Short rolling windows for sequence-aware downstream models
        , avg(temperature_mean_c) over (
            partition by location
            order by weather_date
            rows between 2 preceding and current row
        ) as temperature_mean_3d_avg_c
        , sum(precipitation_total_mm) over (
            partition by location
            order by weather_date
            rows between 2 preceding and current row
        ) as precipitation_total_3d_mm
        , sum(snowfall_total_cm) over (
            partition by location
            order by weather_date
            rows between 2 preceding and current row
        ) as snowfall_total_3d_cm
        , sum(freezing_hours) over (
            partition by location
            order by weather_date
            rows between 2 preceding and current row
        ) as freezing_hours_3d
        , sum(wet_freeze_hours) over (
            partition by location
            order by weather_date
            rows between 2 preceding and current row
        ) as wet_freeze_hours_3d
        , sum(freeze_thaw_cycles) over (
            partition by location
            order by weather_date
            rows between 6 preceding and current row
        ) as freeze_thaw_cycles_7d
    from with_terrain
)

select *
from final
order by location, weather_date