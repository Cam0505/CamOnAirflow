{{ config(materialized='table') }}

/*
NZ-focused daily ice quality estimate.

This staging model combines:
- spectral features from base_spectral_daily_features
- weather/terrain features from base_weather_daily_ice_features
- NZ-tuned sentinel category logic from scripts/sentinel_visual.py

Output grain: location x date (daily)
*/

with spectral as (
    select
        spectral_date as obs_date
        , location
        , month_col
        , ndsi
        , ndsi_smooth
        , ndsi_best
        , ndwi
        , ndwi_smooth
        , ndwi_best
        , ndii
        , ndii_smooth
        , ndii_best
        , ndsi_minus_ndwi
        , ndsi_minus_ndii
        , ndwi_minus_ndii
        , ndsi_change_vs_prev_obs
        , ndwi_change_vs_prev_obs
        , ndii_change_vs_prev_obs
        , ndsi_dayofyear_anomaly
        , ndwi_dayofyear_anomaly
        , ndii_dayofyear_anomaly
        , ndsi_out_of_expected_range_flag
        , ndwi_out_of_expected_range_flag
        , ndii_out_of_expected_range_flag
        , all_indices_null_flag
    from {{ ref('base_spectral_daily_features') }}
)

, weather as (
    select
        weather_date as obs_date
        , location
        , country
        , timezone
        , site_elevation_m
        , site_slope
        , hourly_obs_count
        , temperature_mean_c
        , temperature_min_c
        , temperature_max_c
        , temperature_change_vs_prev_day_c
        , precipitation_total_mm
        , precipitation_change_vs_prev_day_mm
        , rain_total_mm
        , snowfall_total_cm
        , snow_depth_mean_m
        , shortwave_energy_mj_m2
        , cloudcover_mean_pct
        , cloud_cover_low_mean_pct
        , freezing_hours
        , hard_freeze_hours
        , thaw_hours
        , wet_freeze_hours
        , supercooled_risk_hours
        , freeze_thaw_cycles
        , wind_chill_mean_c
        , vapor_pressure_deficit_mean_kpa
        , freezing_hour_ratio
        , wet_freeze_hour_ratio
        , supercooled_risk_hour_ratio
        , temperature_mean_3d_avg_c
        , precipitation_total_3d_mm
        , snowfall_total_3d_cm
        , freezing_hours_3d
        , wet_freeze_hours_3d
        , freeze_thaw_cycles_7d
    from {{ ref('base_weather_daily_ice_features') }}
    where upper(country) in ('NZ', 'NEW ZEALAND')
)

, joined as (
    select
        s.obs_date
        , s.location
        , w.country
        , w.timezone
        , w.site_elevation_m
        , w.site_slope

        , s.month_col
        , s.ndsi
        , s.ndsi_smooth
        , s.ndsi_best
        , s.ndwi
        , s.ndwi_smooth
        , s.ndwi_best
        , s.ndii
        , s.ndii_smooth
        , s.ndii_best
        , s.ndsi_minus_ndwi
        , s.ndsi_minus_ndii
        , s.ndwi_minus_ndii
        , s.ndsi_change_vs_prev_obs
        , s.ndwi_change_vs_prev_obs
        , s.ndii_change_vs_prev_obs
        , s.ndsi_dayofyear_anomaly
        , s.ndwi_dayofyear_anomaly
        , s.ndii_dayofyear_anomaly
        , s.ndsi_out_of_expected_range_flag
        , s.ndwi_out_of_expected_range_flag
        , s.ndii_out_of_expected_range_flag
        , s.all_indices_null_flag

        , w.hourly_obs_count
        , w.temperature_mean_c
        , w.temperature_min_c
        , w.temperature_max_c
        , w.temperature_change_vs_prev_day_c
        , w.precipitation_total_mm
        , w.precipitation_change_vs_prev_day_mm
        , w.rain_total_mm
        , w.snowfall_total_cm
        , w.snow_depth_mean_m
        , w.shortwave_energy_mj_m2
        , w.cloudcover_mean_pct
        , w.cloud_cover_low_mean_pct
        , w.freezing_hours
        , w.hard_freeze_hours
        , w.thaw_hours
        , w.wet_freeze_hours
        , w.supercooled_risk_hours
        , w.freeze_thaw_cycles
        , w.wind_chill_mean_c
        , w.vapor_pressure_deficit_mean_kpa
        , w.freezing_hour_ratio
        , w.wet_freeze_hour_ratio
        , w.supercooled_risk_hour_ratio
        , w.temperature_mean_3d_avg_c
        , w.precipitation_total_3d_mm
        , w.snowfall_total_3d_cm
        , w.freezing_hours_3d
        , w.wet_freeze_hours_3d
        , w.freeze_thaw_cycles_7d
    from spectral as s
    inner join weather as w
        on s.location = w.location
        and s.obs_date = w.obs_date
)

, with_nz_sentinel_class as (
    select
        *
        , coalesce(ndsi, ndsi_smooth) as ndsi_class_input
        , coalesce(ndwi, ndwi_smooth) as ndwi_class_input
        , coalesce(ndii, ndii_smooth) as ndii_class_input
        , 0.26 - (case when month_col in (8, 9, 10) then 0.04 else 0.00 end) as ndsi_nz_threshold_main
        , case
            when coalesce(ndsi, ndsi_smooth) > (0.26 - (case when month_col in (8, 9, 10) then 0.04 else 0.00 end)) then
                case
                    when coalesce(ndii, ndii_smooth) < 0.28 and coalesce(ndwi, ndwi_smooth) < 0.04 then 'Dry/Brittle Ice'
                    when coalesce(ndii, ndii_smooth) < 0.52 and coalesce(ndwi, ndwi_smooth) < 0.14 then 'Good Ice Conditions'
                    when coalesce(ndii, ndii_smooth) >= 0.60 or coalesce(ndwi, ndwi_smooth) >= 0.16 then 'Wet/Thawing Ice'
                    else 'Uncertain Ice'
                end
            when coalesce(ndsi, ndsi_smooth) > 0.10 and coalesce(ndsi, ndsi_smooth) <= (0.26 - (case when month_col in (8, 9, 10) then 0.04 else 0.00 end)) then
                case
                    when coalesce(ndii, ndii_smooth) < 0.68 and coalesce(ndwi, ndwi_smooth) < 0.30 then 'Patchy Ice/Snow'
                    else 'Patchy & Wet'
                end
            else 'Bare Rock or Error'
        end as ice_condition_category_nz
    from joined
)

, with_scores as (
    select
        *

        , case ice_condition_category_nz
            when 'Good Ice Conditions' then 0.95
            when 'Dry/Brittle Ice' then 0.78
            when 'Patchy Ice/Snow' then 0.58
            when 'Uncertain Ice' then 0.50
            when 'Patchy & Wet' then 0.35
            when 'Wet/Thawing Ice' then 0.22
            else 0.05
        end as spectral_ice_score_0_1

        , least(
            1.0
            , greatest(
                0.0
                , (
                    (coalesce(freezing_hour_ratio, 0.0) * 0.35)
                    + (coalesce(wet_freeze_hour_ratio, 0.0) * 0.25)
                    + (coalesce(supercooled_risk_hour_ratio, 0.0) * 0.10)
                    + (least(coalesce(freezing_hours_3d, 0.0), 72.0) / 72.0 * 0.20)
                    + (least(coalesce(freeze_thaw_cycles_7d, 0.0), 14.0) / 14.0 * 0.10)
                )
            )
        ) as freeze_support_score_0_1

        , least(
            1.0
            , greatest(
                0.0
                , (
                    (greatest(coalesce(temperature_max_c, 0.0), 0.0) / 12.0 * 0.35)
                    + (greatest(coalesce(rain_total_mm, 0.0), 0.0) / 20.0 * 0.25)
                    + (greatest(coalesce(thaw_hours, 0.0), 0.0) / 24.0 * 0.20)
                    + (greatest(coalesce(shortwave_energy_mj_m2, 0.0), 0.0) / 30.0 * 0.20)
                )
            )
        ) as melt_pressure_score_0_1

        , case
            when all_indices_null_flag = 1 then 0.0
            else greatest(
                0.0
                , least(
                    1.0
                    , 1.0
                      - (coalesce(ndsi_out_of_expected_range_flag, 0) * 0.34)
                      - (coalesce(ndwi_out_of_expected_range_flag, 0) * 0.33)
                      - (coalesce(ndii_out_of_expected_range_flag, 0) * 0.33)
                )
            )
        end as spectral_quality_score_0_1
    from with_nz_sentinel_class
)

, final as (
    select
        obs_date
        , location
        , country
        , timezone
        , site_elevation_m
        , site_slope

        , month_col
        , ndsi_nz_threshold_main

        , ndsi
        , ndsi_smooth
        , ndsi_best
        , ndwi
        , ndwi_smooth
        , ndwi_best
        , ndii
        , ndii_smooth
        , ndii_best

        , ice_condition_category_nz
        , spectral_ice_score_0_1
        , freeze_support_score_0_1
        , melt_pressure_score_0_1
        , spectral_quality_score_0_1

        , greatest(
            0.0
            , least(
                1.0
                , (
                    spectral_ice_score_0_1 * 0.50
                    + freeze_support_score_0_1 * 0.25
                    + (1.0 - melt_pressure_score_0_1) * 0.25
                ) * (0.70 + 0.30 * spectral_quality_score_0_1)
            )
        ) as ice_quality_estimate_score_0_1

        , case
            when all_indices_null_flag = 1 then 'Unknown / Missing Spectral Signal'
            when (
                greatest(
                    0.0
                    , least(
                        1.0
                        , (
                            spectral_ice_score_0_1 * 0.50
                            + freeze_support_score_0_1 * 0.25
                            + (1.0 - melt_pressure_score_0_1) * 0.25
                        ) * (0.70 + 0.30 * spectral_quality_score_0_1)
                    )
                )
            ) >= 0.80 then 'Excellent Ice'
            when (
                greatest(
                    0.0
                    , least(
                        1.0
                        , (
                            spectral_ice_score_0_1 * 0.50
                            + freeze_support_score_0_1 * 0.25
                            + (1.0 - melt_pressure_score_0_1) * 0.25
                        ) * (0.70 + 0.30 * spectral_quality_score_0_1)
                    )
                )
            ) >= 0.62 then 'Good Ice'
            when (
                greatest(
                    0.0
                    , least(
                        1.0
                        , (
                            spectral_ice_score_0_1 * 0.50
                            + freeze_support_score_0_1 * 0.25
                            + (1.0 - melt_pressure_score_0_1) * 0.25
                        ) * (0.70 + 0.30 * spectral_quality_score_0_1)
                    )
                )
            ) >= 0.42 then 'Marginal / Variable Ice'
            when (
                greatest(
                    0.0
                    , least(
                        1.0
                        , (
                            spectral_ice_score_0_1 * 0.50
                            + freeze_support_score_0_1 * 0.25
                            + (1.0 - melt_pressure_score_0_1) * 0.25
                        ) * (0.70 + 0.30 * spectral_quality_score_0_1)
                    )
                )
            ) >= 0.25 then 'Poor / Thawing Ice'
            else 'No Usable Ice'
        end as ice_quality_bucket_nz

        , ndsi_minus_ndwi
        , ndsi_minus_ndii
        , ndwi_minus_ndii
        , ndsi_change_vs_prev_obs
        , ndwi_change_vs_prev_obs
        , ndii_change_vs_prev_obs
        , ndsi_dayofyear_anomaly
        , ndwi_dayofyear_anomaly
        , ndii_dayofyear_anomaly

        , hourly_obs_count
        , temperature_mean_c
        , temperature_min_c
        , temperature_max_c
        , temperature_change_vs_prev_day_c
        , precipitation_total_mm
        , precipitation_change_vs_prev_day_mm
        , rain_total_mm
        , snowfall_total_cm
        , snow_depth_mean_m
        , shortwave_energy_mj_m2
        , cloudcover_mean_pct
        , cloud_cover_low_mean_pct
        , freezing_hours
        , hard_freeze_hours
        , thaw_hours
        , wet_freeze_hours
        , supercooled_risk_hours
        , freeze_thaw_cycles
        , wind_chill_mean_c
        , vapor_pressure_deficit_mean_kpa
        , freezing_hour_ratio
        , wet_freeze_hour_ratio
        , supercooled_risk_hour_ratio
        , temperature_mean_3d_avg_c
        , precipitation_total_3d_mm
        , snowfall_total_3d_cm
        , freezing_hours_3d
        , wet_freeze_hours_3d
        , freeze_thaw_cycles_7d

        , ndsi_out_of_expected_range_flag
        , ndwi_out_of_expected_range_flag
        , ndii_out_of_expected_range_flag
        , all_indices_null_flag
    from with_scores
)

select *
from final
order by location, obs_date