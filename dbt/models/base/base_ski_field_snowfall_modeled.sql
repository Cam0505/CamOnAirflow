WITH snowfall_raw AS (
    SELECT
        CAST(s.date AS DATE) AS datecol
        , s.location AS ski_field
        , s.country
        , COALESCE(
            s.region_profile,
            CASE
                WHEN s.country = 'JP' THEN 'japan_maritime_powder'
                WHEN s.country = 'AU' THEN 'australian_alps'
                WHEN s.country = 'NZ' THEN 'nz_maritime_alpine'
                ELSE NULL
            END
        ) AS region_profile
        , CAST(COALESCE(s.snowfall, 0) AS DOUBLE) AS om_direct_snowfall_cm
        , CAST(COALESCE(s.precipitation_sum, 0) AS DOUBLE) AS precipitation_mm
        , CAST(COALESCE(s.rain_sum, 0) AS DOUBLE) AS rain_mm
        , CAST(COALESCE(s.precipitation_hours, 0) AS DOUBLE) AS precipitation_hours
        , CAST(COALESCE(s.temperature_mean, 0) AS DOUBLE) AS temperature_mean_c
        , CAST(COALESCE(s.temperature_min, 0) AS DOUBLE) AS temperature_min_c
        , CAST(COALESCE(s.temperature_max, 0) AS DOUBLE) AS temperature_max_c
        , CAST(COALESCE(s.relative_humidity_mean, 0) AS DOUBLE) AS relative_humidity_pct
        , CAST(COALESCE(s.snow_depth, 0) AS DOUBLE) AS snow_depth_cm
        , CAST(COALESCE(s.snow_depth_m, COALESCE(s.snow_depth, 0) / 100.0) AS DOUBLE) AS snow_depth_m
        , CAST(COALESCE(s.resort_elevation, l.resort_elevation, 0) AS DOUBLE) AS resort_elevation_m
        , l.lat
        , l.lon
        , l.timezone
    FROM {{ source('snowfall', 'ski_field_snowfall') }} AS s
    LEFT JOIN {{ source('snowfall', 'ski_field_lookup') }} AS l
        ON s.location = l.name
        AND s.country = l.country
    WHERE COALESCE(s.country, '') IN ('NZ', 'AU', 'JP')
)

, profile_constants AS (
    SELECT *
    FROM {{ ref('snowfall_profile_constants') }}
)

, seasonal_features AS (
    SELECT
        r.*
        , c.snow_full_below_c
        , c.rain_all_above_c
        , c.snowpack_gain_factor
        , c.slr_very_cold
        , c.slr_cold
        , c.slr_cool
        , c.slr_near_zero
        , c.slr_marginal
        , c.slr_warm
        , c.humidity_adjust_90
        , c.humidity_adjust_80
        , c.humidity_adjust_60
        , c.cold_bonus_very_cold
        , c.cold_bonus_cold
        , c.cold_bonus_marginal
        , c.cold_bonus_warm
        , c.rain_penalty_low
        , c.rain_penalty_mid
        , c.rain_penalty_high
        , c.pack_bonus_base
        , c.pack_bonus_depth_mult
        , c.pack_bonus_persistent_mult
        , c.depth_proxy_mult
        , c.rain_ratio_slope
        , c.rain_ratio_floor
        , c.storm_bonus_high
        , c.storm_bonus_medium
        , c.shoulder_cold_penalty
        , c.shoulder_mid_penalty
        , c.shoulder_warm_penalty
        , c.deep_pack_bonus_high
        , c.deep_pack_bonus_medium
        , c.cold_storm_signal_bonus
        , c.warm_mixed_storm_penalty_high
        , c.warm_mixed_storm_penalty_medium
        , EXTRACT(MONTH FROM r.datecol) AS month_col
        , CASE
            WHEN r.lat >= 0 AND EXTRACT(MONTH FROM r.datecol) IN (11, 12)
                THEN EXTRACT(YEAR FROM r.datecol) + 1
            ELSE EXTRACT(YEAR FROM r.datecol)
        END AS year_col
        , AVG(COALESCE(r.snow_depth_m, 0.0)) OVER (
            PARTITION BY r.ski_field
            ORDER BY r.datecol
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS persistent_depth_m
        , GREATEST(
            COALESCE(r.snow_depth_cm, 0.0)
                - LAG(COALESCE(r.snow_depth_cm, 0.0), 1, 0.0) OVER (
                    PARTITION BY r.ski_field ORDER BY r.datecol
                )
            , 0.0
        ) AS depth_gain_cm
    FROM snowfall_raw AS r
    LEFT JOIN profile_constants AS c
        ON r.region_profile = c.region_profile
)

, transformed AS (
    SELECT
        *
        , CASE
            WHEN temperature_mean_c <= snow_full_below_c THEN 1.0
            WHEN temperature_mean_c >= rain_all_above_c THEN 0.0
            ELSE GREATEST(
                0.0,
                LEAST(
                    1.0,
                    (rain_all_above_c - temperature_mean_c)
                    / NULLIF(rain_all_above_c - snow_full_below_c, 0.0)
                )
            )
        END AS snow_fraction
        , CASE
            WHEN temperature_mean_c <= -8.0 THEN slr_very_cold
            WHEN temperature_mean_c <= -5.0 THEN slr_cold
            WHEN temperature_mean_c <= -2.0 THEN slr_cool
            WHEN temperature_mean_c <= 0.0 THEN slr_near_zero
            WHEN temperature_mean_c <= 1.5 THEN slr_marginal
            ELSE slr_warm
        END AS slr_base
        , CASE
            WHEN relative_humidity_pct >= 90 THEN humidity_adjust_90
            WHEN relative_humidity_pct >= 80 THEN humidity_adjust_80
            WHEN relative_humidity_pct <= 60 THEN humidity_adjust_60
            ELSE 1.0
        END AS humidity_adjust
    FROM seasonal_features
)

, modeled AS (
    SELECT
        *
        , slr_base * humidity_adjust AS snow_to_liquid_ratio
        , precipitation_mm * snow_fraction * (slr_base * humidity_adjust) / 10.0 AS precip_temp_snowfall_cm
        , CASE
            WHEN precipitation_mm > 0
                THEN GREATEST(0.0, LEAST(1.0, rain_mm / precipitation_mm))
            ELSE 0.0
        END AS rain_ratio
    FROM transformed
)

, final_metrics AS (
    SELECT
        ski_field
        , country
        , region_profile
        , year_col
        , month_col
        , datecol
        , resort_elevation_m
        , lat
        , lon
        , timezone
        , om_direct_snowfall_cm
        , precipitation_mm
        , rain_mm
        , precipitation_hours
        , temperature_mean_c
        , temperature_min_c
        , temperature_max_c
        , relative_humidity_pct
        , snow_depth_m
        , snow_depth_cm
        , persistent_depth_m
        , depth_gain_cm
        , snow_fraction
        , snow_to_liquid_ratio
        , precip_temp_snowfall_cm
        , GREATEST(
            CASE
                WHEN region_profile = 'japan_maritime_powder' THEN
                    precip_temp_snowfall_cm
                    * CASE
                        WHEN temperature_mean_c <= -5.0 THEN cold_bonus_very_cold
                        WHEN temperature_mean_c <= -2.0 THEN cold_bonus_cold
                        WHEN temperature_mean_c <= 0.5 THEN cold_bonus_marginal
                        ELSE cold_bonus_warm
                    END
                    * CASE
                        WHEN snow_fraction < 0.20 THEN rain_penalty_low
                        WHEN snow_fraction < 0.50 THEN rain_penalty_mid
                        WHEN snow_fraction < 0.80 THEN rain_penalty_high
                        ELSE 1.0
                    END
                    * GREATEST(rain_ratio_floor, LEAST(1.02, 1.02 - rain_ratio_slope * rain_ratio))
                    * CASE
                        WHEN precipitation_mm >= 12.0 AND temperature_mean_c <= -1.5 THEN storm_bonus_high
                        WHEN precipitation_mm >= 8.0 AND temperature_mean_c <= 0.0 THEN storm_bonus_medium
                        ELSE 1.0
                    END
                    * CASE
                        WHEN month_col IN (11, 4, 5) AND temperature_mean_c <= -3.0 THEN shoulder_cold_penalty
                        WHEN month_col IN (11, 4, 5) AND temperature_mean_c <= -1.0 THEN shoulder_mid_penalty
                        WHEN month_col IN (11, 4, 5) THEN shoulder_warm_penalty
                        ELSE 1.0
                    END
                    * CASE
                        WHEN month_col IN (12, 1, 2) AND persistent_depth_m >= 1.2 THEN deep_pack_bonus_high
                        WHEN month_col IN (12, 1, 2) AND persistent_depth_m >= 0.8 THEN deep_pack_bonus_medium
                        ELSE 1.0
                    END
                    * CASE
                        WHEN om_direct_snowfall_cm >= 8.0 AND temperature_mean_c <= -2.0 THEN cold_storm_signal_bonus
                        ELSE 1.0
                    END
                    * CASE
                        WHEN precipitation_mm >= 12.0 AND temperature_mean_c > 0.0 THEN warm_mixed_storm_penalty_high
                        WHEN precipitation_mm >= 8.0 AND temperature_mean_c > 0.5 THEN warm_mixed_storm_penalty_medium
                        ELSE 1.0
                    END
                    * (
                        pack_bonus_base
                        + LEAST(snow_depth_m, 2.0) * pack_bonus_depth_mult
                        + LEAST(persistent_depth_m, 2.0) * pack_bonus_persistent_mult
                    )
                    * EXP(0.48 * LEAST(GREATEST(snow_depth_m - 0.8, 0.0), 0.9))
                    + depth_gain_cm * depth_proxy_mult
                ELSE
                    precip_temp_snowfall_cm
                    * CASE
                        WHEN temperature_mean_c <= -5.0 THEN cold_bonus_very_cold
                        WHEN temperature_mean_c <= -2.0 THEN cold_bonus_cold
                        WHEN temperature_mean_c <= 0.75 THEN cold_bonus_marginal
                        ELSE cold_bonus_warm
                    END
                    * CASE
                        WHEN snow_fraction < 0.20 THEN rain_penalty_low
                        WHEN snow_fraction < 0.50 THEN rain_penalty_mid
                        WHEN snow_fraction < 0.80 THEN rain_penalty_high
                        ELSE 1.0
                    END
                    * (
                        pack_bonus_base
                        + LEAST(snow_depth_m, 1.5) * pack_bonus_depth_mult
                        + LEAST(persistent_depth_m, 1.5) * pack_bonus_persistent_mult
                    )
                    + depth_gain_cm * depth_proxy_mult
            END
            , 0.0
        ) AS snowpack_adjusted_snowfall_cm
        , GREATEST(depth_gain_cm * 0.40 + precip_temp_snowfall_cm * 0.60, 0.0) AS depth_precip_blend_cm
    FROM modeled
)

SELECT
    ski_field
    , country
    , region_profile
    , year_col
    , month_col
    , datecol
    , resort_elevation_m
    , lat
    , lon
    , timezone
    , om_direct_snowfall_cm
    , precipitation_mm
    , rain_mm
    , precipitation_hours
    , temperature_mean_c
    , temperature_min_c
    , temperature_max_c
    , relative_humidity_pct
    , snow_depth_m
    , snow_depth_cm
    , persistent_depth_m
    , depth_gain_cm
    , snow_fraction
    , snow_to_liquid_ratio
    , precip_temp_snowfall_cm
    , snowpack_adjusted_snowfall_cm
    , depth_precip_blend_cm
    , snowpack_adjusted_snowfall_cm AS daily_snowfall_cm
FROM final_metrics
ORDER BY ski_field, datecol
