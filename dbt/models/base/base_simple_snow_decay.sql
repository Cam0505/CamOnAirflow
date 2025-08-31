{{ config(materialized='table') }}

/*
Simple Snow Decay Analysis

Basic analysis of snow melt patterns:
- Daily decay rates when snow is melting
- Temperature correlation
- Annual comparisons to baseline year
*/


WITH daily_snow AS (
    SELECT
        date
        , location
        , country
        , EXTRACT(YEAR FROM date) AS year
        , AVG(temperature_mean) AS temperature_mean
        , AVG(avg_snow_depth) AS avg_snow_depth
    FROM {{ source('snowfall', 'ski_field_snowfall') }}
    WHERE
        avg_snow_depth IS NOT NULL
        AND avg_snow_depth > 0
        AND EXTRACT(MONTH FROM date) BETWEEN 6 AND 11  -- Winter months (Southern Hemisphere)
    GROUP BY date, location, country, EXTRACT(YEAR FROM date)
)

, snow_with_changes AS (
    SELECT
        *
        , temperature_mean AS avg_daily_decay_temp
        , LAG(avg_snow_depth, 1) OVER (PARTITION BY location ORDER BY date) AS prev_snow_depth

        -- Identify decay days (snow decreasing)
        , avg_snow_depth - LAG(avg_snow_depth, 1) OVER (PARTITION BY location ORDER BY date) AS daily_snow_change

        , CASE
            WHEN avg_snow_depth - LAG(avg_snow_depth, 1) OVER (PARTITION BY location ORDER BY date) < -0.001 -- At least 1mm loss
                THEN 1
            ELSE 0
        END AS is_decay_day

        -- Calculate percentage change
        , CASE
            WHEN LAG(avg_snow_depth, 1) OVER (PARTITION BY location ORDER BY date) > 0
                THEN (
                    (avg_snow_depth - LAG(avg_snow_depth, 1) OVER (PARTITION BY location ORDER BY date))
                    / LAG(avg_snow_depth, 1) OVER (PARTITION BY location ORDER BY date)
                ) * 100
        END AS daily_pct_change

    FROM daily_snow
)

-- Annual decay statistics
-- Annual snowfall statistics
, annual_snowfall_stats AS (
    SELECT
        location
        , country
        , EXTRACT(YEAR FROM date) AS year
        , SUM(snowfall) AS total_annual_snowfall
        , AVG(temperature_mean) AS avg_temperature
    FROM {{ source('snowfall', 'ski_field_snowfall') }}
    WHERE EXTRACT(MONTH FROM date) BETWEEN 6 AND 11
    GROUP BY location, country, EXTRACT(YEAR FROM date)
)

, annual_decay_stats AS (
    SELECT
        location
        , year
        , country
        -- Count metrics
        , COUNT(*) AS total_snow_days
        , SUM(is_decay_day) AS decay_days
        , ROUND(SUM(is_decay_day)::FLOAT / COUNT(*) * 100, 1) AS decay_day_percentage
        , AVG(avg_daily_decay_temp) AS avg_decay_temp_c

        -- Percentage decay rates
        , AVG(CASE WHEN is_decay_day = 1 THEN ABS(daily_pct_change) END) AS avg_decay_rate_pct_per_day

    FROM snow_with_changes
    WHERE daily_snow_change IS NOT NULL  -- Exclude first day per location
    GROUP BY location, year, country
    HAVING SUM(is_decay_day) >= 10  -- At least 10 decay days per year for reliable stats
)

-- Calculate baseline year for indexing
, baseline_calculation AS (
    SELECT
        location
        , MIN(year) AS baseline_year
    FROM annual_decay_stats
    GROUP BY location
)

, baseline_stats AS (
    SELECT
        ads.location
        , bc.baseline_year
        , ads.avg_decay_rate_pct_per_day AS baseline_decay_rate_pct
        , snow.total_annual_snowfall AS baseline_annual_snowfall
        , snow.avg_temperature AS baseline_annual_temp
    FROM baseline_calculation AS bc
    INNER JOIN annual_decay_stats AS ads
        ON
            bc.location = ads.location
            AND bc.baseline_year = ads.year
    INNER JOIN annual_snowfall_stats
        AS snow ON ads.location = snow.location
    AND ads.year = snow.year
)

SELECT
    ads.location
    , ads.year
    , ads.country
    , ads.total_snow_days
    , ads.decay_days

    -- Decay rate metrics
    , bs.baseline_year

    -- Baseline and index calculations
    , ads.avg_decay_temp_c
    , ROUND(ads.avg_decay_rate_pct_per_day, 1) AS avg_decay_rate_pct_per_day
    , ROUND(bs.baseline_annual_snowfall, 1) AS baseline_annual_snowfall_cm
    , ROUND(bs.baseline_annual_temp, 1) AS baseline_annual_temp_c
    , ROUND(snow.total_annual_snowfall, 1) AS total_annual_snowfall_cm

    -- Index calculations (difference from baseline year, using percentage decay rate)
    , ROUND(snow.avg_temperature, 1) AS avg_annual_temp_c

    , CASE
        WHEN bs.baseline_decay_rate_pct IS NOT NULL
            THEN ROUND(ads.avg_decay_rate_pct_per_day - bs.baseline_decay_rate_pct, 2)
    END AS decay_rate_index

    -- Year-over-year change
    , ROUND(
        CASE
            WHEN LAG(ads.avg_decay_rate_pct_per_day, 1) OVER (PARTITION BY ads.location ORDER BY ads.year) > 0
                THEN (
                    (ads.avg_decay_rate_pct_per_day - LAG(ads.avg_decay_rate_pct_per_day, 1) OVER (PARTITION BY ads.location ORDER BY ads.year))
                    / LAG(ads.avg_decay_rate_pct_per_day, 1) OVER (PARTITION BY ads.location ORDER BY ads.year)
                ) * 100
        END, 1
    ) AS year_over_year_decay_change_pct

    -- Data quality rating
    , CASE
        WHEN ads.decay_days >= 100 THEN 'High'
        WHEN ads.decay_days >= 50 THEN 'Medium'
        WHEN ads.decay_days >= 10 THEN 'Low'
        ELSE 'Very Low'
    END AS data_quality_rating

FROM annual_decay_stats AS ads
LEFT JOIN baseline_stats AS bs ON ads.location = bs.location
LEFT JOIN annual_snowfall_stats AS snow ON ads.location = snow.location AND ads.year = snow.year
ORDER BY ads.location, ads.year
