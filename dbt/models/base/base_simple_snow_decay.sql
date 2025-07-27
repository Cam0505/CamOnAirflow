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
        CAST(datetime AS DATE) as date,
        location,
        EXTRACT(YEAR FROM datetime) as year,
        AVG(temperature) as temperature_mean,
        AVG(snow_depth) as avg_snow_depth
    FROM {{ source('snowfall', 'ski_field_snowfall_hourly') }}
    WHERE snow_depth IS NOT NULL 
      AND snow_depth > 0
      AND EXTRACT(MONTH FROM datetime) BETWEEN 6 AND 11  -- Winter months (Southern Hemisphere)
    GROUP BY CAST(datetime AS DATE), location, EXTRACT(YEAR FROM datetime)
),

snow_with_changes AS (
    SELECT 
        *,
        LAG(avg_snow_depth, 1) OVER (PARTITION BY location ORDER BY date) as prev_snow_depth,
        avg_snow_depth - LAG(avg_snow_depth, 1) OVER (PARTITION BY location ORDER BY date) as daily_snow_change,
        
        -- Identify decay days (snow decreasing)
        CASE 
            WHEN avg_snow_depth - LAG(avg_snow_depth, 1) OVER (PARTITION BY location ORDER BY date) < -0.001 -- At least 1mm loss
            THEN 1 
            ELSE 0 
        END as is_decay_day,

        temperature_mean as avg_daily_decay_temp,
        
        -- Calculate percentage change
        CASE 
            WHEN LAG(avg_snow_depth, 1) OVER (PARTITION BY location ORDER BY date) > 0
            THEN ((avg_snow_depth - LAG(avg_snow_depth, 1) OVER (PARTITION BY location ORDER BY date)) 
                  / LAG(avg_snow_depth, 1) OVER (PARTITION BY location ORDER BY date)) * 100
            ELSE NULL
        END as daily_pct_change
        
    FROM daily_snow
),

-- Annual decay statistics
-- Annual snowfall statistics
annual_snowfall_stats AS (
    SELECT 
        location,
        EXTRACT(YEAR FROM datetime) as year,
        SUM(snowfall) as total_annual_snowfall,
        AVG(temperature) as avg_temperature
    FROM {{ source('snowfall', 'ski_field_snowfall_hourly') }}
    where EXTRACT(MONTH FROM datetime) BETWEEN 6 AND 11
    GROUP BY location, EXTRACT(YEAR FROM datetime) 
),

annual_decay_stats AS (
    SELECT 
        location,
        year,
        
        -- Count metrics
        COUNT(*) as total_snow_days,
        SUM(is_decay_day) as decay_days,
        ROUND(SUM(is_decay_day)::FLOAT / COUNT(*) * 100, 1) as decay_day_percentage,
        AVG(avg_daily_decay_temp) as avg_decay_temp_c,
        
        -- Percentage decay rates
        AVG(CASE WHEN is_decay_day = 1 THEN ABS(daily_pct_change) END) as avg_decay_rate_pct_per_day,
        
    FROM snow_with_changes
    WHERE daily_snow_change IS NOT NULL  -- Exclude first day per location
    GROUP BY location, year
    HAVING SUM(is_decay_day) >= 10  -- At least 10 decay days per year for reliable stats
),

-- Calculate baseline year for indexing
baseline_calculation AS (
    SELECT 
        location,
        MIN(year) as baseline_year
    FROM annual_decay_stats
    GROUP BY location
),

baseline_stats AS (
    SELECT 
        ads.location,
        bc.baseline_year,
        ads.avg_decay_rate_pct_per_day as baseline_decay_rate_pct,
        snow.total_annual_snowfall as baseline_annual_snowfall,
        snow.avg_temperature as baseline_annual_temp
    FROM baseline_calculation bc
    JOIN annual_decay_stats ads ON ads.location = bc.location 
                                AND ads.year = bc.baseline_year
    JOIN annual_snowfall_stats snow ON ads.location = snow.location 
                                     AND ads.year = snow.year
)

SELECT 
    ads.location,
    ads.year,
    ads.total_snow_days,
    ads.decay_days,
    
    -- Decay rate metrics
    ROUND(ads.avg_decay_rate_pct_per_day, 1) as avg_decay_rate_pct_per_day,
    
    -- Baseline and index calculations
    bs.baseline_year,
    ROUND(bs.baseline_annual_snowfall, 1) as baseline_annual_snowfall_cm,
    ROUND(bs.baseline_annual_temp, 1) as baseline_annual_temp_c,
    ROUND(snow.total_annual_snowfall, 1) as total_annual_snowfall_cm,
    ROUND(snow.avg_temperature, 1) as avg_annual_temp_c,
    
    -- Index calculations (difference from baseline year, using percentage decay rate)
    CASE 
        WHEN bs.baseline_decay_rate_pct IS NOT NULL
        THEN ROUND(ads.avg_decay_rate_pct_per_day - bs.baseline_decay_rate_pct, 2)
        ELSE NULL
    END as decay_rate_index,

    ads.avg_decay_temp_c as avg_decay_temp_c,
    
    -- Year-over-year change
    ROUND(
        CASE 
            WHEN LAG(ads.avg_decay_rate_pct_per_day, 1) OVER (PARTITION BY ads.location ORDER BY ads.year) > 0
            THEN ((ads.avg_decay_rate_pct_per_day - LAG(ads.avg_decay_rate_pct_per_day, 1) OVER (PARTITION BY ads.location ORDER BY ads.year)) 
                  / LAG(ads.avg_decay_rate_pct_per_day, 1) OVER (PARTITION BY ads.location ORDER BY ads.year)) * 100
            ELSE NULL
        END, 1
    ) as year_over_year_decay_change_pct,
    
    -- Data quality rating
    CASE 
        WHEN ads.decay_days >= 100 THEN 'High'
        WHEN ads.decay_days >= 50 THEN 'Medium' 
        WHEN ads.decay_days >= 10 THEN 'Low'
        ELSE 'Very Low'
    END as data_quality_rating

FROM annual_decay_stats ads
LEFT JOIN baseline_stats bs ON ads.location = bs.location
LEFT JOIN annual_snowfall_stats snow ON ads.location = snow.location AND ads.year = snow.year
ORDER BY ads.location, ads.year
