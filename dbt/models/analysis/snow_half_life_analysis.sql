{{ config(materialized='table') }}

/*
Snow Half-Life Annual Index Analysis

Year-over-year index analysis of snow melt patterns:
- Annual averages for index calculation
- 1978 baseline comparison (1978 = 100, like inflation index)
- Long-term trend analysis
- Year-over-year percentage changes
*/

WITH source_data AS (
    SELECT 
        date,
        location,
        snowfall,
        temperature_mean,
        country,
        avg_snow_depth,
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month
    FROM {{ source('snowfall', 'ski_field_snowfall') }}
    WHERE avg_snow_depth IS NOT NULL 
      AND avg_snow_depth > 0
      AND month BETWEEN 6 AND 11  -- Winter months (Southern Hemisphere)
),

-- Identify snow accumulation periods and decay periods
snow_periods AS (
    SELECT 
        *,
        -- Detect new snowfall events (threshold > 1mm to filter noise)
        CASE WHEN snowfall > 1.0 THEN 1 ELSE 0 END as new_snow_flag,
        
        -- Calculate daily change in snow depth
        avg_snow_depth - LAG(avg_snow_depth, 1) OVER (
            PARTITION BY location 
            ORDER BY date
        ) as daily_snow_change,
        
        -- Previous day's snow depth
        LAG(avg_snow_depth, 1) OVER (
            PARTITION BY location 
            ORDER BY date
        ) as prev_snow_depth
        
    FROM source_data
),

-- Create decay period groups
decay_periods AS (
    SELECT 
        *,
        -- Group consecutive days without new snowfall
        SUM(new_snow_flag) OVER (
            PARTITION BY location 
            ORDER BY date 
            ROWS UNBOUNDED PRECEDING
        ) as snow_period_id,
        
        -- Identify if this is a decay day (no new snow and decreasing depth)
        CASE 
            WHEN new_snow_flag = 0 
             AND daily_snow_change < 0 
             AND avg_snow_depth > 0 
            THEN 1 
            ELSE 0 
        END as is_decay_day
        
    FROM snow_periods
),

-- Find peak snow depth at start of each decay period
decay_peaks AS (
    SELECT 
        location,
        snow_period_id,
        MIN(date) as period_start_date,
        MAX(date) as period_end_date,
        
        -- Peak snow depth (max depth in the period)
        MAX(avg_snow_depth) as peak_snow_depth,
        
        -- Date of peak
        FIRST_VALUE(date) OVER (
            PARTITION BY location, snow_period_id 
            ORDER BY avg_snow_depth DESC 
            ROWS UNBOUNDED PRECEDING
        ) as peak_date,
        
        -- Final snow depth in period
        MIN(avg_snow_depth) as final_snow_depth,
        
        -- Count decay days in period
        SUM(is_decay_day) as decay_days,
        
        -- Average temperature during decay
        AVG(CASE WHEN is_decay_day = 1 THEN temperature_mean END) as avg_decay_temp
        
    FROM decay_periods
    WHERE snow_period_id > 0  -- Exclude initial period before first snowfall
    GROUP BY location, snow_period_id
    HAVING SUM(is_decay_day) >= 3  -- Minimum 3 days of decay to be valid
),

-- Calculate half-life events (periods that reach 50% decay)
half_life_events AS (
    SELECT 
        dp.*,
        -- Calculate 50% threshold
        peak_snow_depth * 0.5 as half_depth_threshold,
        
        -- Check if period reached 50% decay
        CASE 
            WHEN final_snow_depth <= (peak_snow_depth * 0.5) 
            THEN 1 
            ELSE 0 
        END as reached_half_life,
        
        -- Estimate days to half-life (linear interpolation)
        CASE 
            WHEN final_snow_depth <= (peak_snow_depth * 0.5)
            THEN decay_days * (
                (peak_snow_depth - (peak_snow_depth * 0.5)) / 
                (peak_snow_depth - final_snow_depth)
            )
            ELSE NULL
        END as days_to_half_life
        
    FROM decay_peaks
    WHERE peak_snow_depth >= 10  -- Minimum 10cm peak to be significant
),

-- Calculate detailed half-life timing using daily data
detailed_half_life AS (
    SELECT 
        he.location,
        he.snow_period_id,
        he.peak_snow_depth,
        he.half_depth_threshold,
        he.period_start_date,
        he.period_end_date,
        he.avg_decay_temp,
        
        -- Find exact date when snow depth crossed 50% threshold
        MIN(
            CASE 
                WHEN dp.avg_snow_depth <= he.half_depth_threshold 
                THEN dp.date 
            END
        ) as half_life_date,
        
        -- Calculate exact days to half-life
        MIN(
            CASE 
                WHEN dp.avg_snow_depth <= he.half_depth_threshold 
                THEN dp.date - he.peak_date 
            END
        ) as exact_days_to_half_life
        
    FROM half_life_events he
    JOIN decay_periods dp ON he.location = dp.location 
                          AND he.snow_period_id = dp.snow_period_id
    WHERE he.reached_half_life = 1
    GROUP BY he.location, he.snow_period_id, he.peak_snow_depth, 
             he.half_depth_threshold, he.period_start_date, 
             he.period_end_date, he.avg_decay_temp, he.peak_date
),

-- Annual aggregations for index calculation
annual_half_life AS (
    SELECT 
        location,
        EXTRACT(YEAR FROM period_start_date) as year,
        COUNT(*) as half_life_events_count,
        AVG(exact_days_to_half_life) as avg_days_to_half_life,
        MEDIAN(exact_days_to_half_life) as median_days_to_half_life,
        AVG(avg_decay_temp) as avg_annual_decay_temp,
        AVG(peak_snow_depth) as avg_annual_peak_depth,
        STDDEV(exact_days_to_half_life) as stddev_annual_half_life
        
    FROM detailed_half_life
    WHERE exact_days_to_half_life IS NOT NULL
    GROUP BY location, EXTRACT(YEAR FROM period_start_date)
    HAVING COUNT(*) >= 2  -- Minimum 2 half-life events per year for reliable average
),

-- Calculate 1978 baseline for index (or earliest available year if 1978 not available)
baseline_calculation AS (
    SELECT 
        location,
        MIN(year) as first_available_year,
        -- Use 1978 if available, otherwise use first available year
        CASE 
            WHEN MIN(CASE WHEN year = 1978 THEN year END) IS NOT NULL THEN 1978
            ELSE MIN(year)
        END as baseline_year
    FROM annual_half_life
    GROUP BY location
),

baseline_half_life AS (
    SELECT 
        ahl.location,
        bc.baseline_year,
        ahl.avg_days_to_half_life as baseline_half_life
    FROM annual_half_life ahl
    JOIN baseline_calculation bc ON ahl.location = bc.location 
                                AND ahl.year = bc.baseline_year
),

-- Calculate multi-year rolling averages for smoothing
rolling_averages AS (
    SELECT 
        location,
        year,
        avg_days_to_half_life,
        -- 3-year centered rolling average
        AVG(avg_days_to_half_life) OVER (
            PARTITION BY location 
            ORDER BY year 
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        ) as three_year_avg,
        -- 5-year centered rolling average
        AVG(avg_days_to_half_life) OVER (
            PARTITION BY location 
            ORDER BY year 
            ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
        ) as five_year_avg
    FROM annual_half_life
),

-- Final output with index calculations
final_annual_index AS (
    SELECT 
        ahl.location,
        ahl.year,
        ahl.half_life_events_count,
        ahl.avg_days_to_half_life,
        ahl.median_days_to_half_life,
        ahl.stddev_annual_half_life,
        ahl.avg_annual_decay_temp,
        ahl.avg_annual_peak_depth,
        
        -- Baseline information
        bhl.baseline_year,
        bhl.baseline_half_life,
        
        -- Rolling averages
        ra.three_year_avg,
        ra.five_year_avg,
        
        -- Index-based half-life (baseline year = 100)
        CASE 
            WHEN bhl.baseline_half_life IS NOT NULL AND bhl.baseline_half_life > 0
            THEN (ahl.avg_days_to_half_life / bhl.baseline_half_life) * 100
            ELSE NULL
        END as half_life_index,
        
        -- Index using 3-year rolling average (more stable)
        CASE 
            WHEN bhl.baseline_half_life IS NOT NULL AND bhl.baseline_half_life > 0
            THEN (ra.three_year_avg / bhl.baseline_half_life) * 100
            ELSE NULL
        END as half_life_index_3yr_smooth,
        
        -- Year-over-year change
        LAG(ahl.avg_days_to_half_life, 1) OVER (
            PARTITION BY ahl.location 
            ORDER BY ahl.year
        ) as prev_year_half_life,
        
        -- Calculate percentage change from previous year
        CASE 
            WHEN LAG(ahl.avg_days_to_half_life, 1) OVER (
                PARTITION BY ahl.location 
                ORDER BY ahl.year
            ) > 0
            THEN ((ahl.avg_days_to_half_life - LAG(ahl.avg_days_to_half_life, 1) OVER (
                PARTITION BY ahl.location 
                ORDER BY ahl.year
            )) / LAG(ahl.avg_days_to_half_life, 1) OVER (
                PARTITION BY ahl.location 
                ORDER BY ahl.year
            )) * 100
            ELSE NULL
        END as year_over_year_change_pct,
        
        -- Long-term trend: change from baseline
        CASE 
            WHEN bhl.baseline_half_life IS NOT NULL AND bhl.baseline_half_life > 0
            THEN ((ahl.avg_days_to_half_life - bhl.baseline_half_life) / bhl.baseline_half_life) * 100
            ELSE NULL
        END as change_from_baseline_pct
        
    FROM annual_half_life ahl
    LEFT JOIN baseline_half_life bhl ON ahl.location = bhl.location
    LEFT JOIN rolling_averages ra ON ahl.location = ra.location AND ahl.year = ra.year
)

SELECT 
    location,
    year,
    half_life_events_count,
    
    -- Core annual metrics
    ROUND(avg_days_to_half_life, 2) as avg_days_to_half_life,
    ROUND(median_days_to_half_life, 2) as median_days_to_half_life,
    ROUND(stddev_annual_half_life, 2) as stddev_annual_half_life,
    
    -- Rolling averages for trend analysis
    ROUND(three_year_avg, 2) as three_year_rolling_avg,
    ROUND(five_year_avg, 2) as five_year_rolling_avg,
    
    -- Index metrics (baseline = 100)
    baseline_year,
    ROUND(baseline_half_life, 2) as baseline_half_life,
    ROUND(half_life_index, 1) as half_life_index,
    ROUND(half_life_index_3yr_smooth, 1) as half_life_index_3yr_smooth,
    
    -- Change metrics
    ROUND(year_over_year_change_pct, 1) as year_over_year_change_pct,
    ROUND(change_from_baseline_pct, 1) as change_from_baseline_pct,
    
    -- Supporting environmental metrics
    ROUND(avg_annual_decay_temp, 1) as avg_annual_decay_temp,
    ROUND(avg_annual_peak_depth, 1) as avg_annual_peak_depth_cm,
    
    -- Data quality indicators
    CASE 
        WHEN half_life_events_count >= 8 THEN 'High'
        WHEN half_life_events_count >= 5 THEN 'Medium' 
        WHEN half_life_events_count >= 2 THEN 'Low'
        ELSE 'Very Low'
    END as data_quality_rating

FROM final_annual_index
ORDER BY location, year
