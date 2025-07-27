{{ config(materialized='table') }}

/*
Snow Half-Life Time Series Analysis

High-granularity analysis of snow melt patterns showing:
- Weekly and monthly averages of half-life duration
- Seasonal trends within winter periods
- Temperature correlation with melt rates
- Detailed temporal patterns for graphing and trend analysis
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
        EXTRACT(MONTH FROM date) as month,
        EXTRACT(WEEK FROM date) as week_of_year,
        EXTRACT(DOY FROM date) as day_of_year
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
        AVG(CASE WHEN is_decay_day = 1 THEN temperature_mean END) as avg_decay_temp,
        
        -- Add temporal fields for grouping
        EXTRACT(YEAR FROM MIN(date)) as year,
        EXTRACT(MONTH FROM MIN(date)) as month,
        EXTRACT(WEEK FROM MIN(date)) as week_of_year
        
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
        he.year,
        he.month,
        he.week_of_year,
        
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
             he.period_end_date, he.avg_decay_temp, he.peak_date,
             he.year, he.month, he.week_of_year
),

-- Weekly aggregations
weekly_half_life AS (
    SELECT 
        location,
        year,
        week_of_year,
        -- Create date for the start of each week for plotting
        DATE_TRUNC('week', MIN(period_start_date)) as week_start_date,
        
        COUNT(*) as half_life_events_count,
        AVG(exact_days_to_half_life) as avg_days_to_half_life,
        MEDIAN(exact_days_to_half_life) as median_days_to_half_life,
        AVG(avg_decay_temp) as avg_weekly_decay_temp,
        AVG(peak_snow_depth) as avg_weekly_peak_depth,
        MIN(exact_days_to_half_life) as min_days_to_half_life,
        MAX(exact_days_to_half_life) as max_days_to_half_life,
        STDDEV(exact_days_to_half_life) as stddev_days_to_half_life
        
    FROM detailed_half_life
    WHERE exact_days_to_half_life IS NOT NULL
    GROUP BY location, year, week_of_year
),

-- Monthly aggregations  
monthly_half_life AS (
    SELECT 
        location,
        year,
        month,
        -- Create proper month start date
        DATE_TRUNC('month', MIN(period_start_date)) as month_start_date,
        
        COUNT(*) as half_life_events_count,
        AVG(exact_days_to_half_life) as avg_days_to_half_life,
        MEDIAN(exact_days_to_half_life) as median_days_to_half_life,
        AVG(avg_decay_temp) as avg_monthly_decay_temp,
        AVG(peak_snow_depth) as avg_monthly_peak_depth,
        MIN(exact_days_to_half_life) as min_days_to_half_life,
        MAX(exact_days_to_half_life) as max_days_to_half_life,
        STDDEV(exact_days_to_half_life) as stddev_days_to_half_life
        
    FROM detailed_half_life
    WHERE exact_days_to_half_life IS NOT NULL
    GROUP BY location, year, month
),

-- Combine weekly and monthly data with time period indicator
final_time_series AS (
    -- Weekly data
    SELECT 
        location,
        year,
        'weekly' as time_granularity,
        week_of_year as time_period,
        week_start_date as period_date,
        half_life_events_count,
        
        -- Time-based metrics (hours for precision)
        ROUND(avg_days_to_half_life, 2) as avg_days_to_half_life,
        ROUND(median_days_to_half_life, 2) as median_days_to_half_life,
        ROUND(avg_days_to_half_life * 24, 1) as avg_hours_to_half_life,
        ROUND(min_days_to_half_life, 2) as min_days_to_half_life,
        ROUND(max_days_to_half_life, 2) as max_days_to_half_life,
        ROUND(stddev_days_to_half_life, 2) as stddev_days_to_half_life,
        
        -- Environmental context
        ROUND(avg_weekly_decay_temp, 1) as avg_decay_temp,
        ROUND(avg_weekly_peak_depth, 1) as avg_peak_depth_cm,
        
        -- Data quality
        CASE 
            WHEN half_life_events_count >= 3 THEN 'High'
            WHEN half_life_events_count >= 2 THEN 'Medium' 
            ELSE 'Low'
        END as data_quality_rating
        
    FROM weekly_half_life
    WHERE half_life_events_count >= 1
    
    UNION ALL
    
    -- Monthly data
    SELECT 
        location,
        year,
        'monthly' as time_granularity,
        month as time_period,
        month_start_date as period_date,
        half_life_events_count,
        
        -- Time-based metrics (hours for precision)
        ROUND(avg_days_to_half_life, 2) as avg_days_to_half_life,
        ROUND(median_days_to_half_life, 2) as median_days_to_half_life,
        ROUND(avg_days_to_half_life * 24, 1) as avg_hours_to_half_life,
        ROUND(min_days_to_half_life, 2) as min_days_to_half_life,
        ROUND(max_days_to_half_life, 2) as max_days_to_half_life,
        ROUND(stddev_days_to_half_life, 2) as stddev_days_to_half_life,
        
        -- Environmental context
        ROUND(avg_monthly_decay_temp, 1) as avg_decay_temp,
        ROUND(avg_monthly_peak_depth, 1) as avg_peak_depth_cm,
        
        -- Data quality
        CASE 
            WHEN half_life_events_count >= 5 THEN 'High'
            WHEN half_life_events_count >= 3 THEN 'Medium' 
            ELSE 'Low'
        END as data_quality_rating
        
    FROM monthly_half_life
    WHERE half_life_events_count >= 1
)

SELECT 
    location,
    year,
    time_granularity,
    time_period,
    period_date,
    half_life_events_count,
    avg_days_to_half_life,
    median_days_to_half_life,
    avg_hours_to_half_life,
    min_days_to_half_life,
    max_days_to_half_life,
    stddev_days_to_half_life,
    avg_decay_temp,
    avg_peak_depth_cm,
    data_quality_rating

FROM final_time_series
ORDER BY location, time_granularity, year, time_period
