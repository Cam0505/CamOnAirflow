-- filepath: /workspaces/CamOnAirFlow/dbt/models/base/base_ski_lift_times.sql
SELECT
    osm_id,
    resort,
    name,
    lift_type,
    lift_length_m,
    lift_speed_mps,
    -- Calculate lift time with comprehensive fallback logic
    CASE
        -- First priority: Use actual speed if available
        WHEN lift_speed_mps IS NOT NULL AND lift_speed_mps > 0 THEN lift_length_m / lift_speed_mps
        
        -- Second priority: Use duration if available and valid
        WHEN duration IS NOT NULL AND TRY_CAST(duration AS DOUBLE) IS NOT NULL AND TRY_CAST(duration AS DOUBLE) > 0 
        THEN TRY_CAST(duration AS DOUBLE)
        
        -- Third priority: Calculate based on lift type and length using typical speeds
        WHEN lift_length_m IS NOT NULL AND lift_length_m > 0 AND lift_type IS NOT NULL THEN
            CASE 
                WHEN LOWER(lift_type) IN ('gondola') THEN lift_length_m / 5.0  -- 5 m/s typical
                WHEN LOWER(lift_type) IN ('chair_lift', 'yes') THEN lift_length_m / 2.5  -- 2.5 m/s typical
                WHEN LOWER(lift_type) IN ('j-bar', 'drag_lift', 'platter', 't-bar', 'rope_tow') THEN lift_length_m / 3.0  -- 3 m/s typical
                WHEN LOWER(lift_type) IN ('magic_carpet') THEN lift_length_m / 1.5  -- 1.5 m/s typical
                WHEN LOWER(lift_type) IN ('mixed_lift') THEN lift_length_m / 3.5  -- 3.5 m/s typical
                WHEN LOWER(lift_type) IN ('station', 'goods') THEN lift_length_m / 4.0  -- 4 m/s typical
                ELSE lift_length_m / 2.5  -- Default chairlift speed
            END
            
        -- Fourth priority: Use fixed defaults based on lift type when length is unavailable
        WHEN lift_type IS NOT NULL THEN
            CASE 
                WHEN LOWER(lift_type) IN ('gondola') THEN 480  -- 8 minutes
                WHEN LOWER(lift_type) IN ('chair_lift', 'yes') THEN 360  -- 6 minutes
                WHEN LOWER(lift_type) IN ('j-bar', 'drag_lift', 'platter', 't-bar', 'rope_tow') THEN 180  -- 3 minutes
                WHEN LOWER(lift_type) IN ('magic_carpet') THEN 60  -- 1 minute
                WHEN LOWER(lift_type) IN ('mixed_lift') THEN 240  -- 4 minutes
                WHEN LOWER(lift_type) IN ('station', 'goods') THEN 300  -- 5 minutes
                ELSE 300  -- 5 minutes default
            END
            
        -- Final fallback: 5 minutes default
        ELSE 300
    END AS lift_time_sec,
    
    -- Add derived speed for analysis
    CASE
        WHEN lift_speed_mps IS NOT NULL AND lift_speed_mps > 0 THEN lift_speed_mps
        WHEN lift_length_m IS NOT NULL AND lift_length_m > 0 AND lift_type IS NOT NULL THEN
            CASE 
                WHEN LOWER(lift_type) IN ('gondola') THEN 5.0
                WHEN LOWER(lift_type) IN ('chair_lift', 'yes') THEN 2.5
                WHEN LOWER(lift_type) IN ('j-bar', 'drag_lift', 'platter', 't-bar', 'rope_tow') THEN 3.0
                WHEN LOWER(lift_type) IN ('magic_carpet') THEN 1.5
                WHEN LOWER(lift_type) IN ('mixed_lift') THEN 3.5
                WHEN LOWER(lift_type) IN ('station', 'goods') THEN 4.0
                ELSE 2.5
            END
        ELSE NULL
    END AS derived_speed_mps,
    
    -- Quality indicator for the time calculation
    CASE
        WHEN lift_speed_mps IS NOT NULL AND lift_speed_mps > 0 THEN 'actual_speed'
        WHEN duration IS NOT NULL AND TRY_CAST(duration AS DOUBLE) IS NOT NULL AND TRY_CAST(duration AS DOUBLE) > 0 THEN 'actual_duration'
        WHEN lift_length_m IS NOT NULL AND lift_length_m > 0 AND lift_type IS NOT NULL THEN 'calculated_from_type'
        WHEN lift_type IS NOT NULL THEN 'type_default'
        ELSE 'fallback_default'
    END AS time_calculation_method

FROM {{ ref('base_filtered_ski_lifts') }}
WHERE osm_id IS NOT NULL