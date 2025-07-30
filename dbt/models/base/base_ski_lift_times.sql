SELECT
    osm_id,
    resort,
    name,
    lift_length_m,
    lift_speed_mps,
    -- If speed is available, use it; otherwise, fallback to duration if available
    CASE
        WHEN lift_speed_mps IS NOT NULL AND lift_speed_mps > 0 THEN lift_length_m / lift_speed_mps
        WHEN duration IS NOT NULL AND duration ~ '^[0-9.]+$' THEN CAST(duration AS DOUBLE)
        ELSE NULL
    END AS lift_time_sec
FROM {{ source('ski_runs', 'ski_lifts') }}