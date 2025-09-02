-- Map lifts to runs using only top/bottom coordinates

WITH lift_positions AS (
    SELECT
        osm_id as lift_osm_id,
        resort,
        name as lift_name,
        COALESCE(lift_type, 'unknown') as lift_type,
        country_code,
        top_lat,
        top_lon,
        bottom_lat,
        bottom_lon,
        lift_length_m,
        lift_speed_mps
    FROM {{ ref('base_filtered_ski_lifts') }}
    WHERE top_lat IS NOT NULL 
      AND top_lon IS NOT NULL
      AND bottom_lat IS NOT NULL
      AND bottom_lon IS NOT NULL
),

run_positions AS (
    SELECT
        osm_id as run_osm_id,
        resort,
        COALESCE(run_name, 'Unnamed Run') as run_name,
        COALESCE(difficulty, 'unknown') as difficulty,
        top_lat,
        top_lon,
        bottom_lat,
        bottom_lon,
        run_length_m,
        top_elevation_m,
        bottom_elevation_m,
        turniness_score
    FROM {{ ref('base_filtered_ski_runs') }}
    WHERE top_lat IS NOT NULL 
      AND top_lon IS NOT NULL
      AND bottom_lat IS NOT NULL
      AND bottom_lon IS NOT NULL
),

-- Lifts that service runs (lift top near run top)
lift_services_runs AS (
    SELECT
        l.lift_osm_id,
        l.resort,
        l.lift_name,
        l.lift_type,
        l.country_code,
        l.lift_length_m,
        l.lift_speed_mps,
        r.run_osm_id,
        r.run_name,
        r.difficulty,
        r.run_length_m,
        'lift_services_run' as connection_type,
        0 as run_start_point_index,  -- Start at the top (point index 0)
        0 as run_start_distance_m,   -- Start at the top (distance 0)
        SQRT(
            POW(69.1 * (l.top_lat - r.top_lat), 2) + 
            POW(69.1 * (l.top_lon - r.top_lon) * COS(r.top_lat / 57.3), 2)
        ) * 1609.34 AS connection_distance_m,
        ROW_NUMBER() OVER (PARTITION BY l.lift_osm_id, r.run_osm_id ORDER BY 
            SQRT(
                POW(69.1 * (l.top_lat - r.top_lat), 2) + 
                POW(69.1 * (l.top_lon - r.top_lon) * COS(r.top_lat / 57.3), 2)
            ) * 1609.34 ASC
        ) as rank
    FROM lift_positions l
    JOIN run_positions r ON l.resort = r.resort
    WHERE SQRT(
        POW(69.1 * (l.top_lat - r.top_lat), 2) + 
        POW(69.1 * (l.top_lon - r.top_lon) * COS(r.top_lat / 57.3), 2)
    ) * 1609.34 <= 45  -- Within 45m 
),

-- Runs that feed into lifts (run bottom near lift bottom)
runs_feed_lifts AS (
    SELECT
        l.lift_osm_id,
        l.resort,
        l.lift_name,
        l.lift_type,
        l.country_code,
        l.lift_length_m,
        l.lift_speed_mps,
        r.run_osm_id,
        r.run_name,
        r.difficulty,
        r.run_length_m,
        'run_feeds_lift' as connection_type,
        999999 as run_end_point_index,  -- End of the run
        r.run_length_m as run_end_distance_m,  -- End of the run
        SQRT(
            POW(69.1 * (l.bottom_lat - r.bottom_lat), 2) + 
            POW(69.1 * (l.bottom_lon - r.bottom_lon) * COS(r.bottom_lat / 57.3), 2)
        ) * 1609.34 AS connection_distance_m,
        ROW_NUMBER() OVER (PARTITION BY l.lift_osm_id, r.run_osm_id ORDER BY 
            SQRT(
                POW(69.1 * (l.bottom_lat - r.bottom_lat), 2) + 
                POW(69.1 * (l.bottom_lon - r.bottom_lon) * COS(r.bottom_lat / 57.3), 2)
            ) * 1609.34 ASC
        ) as rank
    FROM lift_positions l
    JOIN run_positions r ON l.resort = r.resort
    WHERE SQRT(
        POW(69.1 * (l.bottom_lat - r.bottom_lat), 2) + 
        POW(69.1 * (l.bottom_lon - r.bottom_lon) * COS(r.bottom_lat / 57.3), 2)
    ) * 1609.34 <= 75  -- Within 75m (increased slightly for reliability)
)


-- 🔧 Manual overrides for known missing mappings
,manual_overrides AS (
    SELECT
        200252642       AS lift_osm_id,                -- Whitestar Express
        'Cardrona Alpine Resort' AS resort,
        'whitestar express' AS lift_name,
        'chair_lift'    AS lift_type,
        'NZ'            AS country_code,
        1217.5183       AS lift_length_m,
        2.5             AS lift_speed_mps,
        1394842466      AS run_osm_id,                 -- Over Run
        'Over Run'      AS run_name,
        'easy'          AS difficulty,
        859.9009        AS run_length_m,
        'run_feeds_lift' AS connection_type,
        999999          AS point_index,
        859.9009        AS distance_m,
        0.0             AS connection_distance_m       -- force connection
)

-- Combine the two types of connections with consistent column names
SELECT
    lift_osm_id,
    resort,
    lift_name,
    lift_type,
    country_code,
    lift_length_m,
    lift_speed_mps,
    run_osm_id,
    run_name,
    difficulty,
    run_length_m,
    connection_type,
    run_start_point_index as point_index,
    run_start_distance_m as distance_m,
    connection_distance_m
FROM lift_services_runs
WHERE rank = 1  -- Only closest connection

UNION ALL

SELECT
    lift_osm_id,
    resort,
    lift_name,
    lift_type,
    country_code,
    lift_length_m,
    lift_speed_mps,
    run_osm_id,
    run_name,
    difficulty,
    run_length_m,
    connection_type,
    run_end_point_index as point_index,
    run_end_distance_m as distance_m,
    connection_distance_m
FROM runs_feed_lifts
WHERE rank = 1  -- Only closest connection
UNION ALL
SELECT * FROM manual_overrides
ORDER BY resort, lift_osm_id, run_osm_id, connection_type