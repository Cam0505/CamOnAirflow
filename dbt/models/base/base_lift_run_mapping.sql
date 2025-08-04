-- Map lifts to ski runs based on spatial proximity (simplified approach)

WITH lift_positions AS (
    SELECT
        osm_id as lift_osm_id,
        resort,
        name as lift_name,
        COALESCE(lift_type, 'unknown') as lift_type,
        top_lat,
        top_lon,
        bottom_lat,
        bottom_lon,
        lift_length_m,
        lift_speed_mps
    FROM {{ ref('base_filtered_ski_lifts') }}
    WHERE top_lat IS NOT NULL AND top_lon IS NOT NULL
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
        run_length_m
    FROM {{ ref('base_filtered_ski_runs') }}
    WHERE top_lat IS NOT NULL AND top_lon IS NOT NULL
),
lift_services_runs AS (
    -- Lifts that service runs (lift top near run top)
    SELECT
        l.lift_osm_id,
        l.resort,
        l.lift_name,
        l.lift_type,
        l.lift_length_m,
        l.lift_speed_mps,
        r.run_osm_id,
        r.run_name,
        r.difficulty,
        r.run_length_m,
        'lift_services_run' as connection_type,
        SQRT(
            POW(69.1 * (l.top_lat - r.top_lat), 2) + 
            POW(69.1 * (l.top_lon - r.top_lon) * COS(r.top_lat / 57.3), 2)
        ) * 1609.34 AS min_connection_distance_m
    FROM lift_positions l
    CROSS JOIN run_positions r
    WHERE l.resort = r.resort
        AND SQRT(
            POW(69.1 * (l.top_lat - r.top_lat), 2) + 
            POW(69.1 * (l.top_lon - r.top_lon) * COS(r.top_lat / 57.3), 2)
        ) * 1609.34 <= 50
),
runs_feed_lifts AS (
    -- Runs that feed into lifts (run bottom near lift bottom)
    SELECT
        l.lift_osm_id,
        l.resort,
        l.lift_name,
        l.lift_type,
        l.lift_length_m,
        l.lift_speed_mps,
        r.run_osm_id,
        r.run_name,
        r.difficulty,
        r.run_length_m,
        'run_feeds_lift' as connection_type,
        SQRT(
            POW(69.1 * (r.bottom_lat - l.bottom_lat), 2) + 
            POW(69.1 * (r.bottom_lon - l.bottom_lon) * COS(l.bottom_lat / 57.3), 2)
        ) * 1609.34 AS min_connection_distance_m
    FROM lift_positions l
    CROSS JOIN run_positions r
    WHERE l.resort = r.resort
        AND SQRT(
            POW(69.1 * (r.bottom_lat - l.bottom_lat), 2) + 
            POW(69.1 * (r.bottom_lon - l.bottom_lon) * COS(l.bottom_lat / 57.3), 2)
        ) * 1609.34 <= 100
)

-- Union both connection types - allows multiple connections per run
SELECT
    lift_osm_id,
    resort,
    lift_name,
    lift_type,
    lift_length_m,
    lift_speed_mps,
    run_osm_id,
    run_name,
    difficulty,
    run_length_m,
    connection_type,
    min_connection_distance_m,
    NULL as to_run_osm_id,
    NULL as to_run_name,
    NULL as to_difficulty,
    NULL as to_run_length_m
FROM lift_services_runs

UNION ALL

SELECT
    lift_osm_id,
    resort,
    lift_name,
    lift_type,
    lift_length_m,
    lift_speed_mps,
    run_osm_id,
    run_name,
    difficulty,
    run_length_m,
    connection_type,
    min_connection_distance_m,
    NULL as to_run_osm_id,
    NULL as to_run_name,
    NULL as to_difficulty,
    NULL as to_run_length_m
FROM runs_feed_lifts
ORDER BY resort, lift_osm_id, run_osm_id, connection_type