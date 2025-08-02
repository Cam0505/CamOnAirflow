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
    FROM {{ source('ski_runs', 'ski_lifts') }}
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
    FROM {{ source('ski_runs', 'ski_runs') }}
    WHERE top_lat IS NOT NULL AND top_lon IS NOT NULL
),
lift_run_distances AS (
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
        -- Distance from lift top to run top (lift services run from top)
        SQRT(
            POW(69.1 * (l.top_lat - r.top_lat), 2) + 
            POW(69.1 * (l.top_lon - r.top_lon) * COS(r.top_lat / 57.3), 2)
        ) * 1609.34 AS distance_lift_top_to_run_top_m,
        -- Distance from lift top to run bottom (alternative connection)
        SQRT(
            POW(69.1 * (l.top_lat - r.bottom_lat), 2) + 
            POW(69.1 * (l.top_lon - r.bottom_lon) * COS(r.bottom_lat / 57.3), 2)
        ) * 1609.34 AS distance_lift_top_to_run_bottom_m,
        -- Distance from run bottom to lift bottom (run connects to lift)
        SQRT(
            POW(69.1 * (r.bottom_lat - l.bottom_lat), 2) + 
            POW(69.1 * (r.bottom_lon - l.bottom_lon) * COS(l.bottom_lat / 57.3), 2)
        ) * 1609.34 AS distance_run_bottom_to_lift_bottom_m
    FROM lift_positions l
    CROSS JOIN run_positions r
    WHERE l.resort = r.resort
)

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
    distance_lift_top_to_run_top_m,
    distance_lift_top_to_run_bottom_m,
    distance_run_bottom_to_lift_bottom_m,
    -- Determine connection type
    CASE 
        WHEN distance_lift_top_to_run_top_m <= 100 THEN 'lift_services_run'
        WHEN distance_run_bottom_to_lift_bottom_m <= 100 THEN 'run_feeds_lift'
        ELSE 'no_connection'
    END AS connection_type,
    -- Get the minimum distance for connections
    LEAST(
        distance_lift_top_to_run_top_m,
        distance_lift_top_to_run_bottom_m,
        distance_run_bottom_to_lift_bottom_m
    ) AS min_connection_distance_m,
    NULL as to_run_osm_id,
    NULL as to_run_name,
    NULL as to_difficulty,
    NULL as to_run_length_m
FROM lift_run_distances
WHERE LEAST(
    distance_lift_top_to_run_top_m,
    distance_lift_top_to_run_bottom_m,
    distance_run_bottom_to_lift_bottom_m
) <= 100  -- Back to tight 100m threshold
ORDER BY resort, lift_osm_id, min_connection_distance_m