-- Map lifts to all possible first ski segments (including partial runs when they split)

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
        run_length_m,
        top_elevation_m,
        bottom_elevation_m,
        turniness_score
    FROM {{ ref('base_filtered_ski_runs') }}
    WHERE top_lat IS NOT NULL AND top_lon IS NOT NULL
),
run_points AS (
    SELECT
        osm_id as run_osm_id,
        resort,
        lat,
        lon,
        elevation_m,
        distance_along_run_m,
        point_index,
        MAX(point_index) OVER (PARTITION BY osm_id) as max_point_index
    FROM {{ ref('base_filtered_ski_points') }}
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
        pts.point_index as run_start_point_index,
        pts.distance_along_run_m as run_start_distance_m,
        SQRT(
            POW(69.1 * (l.top_lat - pts.lat), 2) + 
            POW(69.1 * (l.top_lon - pts.lon) * COS(pts.lat / 57.3), 2)
        ) * 1609.34 AS connection_distance_m,
        ROW_NUMBER() OVER (PARTITION BY l.lift_osm_id, r.run_osm_id ORDER BY 
            SQRT(
                POW(69.1 * (l.top_lat - pts.lat), 2) + 
                POW(69.1 * (l.top_lon - pts.lon) * COS(pts.lat / 57.3), 2)
            ) * 1609.34 ASC
        ) as rank
    FROM lift_positions l
    INNER JOIN run_points pts ON (
        l.resort = pts.resort
        -- Only connect to first few points of run (top)
        AND pts.point_index <= 2
    )
    INNER JOIN run_positions r ON pts.run_osm_id = r.run_osm_id
    WHERE SQRT(
        POW(69.1 * (l.top_lat - pts.lat), 2) + 
        POW(69.1 * (l.top_lon - pts.lon) * COS(pts.lat / 57.3), 2)
    ) * 1609.34 <= 50  -- Within 50m
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
        pts.point_index as run_end_point_index,
        pts.distance_along_run_m as run_end_distance_m,
        SQRT(
            POW(69.1 * (l.bottom_lat - pts.lat), 2) + 
            POW(69.1 * (l.bottom_lon - pts.lon) * COS(pts.lat / 57.3), 2)
        ) * 1609.34 AS connection_distance_m,
        ROW_NUMBER() OVER (PARTITION BY l.lift_osm_id, r.run_osm_id ORDER BY 
            SQRT(
                POW(69.1 * (l.bottom_lat - pts.lat), 2) + 
                POW(69.1 * (l.bottom_lon - pts.lon) * COS(pts.lat / 57.3), 2)
            ) * 1609.34 ASC
        ) as rank
    FROM lift_positions l
    INNER JOIN run_points pts ON (
        l.resort = pts.resort
        -- Only connect to last few points of run (bottom)
        AND pts.point_index >= (pts.max_point_index - 3)
    )
    INNER JOIN run_positions r ON pts.run_osm_id = r.run_osm_id
    WHERE SQRT(
        POW(69.1 * (l.bottom_lat - pts.lat), 2) + 
        POW(69.1 * (l.bottom_lon - pts.lon) * COS(pts.lat / 57.3), 2)
    ) * 1609.34 <= 50  -- Within 50m
)

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
    run_start_point_index,
    run_start_distance_m,
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
    run_end_point_index,
    run_end_distance_m,
    connection_distance_m
FROM runs_feed_lifts
WHERE rank = 1  -- Only closest connection

ORDER BY resort, lift_osm_id, run_osm_id, connection_type