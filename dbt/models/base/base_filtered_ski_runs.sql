WITH deduplicated_runs AS (
    SELECT
        osm_id,
        resort,
        country_code,
        run_name,
        piste_type,
        run_length_m,
        n_points,
        CASE
            WHEN difficulty = 'extreme' THEN 'intermediate'
            WHEN difficulty = 'expert' THEN 'advanced'
            ELSE difficulty
        END AS difficulty,
        turniness_score,
        top_lat,
        top_lon,
        top_elevation_m,
        bottom_lat,
        bottom_lon,
        bottom_elevation_m,
        grooming,
        lit,
        run_length_m / 4.0 AS ski_time_slow_sec,
        run_length_m / 7.0 AS ski_time_medium_sec,
        run_length_m / 10.0 AS ski_time_fast_sec,
        -- Only deduplicate non-null names
        CASE 
            WHEN run_name IS NOT NULL and run_name <> '' THEN
                ROW_NUMBER() OVER (
                    PARTITION BY resort, run_name
                    ORDER BY run_length_m DESC, osm_id
                )
            ELSE 1  -- Always keep runs with NULL names
        END as name_rank
    FROM {{ source('ski_runs', 'ski_runs') }}
    WHERE
        run_length_m >= {{ var('min_run_length', 90) }}
        AND n_points >= {{ var('min_n_points', 2) }}
)

SELECT
    osm_id,
    resort,
    country_code,
    run_name,
    piste_type,
    run_length_m,
    n_points,
    difficulty,
    turniness_score,
    top_lat,
    top_lon,
    top_elevation_m,
    bottom_lat,
    bottom_lon,
    bottom_elevation_m,
    grooming,
    lit,
    ski_time_slow_sec,
    ski_time_medium_sec,
    ski_time_fast_sec
FROM deduplicated_runs
WHERE name_rank = 1  -- Keep longest run for each non-null name, all runs with null names