-- ==============================================================================
-- [INTENT — DO NOT REMOVE] base_filtered_ski_runs
-- Core run filter. Removes:
--   area=yes      — OSM zones/glades mapped as polygons, not runnable lines
--   run_length_m < 10  — stub / connector segments from OSM noise
--   n_points < 2  — single-node entries with no usable geometry
--   osm_id 951853708  — specific Cardrona NZ entry with known bad geometry
-- Difficulty normalisation: 'extreme' → 'intermediate', 'expert' → 'advanced'
--   (keeps the difficulty scale consistent across data providers).
-- Unnamed runs are assigned 'Unnamed <resort> Run <n>' within each resort
--   using ROW_NUMBER over osm_id for a stable, deterministic label.
-- Ski time estimates at bottom are simple placeholders (constant speed);
--   refined times live in staging_ski_time_estimate.
-- NOTE: base_filtered_ski_segments uses an INNER JOIN to this model so that
--   all segment filters are inherited automatically — do not weaken filters
--   here without checking the segment and points models.
-- ==============================================================================

WITH deduplicated_runs AS (
    SELECT
        osm_id,
        resort,
        country_code,
        region,
        run_name,
        COALESCE(LOWER(TRIM(area)), '') as area,
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
        run_length_m / 4.0 AS ski_time_slow_sec,
        run_length_m / 7.0 AS ski_time_medium_sec,
        run_length_m / 10.0 AS ski_time_fast_sec,
        avg_point_resolution_m
    FROM {{ source('ski_runs', 'ski_runs') }}
    WHERE
        COALESCE(LOWER(TRIM(area)), '') <> 'yes'
        AND run_length_m >= {{ var('min_run_length', 10) }}
        AND n_points >= {{ var('min_n_points', 2) }}
),

numbered_runs AS (
    SELECT
        dr.*,
        -- Assign row numbers only to unnamed runs per resort
        CASE 
            WHEN dr.run_name IS NULL OR dr.run_name = '' THEN
                ROW_NUMBER() OVER (PARTITION BY dr.resort ORDER BY dr.osm_id)
            ELSE NULL
        END AS unnamed_num
    FROM deduplicated_runs dr
)

SELECT
    osm_id,
    resort,
    country_code,
    -- Replace null/empty run names with "Unnamed <resort> Run <x>"
    CASE
        WHEN run_name IS NULL OR run_name = ''
            THEN 'Unnamed ' || resort || ' Run ' || unnamed_num
        ELSE run_name
    END AS run_name,
    region,
    area,
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
    ski_time_slow_sec,
    ski_time_medium_sec,
    ski_time_fast_sec
FROM numbered_runs
WHERE osm_id <> 951853708
