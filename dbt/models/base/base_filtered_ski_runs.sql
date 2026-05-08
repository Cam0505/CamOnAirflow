-- ==============================================================================
-- [INTENT — DO NOT REMOVE] base_filtered_ski_runs
-- Core run filter. Removes:
--   area=yes      — OSM zones/glades mapped as polygons, not runnable lines
--   run_length_m < 10  — stub / connector segments from OSM noise
--   n_points < 2  — single-node entries with no usable geometry
--   osm_id 951853708  — specific Cardrona NZ entry with known bad geometry
-- Difficulty normalisation: 'extreme' → 'intermediate'
--   (keeps the difficulty scale consistent across data providers).
-- Unnamed runs are assigned 'Unnamed <resort> Run <n>' within each resort
--   using ROW_NUMBER over osm_id for a stable, deterministic label.
-- Ski time estimates at bottom are simple placeholders (constant speed);
--   refined times live in staging_ski_time_estimate.
-- NOTE: base_filtered_ski_segments uses an INNER JOIN to this model so that
--   all segment filters are inherited automatically — do not weaken filters
--   here without checking the segment and points models.
-- Uphill coordinate fix: some runs are stored with top/bottom coordinates
--   reversed (bottom elevation > top elevation). When detected via
--   top_elevation_m < bottom_elevation_m, the top_* and bottom_* columns
--   are swapped so that top always refers to the higher endpoint.
-- ==============================================================================

-- Applies only to runs that have both elevation values present.
-- NULL-elevation rows are left unchanged (no reliable direction signal).

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
            ELSE difficulty
        END AS difficulty,
        turniness_score,
        -- Swap top/bottom when the stored "top" is actually lower than "bottom"
        CASE WHEN top_elevation_m IS NOT NULL AND bottom_elevation_m IS NOT NULL
                  AND top_elevation_m < bottom_elevation_m
             THEN bottom_lat ELSE top_lat END AS top_lat,
        CASE WHEN top_elevation_m IS NOT NULL AND bottom_elevation_m IS NOT NULL
                  AND top_elevation_m < bottom_elevation_m
             THEN bottom_lon ELSE top_lon END AS top_lon,
        CASE WHEN top_elevation_m IS NOT NULL AND bottom_elevation_m IS NOT NULL
                  AND top_elevation_m < bottom_elevation_m
             THEN bottom_elevation_m ELSE top_elevation_m END AS top_elevation_m,
        CASE WHEN top_elevation_m IS NOT NULL AND bottom_elevation_m IS NOT NULL
                  AND top_elevation_m < bottom_elevation_m
             THEN top_lat ELSE bottom_lat END AS bottom_lat,
        CASE WHEN top_elevation_m IS NOT NULL AND bottom_elevation_m IS NOT NULL
                  AND top_elevation_m < bottom_elevation_m
             THEN top_lon ELSE bottom_lon END AS bottom_lon,
        CASE WHEN top_elevation_m IS NOT NULL AND bottom_elevation_m IS NOT NULL
                  AND top_elevation_m < bottom_elevation_m
             THEN top_elevation_m ELSE bottom_elevation_m END AS bottom_elevation_m,
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
