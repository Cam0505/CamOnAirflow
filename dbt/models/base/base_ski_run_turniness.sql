-- ==============================================================================
-- [INTENT — DO NOT REMOVE] base_ski_run_turniness
-- Thin projection over base_filtered_ski_runs that surfaces turniness metrics.
-- turniness_score is computed upstream in the OSM pipeline (Ski_runs.py).
-- turniness_per_meter normalises by run length so short and long runs are
--   comparable; NULLIF guards against division-by-zero on zero-length runs.
-- Rows where turniness_score IS NULL are excluded — they cannot contribute
--   to gradient distribution or path-matrix calculations.
-- ==============================================================================

SELECT
    resort,
    osm_id,
    run_name,
    piste_type,
    difficulty,
    run_length_m,
    turniness_score,
    turniness_score / NULLIF(run_length_m, 0) AS turniness_per_meter
FROM {{ ref('base_filtered_ski_runs') }}
WHERE turniness_score IS NOT NULL