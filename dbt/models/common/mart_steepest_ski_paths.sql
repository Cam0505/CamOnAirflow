-- ==============================================================================
-- mart_steepest_ski_paths
-- One row per resort: the steepest skiable path through that resort,
-- computed by staging_all_resorts_steepest_path (two-pass DFS).
-- Paths shorter than the resort's 25th-percentile terminal path length are
-- excluded to prevent trivially short steep segments from dominating.
-- Adds country_code via join with base_filtered_ski_runs.
-- avg_gradient_pct is total_vertical_m / total_distance_m * 100.
-- node_ids is a JSON array of "{run_osm_id}_{segment_index}" strings,
-- used by ski_steepest_graph.py to highlight the path.
-- ==============================================================================

SELECT
    s.resort,
    r.country_code,
    s.starting_lift,
    s.starting_lift_id,
    s.run_count,
    s.total_distance_m,
    s.total_vertical_m,
    s.avg_gradient_pct,
    s.min_length_threshold_m,
    s.run_path,
    s.node_ids
FROM {{ ref('staging_all_resorts_steepest_path') }} s
LEFT JOIN (
    SELECT DISTINCT resort, country_code
    FROM {{ ref('base_filtered_ski_runs') }}
) r ON s.resort = r.resort
