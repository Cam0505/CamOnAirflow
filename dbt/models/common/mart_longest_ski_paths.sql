-- ==============================================================================
-- mart_longest_ski_paths
-- One row per resort: the single longest skiable path through that resort,
-- computed by staging_all_resorts_longest_path (run-level DFS).
-- Adds country_code via join with base_filtered_ski_runs.
-- run_osm_ids is a JSON array of integer OSM run IDs on the path,
-- used by ski_network_graph.py to highlight the path.
-- ==============================================================================

SELECT
    s.resort,
    r.country_code,
    s.starting_lift,
    s.starting_lift_id,
    s.run_count,
    s.total_distance_m,
    s.total_vertical_m,
    s.run_path,
    s.node_ids
FROM {{ ref('staging_all_resorts_longest_path') }} s
LEFT JOIN (
    SELECT DISTINCT resort, country_code
    FROM {{ ref('base_filtered_ski_runs') }}
) r ON s.resort = r.resort
