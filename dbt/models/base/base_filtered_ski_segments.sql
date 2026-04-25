-- ==============================================================================
-- [INTENT — DO NOT REMOVE] base_filtered_ski_segments
-- Builds per-segment geometry and gradient for every ski run.
--
-- Gradient priority (blended from adjacent GPS point smoothed gradients):
--   1. Average of from- and to-node smoothed gradients (most reliable)
--   2. Single-node smoothed gradient if only one is available
--   3. Raw elevation/length fallback when no smoothed data exists
-- original_gradient preserves the raw source value (s.gradient * 100)
--   for audit and comparison purposes.
--
-- INNER JOIN to base_filtered_ski_runs is intentional: it propagates all
--   run-level filters (area=yes, min length, n_points, explicit exclusions)
--   into this model automatically. Do NOT change to LEFT JOIN — doing so
--   reintroduces the 29 orphaned segment runs that were previously cleaned up.
--
-- The manual Cardrona connector (UNION ALL at the bottom) patches a missing
--   OSM edge between runs 1394841139 and 1394841137. It uses segment_index=999
--   and zeroed geometry to signal that it is synthetic, not real GPS data.
-- ==============================================================================

WITH segs AS (
    SELECT
        s.run_osm_id,
        s.segment_index,
        s.from_node_id,
        s.to_node_id,
        fp.lat AS from_lat,
        fp.lon AS from_lon,
        tp.lat AS to_lat,
        tp.lon AS to_lon,
        s.length_m,
        -- vertical drop per segment (to_node minus from_node)
        (tp.elevation_m - fp.elevation_m) AS vertical_drop_m,
        fp.elevation_m AS from_elev_m,  -- ✅ NEW
        tp.elevation_m AS to_elev_m,     -- ✅ NEW
        CASE
          WHEN fp.gradient_smoothed IS NOT NULL AND tp.gradient_smoothed IS NOT NULL
            THEN ((fp.gradient_smoothed + tp.gradient_smoothed) / 2.0) * 100.0
          WHEN fp.gradient_smoothed IS NOT NULL
            THEN fp.gradient_smoothed * 100.0
          WHEN tp.gradient_smoothed IS NOT NULL
            THEN tp.gradient_smoothed * 100.0
          WHEN s.length_m IS NULL OR s.length_m <= 0 THEN 0.0
          ELSE ((tp.elevation_m - fp.elevation_m) / s.length_m) * 100.0
        END AS gradient,
        s.gradient*100 AS original_gradient,  -- ✅ NEW: keep original gradient for reference
        r.resort,
        r.country_code,
        r.run_name,     -- ✅ corrected run names from base_filtered_ski_runs
        r.difficulty
    FROM {{ source('ski_runs', 'ski_run_segments') }} s
    LEFT JOIN {{ ref('base_filtered_ski_points') }} fp
      ON s.from_node_id = fp.node_id
     AND s.run_osm_id   = fp.osm_id
    LEFT JOIN {{ ref('base_filtered_ski_points') }} tp
      ON s.to_node_id = tp.node_id
     AND s.run_osm_id = tp.osm_id
    INNER JOIN {{ ref('base_filtered_ski_runs') }} r
      ON s.run_osm_id = r.osm_id
    WHERE COALESCE(LOWER(TRIM(s.area)), '') <> 'yes'
)

SELECT * FROM segs

UNION ALL

-- 🔧 Manual connector: link 1394841139 → 1394841137
SELECT
    1394841139 AS run_osm_id,
    999 AS segment_index,
    12911386761 AS from_node_id,
    12911386741 AS to_node_id,
    NULL AS from_lat,
    NULL AS from_lon,
    NULL AS to_lat,
    NULL AS to_lon,
    0.0 AS length_m,
    0.0 AS vertical_drop_m,
    0.0 AS from_elev_m,
    0.0 AS to_elev_m,
    0.0 AS gradient,  -- flat connector
    0.0 AS original_gradient,  -- flat connector
    'Cardrona Alpine Resort' AS resort,
    'NZ' AS country_code,
    'manual_connector' AS run_name,
    NULL AS difficulty
