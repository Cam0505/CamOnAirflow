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
-- Uphill segment flip: runs stored bottom-to-top in OSM (first point lower than
--   last point) have their segment direction reversed so that segment_index=0 is
--   always the top of the run and from_node/to_node flow downhill. This mirrors
--   the flip in base_filtered_ski_points and ensures the path graph connects runs
--   correctly via shared node IDs.
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

-- Detect runs stored bottom-to-top in OSM (same logic as base_filtered_ski_points).
-- For these runs, segment direction and index are flipped so that segment_index=0
-- always starts at the top of the run, and from_node/to_node match the downhill
-- direction. This ensures graph edges in staging_all_resorts_longest_path connect
-- correctly (A.to_node_id == B.from_node_id).
WITH uphill_runs AS (
    SELECT
        pts.osm_id,
        pts.resort,
        pts.country_code
    FROM {{ source('ski_runs', 'ski_run_points') }} AS pts
    INNER JOIN {{ source('ski_runs', 'ski_runs') }} AS runs
        ON pts.resort = runs.resort
       AND pts.country_code = runs.country_code
       AND pts.osm_id = runs.osm_id
    WHERE runs.piste_type = 'downhill'
      AND runs.run_length_m > 50
      AND COALESCE(LOWER(TRIM(pts.area)), '') <> 'yes'
    GROUP BY
        pts.resort,
        pts.country_code,
        pts.osm_id
    HAVING ARG_MIN(pts.elevation_m, pts.point_index) < ARG_MAX(pts.elevation_m, pts.point_index)
),

max_segment_indexes AS (
    SELECT
        run_osm_id,
        MAX(segment_index) AS max_segment_index
    FROM {{ source('ski_runs', 'ski_run_segments') }}
    GROUP BY run_osm_id
),

segs AS (
    SELECT
        s.run_osm_id,
        -- Flip segment_index so 0 is always the top segment
        CASE
            WHEN ur.osm_id IS NOT NULL THEN msi.max_segment_index - s.segment_index
            ELSE s.segment_index
        END AS segment_index,
        -- Flip from/to node IDs for uphill runs
        CASE WHEN ur.osm_id IS NOT NULL THEN s.to_node_id   ELSE s.from_node_id END AS from_node_id,
        CASE WHEN ur.osm_id IS NOT NULL THEN s.from_node_id ELSE s.to_node_id   END AS to_node_id,
        -- Flip from/to coordinates (fp = original from, tp = original to)
        CASE WHEN ur.osm_id IS NOT NULL THEN tp.lat ELSE fp.lat END AS from_lat,
        CASE WHEN ur.osm_id IS NOT NULL THEN tp.lon ELSE fp.lon END AS from_lon,
        CASE WHEN ur.osm_id IS NOT NULL THEN fp.lat ELSE tp.lat END AS to_lat,
        CASE WHEN ur.osm_id IS NOT NULL THEN fp.lon ELSE tp.lon END AS to_lon,
        s.length_m,
        -- Flip vertical drop sign for uphill runs
        CASE
            WHEN ur.osm_id IS NOT NULL THEN (fp.elevation_m - tp.elevation_m)
            ELSE (tp.elevation_m - fp.elevation_m)
        END AS vertical_drop_m,
        CASE WHEN ur.osm_id IS NOT NULL THEN tp.elevation_m ELSE fp.elevation_m END AS from_elev_m,
        CASE WHEN ur.osm_id IS NOT NULL THEN fp.elevation_m ELSE tp.elevation_m END AS to_elev_m,
        -- Gradient: average is symmetric so no change; single-node and raw fallback swap fp/tp
        CASE
          WHEN fp.gradient_smoothed IS NOT NULL AND tp.gradient_smoothed IS NOT NULL
            THEN ((fp.gradient_smoothed + tp.gradient_smoothed) / 2.0) * 100.0
          WHEN ur.osm_id IS NOT NULL THEN
            CASE
              WHEN tp.gradient_smoothed IS NOT NULL THEN tp.gradient_smoothed * 100.0
              WHEN fp.gradient_smoothed IS NOT NULL THEN fp.gradient_smoothed * 100.0
              WHEN s.length_m IS NULL OR s.length_m <= 0 THEN 0.0
              ELSE ((fp.elevation_m - tp.elevation_m) / s.length_m) * 100.0
            END
          ELSE
            CASE
              WHEN fp.gradient_smoothed IS NOT NULL THEN fp.gradient_smoothed * 100.0
              WHEN tp.gradient_smoothed IS NOT NULL THEN tp.gradient_smoothed * 100.0
              WHEN s.length_m IS NULL OR s.length_m <= 0 THEN 0.0
              ELSE ((tp.elevation_m - fp.elevation_m) / s.length_m) * 100.0
            END
        END AS gradient,
        s.gradient*100 AS original_gradient,
        r.resort,
        r.country_code,
        r.run_name,
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
    LEFT JOIN uphill_runs ur
      ON s.run_osm_id = ur.osm_id
    LEFT JOIN max_segment_indexes msi
      ON s.run_osm_id = msi.run_osm_id
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
