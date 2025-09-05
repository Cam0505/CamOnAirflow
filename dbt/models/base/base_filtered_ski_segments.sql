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
        -- gradient as % slope = (rise/run)*100
        CASE
            WHEN s.length_m IS NULL OR s.length_m = 0 THEN 0.0
            ELSE ((tp.elevation_m - fp.elevation_m) / s.length_m) * 100.0
        END AS gradient,
        r.resort,
        r.run_name,     -- ✅ corrected run names from base_filtered_ski_runs
        r.difficulty
    FROM {{ source('ski_runs', 'ski_run_segments') }} s
    LEFT JOIN {{ ref('base_filtered_ski_points') }} fp
      ON s.from_node_id = fp.node_id
     AND s.run_osm_id   = fp.osm_id
    LEFT JOIN {{ ref('base_filtered_ski_points') }} tp
      ON s.to_node_id = tp.node_id
     AND s.run_osm_id = tp.osm_id
    LEFT JOIN {{ ref('base_filtered_ski_runs') }} r
      ON s.run_osm_id = r.osm_id
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
    0.0 AS gradient,  -- flat connector
    'Cardrona Alpine Resort' AS resort,
    'manual_connector' AS run_name,
    NULL AS difficulty
