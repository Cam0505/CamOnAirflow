SELECT
    p.osm_id,
    p.resort,
    r.country_code,
    r.run_name,
    p.point_index,
    p.lat,
    p.lon,
    p.distance_along_run_m,
    p.elevation_m,
    p.elevation_smoothed_m,
    p.gradient_smoothed,
    p.node_id
FROM {{ source('ski_runs', 'ski_run_points') }} p
JOIN {{ ref('base_filtered_ski_runs') }} r
  ON p.osm_id = r.osm_id
 AND p.resort = r.resort
