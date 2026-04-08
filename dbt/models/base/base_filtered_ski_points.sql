WITH uphill_runs AS (
    SELECT
        pts.resort,
        pts.country_code,
        pts.osm_id,
        ARG_MIN(pts.elevation_m, pts.point_index) AS first_elevation,
        ARG_MAX(pts.elevation_m, pts.point_index) AS last_elevation
    FROM {{ source('ski_runs', 'ski_run_points') }} AS pts
    INNER JOIN {{ source('ski_runs', 'ski_runs') }} AS runs
        ON pts.resort = runs.resort
       AND pts.country_code = runs.country_code
       AND pts.osm_id = runs.osm_id
    WHERE runs.piste_type = 'downhill'
      AND runs.run_length_m > 100
    GROUP BY
        pts.resort,
        pts.country_code,
        pts.osm_id,
        pts.run_name,
        runs.piste_type,
        runs.run_length_m
    HAVING ARG_MIN(pts.elevation_m, pts.point_index) < ARG_MAX(pts.elevation_m, pts.point_index)
),

max_point_indexes AS (
    SELECT
        osm_id,
        resort,
        country_code,
        MAX(point_index) AS max_point_index,
        MAX(distance_along_run_m) AS max_distance_along_run_m
    FROM {{ source('ski_runs', 'ski_run_points') }}
    GROUP BY
        osm_id,
        resort,
        country_code
)

SELECT
    p.osm_id,
    p.resort,
    p.country_code,
    p.run_name,
    CASE
        WHEN ur.osm_id IS NOT NULL THEN mpi.max_point_index - p.point_index
        ELSE p.point_index
    END AS point_index,
    p.lat,
    p.lon,
    CASE
        WHEN ur.osm_id IS NOT NULL THEN mpi.max_distance_along_run_m - p.distance_along_run_m
        ELSE p.distance_along_run_m
    END AS distance_along_run_m,
    p.elevation_m,
    p.elevation_smoothed_m,
    p.gradient_smoothed,
    p.node_id
FROM {{ source('ski_runs', 'ski_run_points') }} AS p
LEFT JOIN uphill_runs AS ur
    ON p.osm_id = ur.osm_id
   AND p.resort = ur.resort
   AND p.country_code = ur.country_code
LEFT JOIN max_point_indexes AS mpi
    ON p.osm_id = mpi.osm_id
   AND p.resort = mpi.resort
   AND p.country_code = mpi.country_code
