-- models/ski_run_gradients.sql

WITH points AS (
    SELECT
        p.osm_id,
        p.resort,
        p.distance_along_run_m,
        p.elevation_m
    FROM {{ source('ski_runs', 'ski_run_points') }} p
    INNER JOIN {{ ref('base_filtered_ski_runs') }} r ON p.osm_id = r.osm_id
)
, first_last_points AS (
    SELECT
        osm_id,
        MIN(distance_along_run_m) AS start_dist,
        MAX(distance_along_run_m) AS end_dist
    FROM points
    GROUP BY osm_id
)
, run_elevations AS (
    SELECT
        p.osm_id,
        p.resort,
        fp.start_dist,
        fp.end_dist,
        FIRST_VALUE(p.elevation_m) OVER (PARTITION BY p.osm_id ORDER BY p.distance_along_run_m ASC) AS start_elev,
        LAST_VALUE(p.elevation_m) OVER (PARTITION BY p.osm_id ORDER BY p.distance_along_run_m ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_elev
    FROM points p
    JOIN first_last_points fp ON p.osm_id = fp.osm_id
)
SELECT
    r.osm_id,
    r.resort,
    r.difficulty,
    r.run_name,
    (start_elev - end_elev) * 100.0 / NULLIF(end_dist - start_dist, 0) AS avg_gradient
FROM run_elevations re
JOIN {{ ref('base_filtered_ski_runs') }} r ON re.osm_id = r.osm_id
GROUP BY r.osm_id, r.resort, r.difficulty, r.run_name, start_elev, end_elev, end_dist, start_dist
