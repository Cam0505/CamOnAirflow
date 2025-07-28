-- models/ski_run_gradients.sql

WITH points AS (
    SELECT
        p.osm_id
        , p.resort
        , p.distance_along_run_m
        , p.elevation_smoothed_m
    FROM {{ source('ski_runs', 'ski_run_points') }} AS p
    INNER JOIN {{ ref('base_filtered_ski_runs') }} AS r ON p.osm_id = r.osm_id
)

, first_last_points AS (
    SELECT
        osm_id
        , MIN(distance_along_run_m) AS start_dist
        , MAX(distance_along_run_m) AS end_dist
    FROM points
    GROUP BY osm_id
)

, run_elevations AS (
    SELECT
        p.osm_id
        , p.resort
        , fp.start_dist
        , fp.end_dist
        , FIRST_VALUE(p.elevation_smoothed_m) OVER (PARTITION BY p.osm_id ORDER BY p.distance_along_run_m ASC) AS start_elev
        , LAST_VALUE(p.elevation_smoothed_m) OVER (
            PARTITION BY p.osm_id ORDER BY p.distance_along_run_m ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS end_elev
    FROM points AS p
    INNER JOIN first_last_points AS fp ON p.osm_id = fp.osm_id
)

SELECT
    r.osm_id
    , r.resort
    , r.difficulty
    , r.run_name
    , (re.start_elev - re.end_elev) * 100.0 / NULLIF(re.end_dist - re.start_dist, 0) AS avg_gradient
FROM run_elevations AS re
INNER JOIN {{ ref('base_filtered_ski_runs') }} AS r ON re.osm_id = r.osm_id
GROUP BY r.osm_id, r.resort, r.difficulty, r.run_name, re.start_elev, re.end_elev, re.end_dist, re.start_dist
