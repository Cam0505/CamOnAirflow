-- ==============================================================================
-- [INTENT — DO NOT REMOVE] base_ski_run_gradients
-- Computes per-run average gradient and steepest segment gradient.
--
-- avg_gradient: end-to-end (start_elev - end_elev) / run_length. Uses
--   first/last points rather than a mean of segment gradients to avoid
--   cumulative GPS noise amplification.
--
-- steepest_gradient: max segment gradient after two filters are applied:
--   1. Segments with |gradient| > difficulty-based threshold are dropped.
--      Thresholds are derived from piste classification angle standards:
--        novice=15°, easy=25°, intermediate=35°, advanced/freeride=50°
--      (converted to % via TAN(RADIANS(angle))*100)
--   2. Segments shorter than 15m are dropped to avoid noisy single-point spikes.
--
-- Gradient calculation uses raw elevation first; falls back to smoothed
--   elevation when raw produces |gradient| > 100% or exactly 0 (which signals
--   a flat GPS artefact). If smoothed also exceeds 110% the segment is zeroed.
--
-- Runs with fewer than 3 points are excluded — they cannot produce a valid
--   segment-to-segment comparison.
-- ==============================================================================

WITH points AS (
    SELECT
        p.osm_id,
        p.resort,
        p.distance_along_run_m,
        p.elevation_m,
        p.elevation_smoothed_m,
        p.point_index,
        r.difficulty
    FROM {{ ref('base_filtered_ski_points') }} AS p
    INNER JOIN {{ ref('base_filtered_ski_runs') }} AS r ON p.osm_id = r.osm_id
    where r.n_points >= 3
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
        , FIRST_VALUE(p.elevation_m) OVER (PARTITION BY p.osm_id ORDER BY p.distance_along_run_m ASC) AS start_elev
        , LAST_VALUE(p.elevation_m) OVER (
            PARTITION BY p.osm_id ORDER BY p.distance_along_run_m ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS end_elev
    FROM points AS p
    INNER JOIN first_last_points AS fp ON p.osm_id = fp.osm_id
)

,segments AS (
    SELECT
        case when curr.difficulty = 'novice' then TAN(RADIANS(15)) * 100.0
            when curr.difficulty = 'easy' then TAN(RADIANS(25)) * 100.0
            when curr.difficulty = 'intermediate' then TAN(RADIANS(35)) * 100.0
            when curr.difficulty = 'advanced' then TAN(RADIANS(50)) * 100.0
            when curr.difficulty = 'freeride' then TAN(RADIANS(50)) * 100.0
            else TAN(RADIANS(50)) * 100.0
        end as steepest_gradient_threshold,
        curr.osm_id,
        curr.resort,
        curr.distance_along_run_m AS seg_start_dist,
        next.distance_along_run_m AS seg_end_dist,
        curr.elevation_m AS seg_start_elev,
        next.elevation_m AS seg_end_elev,
        (next.distance_along_run_m - curr.distance_along_run_m) AS segment_length,
        (curr.elevation_m - next.elevation_m) * 100.0 / NULLIF(next.distance_along_run_m - curr.distance_along_run_m, 0) AS segment_gradient,
        (curr.elevation_smoothed_m - next.elevation_smoothed_m) * 100.0 / NULLIF(next.distance_along_run_m - curr.distance_along_run_m, 0) AS smooth_segment_gradient,
        CASE
            WHEN ABS((curr.elevation_m - next.elevation_m) * 100.0 / NULLIF(next.distance_along_run_m - curr.distance_along_run_m, 0)) > 100
                 OR (curr.elevation_m - next.elevation_m) * 100.0 / NULLIF(next.distance_along_run_m - curr.distance_along_run_m, 0) = 0
                THEN CASE
                    WHEN ABS((curr.elevation_smoothed_m - next.elevation_smoothed_m) * 100.0 / NULLIF(next.distance_along_run_m - curr.distance_along_run_m, 0)) > 110
                        THEN 0
                    ELSE (curr.elevation_smoothed_m - next.elevation_smoothed_m) * 100.0 / NULLIF(next.distance_along_run_m - curr.distance_along_run_m, 0)
                END
            ELSE (curr.elevation_m - next.elevation_m) * 100.0 / NULLIF(next.distance_along_run_m - curr.distance_along_run_m, 0)
        END AS final_segment_gradient
    FROM points curr
    JOIN points next
      ON curr.osm_id = next.osm_id
     AND curr.point_index = next.point_index - 1
    WHERE next.distance_along_run_m > curr.distance_along_run_m
)

,filtered_segments AS (
    SELECT *
    FROM segments
    WHERE ABS(final_segment_gradient) < steepest_gradient_threshold -- filter out extreme gradients
    and segment_length >= 15 -- filter out very short segments
)

,steepest_segment AS (
    SELECT
        osm_id,
        MAX(final_segment_gradient) AS steepest_gradient
    FROM filtered_segments
    GROUP BY osm_id
)

SELECT
    r.osm_id,
    r.resort,
    r.difficulty,
    r.run_name,
    (re.start_elev - re.end_elev) * 100.0 / NULLIF(re.end_dist - re.start_dist, 0) AS avg_gradient,
    steepest_gradient
FROM run_elevations AS re
INNER JOIN {{ ref('base_filtered_ski_runs') }} AS r ON re.osm_id = r.osm_id
LEFT JOIN steepest_segment AS s ON r.osm_id = s.osm_id
GROUP BY r.osm_id, r.resort, r.difficulty, r.run_name, re.start_elev, 
         re.end_elev, re.end_dist, re.start_dist, s.steepest_gradient
