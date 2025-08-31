-- Estimate ski run times based on length, gradient, turniness, and ability

WITH gradients AS (
    SELECT
        osm_id,
        avg_gradient
    FROM {{ ref('base_ski_run_gradients') }}
),
turniness AS (
    SELECT
        osm_id,
        turniness_score,
        turniness_score / NULLIF(run_length_m, 0) AS turniness_per_meter
    FROM {{ ref('base_ski_run_turniness') }}
),
runs AS (
    SELECT
        r.osm_id,
        r.resort,
        r.run_name,
        r.difficulty,
        r.run_length_m,
        COALESCE(g.avg_gradient, 0) AS avg_gradient,
        COALESCE(t.turniness_score, 0) AS turniness_score,
        COALESCE(t.turniness_score / NULLIF(r.run_length_m, 0), 0) AS turniness_per_meter
    FROM {{ ref('base_filtered_ski_runs') }} r
    LEFT JOIN {{ ref('base_ski_run_gradients') }} g ON r.osm_id = g.osm_id
    LEFT JOIN {{ ref('base_ski_run_turniness') }} t ON r.osm_id = t.osm_id
)

SELECT
    osm_id,
    resort,
    run_name,
    difficulty,
    run_length_m,
    avg_gradient,
    turniness_score,
    turniness_per_meter,
    -- Ability base speeds (m/s)
    4.0 AS slow_base_speed,
    7.0 AS intermediate_base_speed,
    10.0 AS fast_base_speed,
    -- Gradient factor: 1 + (avg_gradient_percent / 100) * 0.5 (tune as needed)
    (1 + (COALESCE(avg_gradient, 0) / 100.0) * 0.5) AS gradient_factor,
    -- Turniness factor: 1 - (turniness_per_meter * 2) (tune as needed, min 0.7)
    GREATEST(0.7, 1 - (COALESCE(turniness_per_meter, 0) * 2)) AS turniness_factor,
    -- Estimated times (seconds) for each ability
    run_length_m / (
        4.0 * (1 + (COALESCE(avg_gradient, 0) / 100.0) * 0.5) * GREATEST(0.7, 1 - (COALESCE(turniness_per_meter, 0) * 2))
    ) AS ski_time_slow_sec,
    run_length_m / (
        7.0 * (1 + (COALESCE(avg_gradient, 0) / 100.0) * 0.5) * GREATEST(0.7, 1 - (COALESCE(turniness_per_meter, 0) * 2))
    ) AS ski_time_intermediate_sec,
    run_length_m / (
        10.0 * (1 + (COALESCE(avg_gradient, 0) / 100.0) * 0.5) * GREATEST(0.7, 1 - (COALESCE(turniness_per_meter, 0) * 2))
    ) AS ski_time_fast_sec
FROM runs