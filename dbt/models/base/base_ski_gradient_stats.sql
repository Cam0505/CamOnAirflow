-- models/ski_gradient_stats.sql

SELECT
    resort,
    difficulty,
    COUNT(*) AS run_count,
    AVG(avg_gradient) AS mean_gradient_percent,
    AVG(ATAN(avg_gradient / 100.0) * 180.0 / PI()) AS mean_gradient_degrees,
    max(steepest_gradient) AS mean_steepest_percent,
    max(ATAN(steepest_gradient / 100.0) * 180.0 / PI()) AS mean_steepest_degrees
FROM {{ ref('base_ski_run_gradients') }}
GROUP BY resort, difficulty
ORDER BY resort ASC, mean_gradient_percent DESC