-- models/ski_gradient_stats.sql

SELECT
    resort
    , difficulty
    , COUNT(*) AS run_count
    , AVG(avg_gradient) AS mean_gradient
FROM {{ ref('base_ski_run_gradients') }}
GROUP BY resort, difficulty
ORDER BY resort ASC, mean_gradient DESC