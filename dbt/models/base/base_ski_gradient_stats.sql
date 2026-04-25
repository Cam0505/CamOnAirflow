-- ==============================================================================
-- [INTENT — DO NOT REMOVE] base_ski_gradient_stats
-- Resort-level summary of gradient statistics, grouped by difficulty bucket.
-- mean_gradient_degrees and mean_steepest_degrees convert percent gradient to
--   degrees using ATAN(pct/100)*180/PI() for human-readable comparison.
-- Note: 'mean_steepest_percent/degrees' column names are misleading —
--   they actually use MAX(), not AVG(), to return the steepest observed
--   segment across all runs in that resort/difficulty group.
-- ==============================================================================

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