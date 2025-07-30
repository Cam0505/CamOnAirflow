WITH gradients AS (
    SELECT
        resort,
        run_name,
        avg_gradient
    FROM {{ ref('base_ski_run_gradients') }}
),

binned AS (
    SELECT
        resort,
        CAST(FLOOR((avg_gradient - 5) / 2.25) AS INTEGER) AS gradient_bin, -- 20 bins between 0 and 100%
        COUNT(*) AS n_runs
    FROM gradients
    WHERE avg_gradient IS NOT NULL AND avg_gradient >= 5 AND avg_gradient <= 50
    GROUP BY resort, gradient_bin
)

SELECT
    resort,
    gradient_bin,
    n_runs,
    5 + gradient_bin * 2.25 AS gradient_bin_center,
    SUM(n_runs) OVER (PARTITION BY resort ORDER BY gradient_bin) * 1.0 /
        SUM(n_runs) OVER (PARTITION BY resort) AS cumulative_pct_runs
FROM binned