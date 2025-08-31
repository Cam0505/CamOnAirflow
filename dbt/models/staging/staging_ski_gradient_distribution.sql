WITH gradients AS (
    SELECT
        resort,
        run_name,
        AVG(ATAN(avg_gradient / 100.0) * 180.0 / PI()) AS avg_gradient_deg
    FROM {{ ref('base_ski_run_gradients') }}
    GROUP BY resort, run_name
),

binned AS (
    SELECT
        resort,
        CAST(FLOOR((avg_gradient_deg - 5) / 2.25) AS INTEGER) AS gradient_bin, -- 20 bins between 5° and 50°
        COUNT(*) AS n_runs
    FROM gradients
    WHERE avg_gradient_deg IS NOT NULL AND avg_gradient_deg >= 5 AND avg_gradient_deg <= 50
    GROUP BY resort, gradient_bin
)

SELECT
    resort,
    gradient_bin,
    n_runs,
    5 + gradient_bin * 2.25 AS gradient_bin_center_deg,
    SUM(n_runs) OVER (PARTITION BY resort ORDER BY gradient_bin) * 1.0 /
        SUM(n_runs) OVER (PARTITION BY resort) AS cumulative_pct_runs
FROM binned