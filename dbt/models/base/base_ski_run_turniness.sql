SELECT
    resort,
    run_name,
    piste_type,
    difficulty,
    run_length_m,
    turniness_score,
    turniness_score / NULLIF(run_length_m, 0) AS turniness_per_meter
FROM {{ source('ski_runs', 'ski_runs') }}
WHERE turniness_score IS NOT NULL