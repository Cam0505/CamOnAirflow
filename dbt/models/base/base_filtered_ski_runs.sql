SELECT
    osm_id
    , resort
    , country_code
    , run_name
    , piste_type
    , run_length_m
    , n_points
    , CASE
        WHEN difficulty = 'extreme' THEN 'intermediate'
        WHEN difficulty = 'expert' THEN 'advanced'
        ELSE difficulty
    END AS difficulty
FROM {{ source('ski_runs', 'ski_runs') }}
WHERE
    run_length_m >= {{ var('min_run_length', 200) }}
    AND n_points >= {{ var('min_n_points', 6) }}