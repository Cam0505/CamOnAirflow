SELECT
    osm_id,
    resort,
    country_code,
    run_name,
    difficulty,
    piste_type,
    run_length_m,
    n_points
FROM {{ source('ski_runs', 'ski_runs') }}
WHERE run_name <> ''
  AND resort IN ('Remarkables', 'Mount Hutt', 'Cardrona', 'Treble Cone', 'Coronet Peak', 'Turoa', 'Whakapapa')
  AND run_length_m >= {{ var('min_run_length', 200) }}
  AND n_points >= {{ var('min_n_points', 6) }}