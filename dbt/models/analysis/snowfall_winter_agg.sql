WITH winter_snowfall AS (
    SELECT
        ski_field
        , datecol
        , daily_snowfall_cm AS snowfall
        , country
        , year_col
        , month_col
    FROM {{ ref('base_ski_field_snowfall_modeled') }}
)

SELECT
    ski_field
    , country
    , year_col
    , month_col
    , AVG(snowfall) AS avg_daily_snowfall
    , SUM(snowfall) AS total_monthly_snowfall
FROM winter_snowfall
GROUP BY country, ski_field, year_col, month_col
ORDER BY country, ski_field, year_col, month_col