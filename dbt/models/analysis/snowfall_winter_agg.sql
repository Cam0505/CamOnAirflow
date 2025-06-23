

WITH winter_snowfall AS (
    SELECT
        location AS ski_field
        , date AS datecol
        , snowfall
        , country
        , EXTRACT(YEAR FROM date) AS year_col
        , EXTRACT(MONTH FROM date) AS month_col
    FROM {{ source('snowfall', 'ski_field_snowfall') }}
    WHERE
        -- Winter months for AU/NZ: June (6), July (7), August (8), September (9), October(10)
        EXTRACT(MONTH FROM date) IN (6, 7, 8, 9, 10, 11)
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