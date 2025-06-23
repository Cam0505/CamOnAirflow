

WITH prewinter_rainfall AS (
    SELECT
        location AS Ice_climbing
        , date AS datecol
        , rain_sum
        , precipitation_sum
        , country
        , EXTRACT(YEAR FROM date) AS year_col
        , EXTRACT(MONTH FROM date) AS month_col
    FROM {{ source('rainfall', 'ice_climbing_rainfall') }}
    WHERE
        -- Pre winter months for NZ: March (3) -> August (8)
        EXTRACT(MONTH FROM date) IN (3, 4, 5, 6, 7, 8)
)

SELECT
    Ice_climbing
    , country
    , year_col
    , month_col
    , AVG(precipitation_sum) AS avg_daily_precipitation
    , SUM(precipitation_sum) AS total_monthly_precipitation
    , AVG(rain_sum) AS avg_daily_rainfall
    , SUM(rain_sum) AS total_monthly_rainfall
FROM prewinter_rainfall
GROUP BY country, Ice_climbing, year_col, month_col
ORDER BY country, Ice_climbing, year_col, month_col