WITH ski_lookup AS (
    SELECT
        name
        , country
        , lat
    FROM {{ source('snowfall', 'ski_field_lookup') }}
)

, winter_snowfall AS (
    SELECT
        snowfall.location AS ski_field
        , date AS datecol
        , snowfall
        , snowfall.country
        , CASE
            WHEN lookup.lat >= 0 AND EXTRACT(MONTH FROM date) = 12
                THEN EXTRACT(YEAR FROM date) + 1
            ELSE EXTRACT(YEAR FROM date)
        END AS year_col
        , EXTRACT(MONTH FROM date) AS month_col
    FROM {{ source('snowfall', 'ski_field_snowfall') }} AS snowfall
    LEFT JOIN ski_lookup AS lookup
        ON snowfall.location = lookup.name
        AND snowfall.country = lookup.country
    WHERE
        CASE
            WHEN lookup.lat >= 0
                THEN EXTRACT(MONTH FROM date) IN (12, 1, 2, 3, 4, 5)
            WHEN lookup.lat < 0
                THEN EXTRACT(MONTH FROM date) IN (6, 7, 8, 9, 10, 11)
            ELSE EXTRACT(MONTH FROM date) IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        END
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