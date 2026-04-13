WITH ski_lookup AS (
    SELECT
        name
        , country
        , lat
    FROM {{ source('snowfall', 'ski_field_lookup') }}
)

, daily_winter_snowfall AS (
    SELECT
        snowfall.location AS ski_field
        , date AS datecol
        , snowfall.snowfall AS daily_snowfall_cm
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

-- Assign season-aware daily order for each ski field.
, labelled AS (
    SELECT
        *
        , CONCAT(country, ' - ', ski_field) AS facet_label
        , ROW_NUMBER() OVER (
            PARTITION BY ski_field, year_col
            ORDER BY datecol
        ) AS day_of_season
    FROM daily_winter_snowfall
)

, cumulative AS (
    SELECT
        ski_field
        , country
        , year_col
        , month_col
        , datecol
        , facet_label
        , day_of_season
        , daily_snowfall_cm
        , SUM(daily_snowfall_cm) OVER (
            PARTITION BY ski_field, year_col
            ORDER BY datecol
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_snowfall_cm
    FROM labelled
)

SELECT
    ski_field
    , country
    , year_col
    , month_col
    , datecol
    , day_of_season
    , facet_label
    , daily_snowfall_cm
    , cumulative_snowfall_cm
FROM cumulative
ORDER BY ski_field, year_col, datecol
