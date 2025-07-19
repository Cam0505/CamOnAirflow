WITH daily_winter_snowfall AS (
    SELECT
        location AS ski_field,
        date AS datecol,
        snowfall AS daily_snowfall_cm,
        country,
        EXTRACT(YEAR FROM date) AS year_col,
        EXTRACT(MONTH FROM date) AS month_col
    FROM {{ source('snowfall', 'ski_field_snowfall') }}
    WHERE
        EXTRACT(MONTH FROM date) IN (6, 7, 8, 9, 10, 11)
),

-- Assign a "season" (snow year) for faceting (you can adjust to June–May if needed)
labelled AS (
    SELECT
        *,
        CONCAT(country, ' - ', ski_field) AS facet_label,
        ROW_NUMBER() OVER (
            PARTITION BY ski_field, year_col
            ORDER BY datecol
        ) AS day_of_season
    FROM daily_winter_snowfall
),

cumulative AS (
    SELECT
        ski_field,
        country,
        year_col,
        month_col,
        datecol,
        facet_label,
        day_of_season,
        daily_snowfall_cm,
        SUM(daily_snowfall_cm) OVER (
            PARTITION BY ski_field, year_col
            ORDER BY datecol
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_snowfall_cm
    FROM labelled
)

SELECT
    ski_field,
    country,
    year_col,
    month_col,
    datecol,
    day_of_season,
    facet_label,
    daily_snowfall_cm,
    cumulative_snowfall_cm
FROM cumulative
ORDER BY ski_field, year_col, datecol
