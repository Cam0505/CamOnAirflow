WITH daily_winter_snowfall AS (
    SELECT
        ski_field
        , datecol
        , daily_snowfall_cm
        , country
        , year_col
        , month_col
    FROM {{ ref('base_ski_field_snowfall_modeled') }}
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
