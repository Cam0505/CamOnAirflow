-- ------------------------------------------------------------------------------
-- Model: Dim Date
-- Description: Dimension Table, date information
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-25 | Cam      | Initial creation
-- 2025-06-22 | Cam      | Added incremental logic and additional date attributes
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------


{{ config(
    materialized='incremental',
    unique_key='date_col'
) }}

WITH RECURSIVE
max_existing_date AS (
    {% if is_incremental() %}
        SELECT COALESCE(MAX(date_col), DATE '1989-12-31') AS max_date FROM {{ this }}
    {% else %}
        SELECT DATE '1989-12-31' AS max_date
    {% endif %}
)

, date_series AS (
    -- Start from the day after max_existing_date
    SELECT max_date + INTERVAL 1 DAY AS date_col FROM max_existing_date
    UNION ALL
    SELECT date_col + INTERVAL 1 DAY
    FROM date_series
    WHERE date_col + INTERVAL 1 DAY < DATE('2030-01-01')
)

SELECT
    date_col
    , EXTRACT(YEAR FROM date_col) AS year_col
    , EXTRACT(MONTH FROM date_col) AS month_col
    , EXTRACT(DAY FROM date_col) AS day_col
    , STRFTIME('%B', date_col) AS month_name
    , STRFTIME('%A', date_col) AS weekday_name
    , EXTRACT(DOW FROM date_col) AS day_of_week
    , COALESCE(EXTRACT(DOW FROM date_col) IN (0, 6), FALSE) AS is_weekend
    , EXTRACT(DOY FROM date_col) AS day_of_year
    , EXTRACT(WEEK FROM date_col) AS week_of_year
    , EXTRACT(QUARTER FROM date_col) AS quarter_col
FROM date_series
WHERE date_col < DATE('2030-01-01')