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
        SELECT COALESCE(max(date_col), DATE '1989-12-31') AS max_date FROM {{ this }}
    {% else %}
        SELECT DATE '1989-12-31' AS max_date
    {% endif %}
),
date_series AS (
    -- Start from the day after max_existing_date
    SELECT max_date + INTERVAL 1 day AS date FROM max_existing_date
    UNION ALL
    SELECT date + INTERVAL 1 day
    FROM date_series
    WHERE date + INTERVAL 1 day < DATE('2030-01-01')
)
SELECT
    date AS date_col,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    strftime('%B', date) AS month_name,
    strftime('%A', date) AS weekday_name,
    EXTRACT(DOW FROM date) AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    EXTRACT(DOY FROM date) AS day_of_year,
    EXTRACT(WEEK FROM date) AS week_of_year,
    EXTRACT(QUARTER FROM date) AS quarter
FROM date_series
WHERE date < DATE('2030-01-01')