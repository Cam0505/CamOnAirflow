WITH RECURSIVE date_series AS (
    SELECT DATE '2000-01-01' AS date
    UNION ALL
    SELECT (date + INTERVAL '1 day')::date
    FROM date_series
    WHERE date + INTERVAL '1 day' < DATE '2030-01-01'
)
SELECT
    date as date_col,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    TO_CHAR(date, 'Month') AS month_name,
    TO_CHAR(date, 'Day') AS weekday_name,
    EXTRACT(DOW FROM date) AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    EXTRACT(DOY FROM date) AS day_of_year,
    EXTRACT(WEEK FROM date) AS week_of_year,
    EXTRACT(QUARTER FROM date) AS quarter
FROM date_series