-- ==============================================================================
-- Model: base_openaq_sensor_summary
-- Description: Monthly summary statistics for OpenAQ sensor data
-- ==============================================================================
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|------------------------------------------------------
-- 2025-06-02 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ==============================================================================



with base as (
    select
        sensor_id,
        city,
        country,
        parameter,
        date_trunc('month', CAST(datetime AS TIMESTAMP)) as month,
        extract(year from CAST(datetime AS TIMESTAMP)) as year,
        extract(month from CAST(datetime AS TIMESTAMP)) as month_num,
        count(*) as num_records,
        count(distinct CAST(datetime AS DATE)) as days_reported,

        -- Value stats
        avg(value) as avg_value,
        min(value) as min_value,
        max(value) as max_value,
        stddev_pop(value) as stddev_value,
        sum(case when value is null then 1 else 0 end) as num_nulls,

        -- Unit (if mixed, will show one arbitrarily)
        min(unit) as unit,

        -- Summary statistics
        avg(summary__min) as avg_min,
        min(summary__min) as min_min,
        max(summary__min) as max_min,

        avg(summary__q02) as avg_q02,
        avg(summary__q25) as avg_q25,
        avg(summary__median) as avg_median,
        avg(summary__q75) as avg_q75,
        avg(summary__q98) as avg_q98,

        avg(summary__max) as avg_max,
        min(summary__max) as min_max,
        max(summary__max) as max_max,

        avg(summary__avg) as avg_avg,
        stddev_pop(summary__avg) as stddev_avg,

        avg(summary__sd) as avg_sd,
        stddev_pop(summary__sd) as stddev_sd,

        -- Coverage statistics
        avg(coverage__expected_count) as avg_expected_count,
        avg(coverage__observed_count) as avg_observed_count,
        avg(coverage__percent_complete) as avg_percent_complete,
        avg(coverage__percent_coverage) as avg_percent_coverage,

        min(coverage__datetime_from__utc) as min_datetime_from_utc,
        min(coverage__datetime_from__local) as min_datetime_from_local,
        max(coverage__datetime_to__utc) as max_datetime_to_utc,
        max(coverage__datetime_to__local) as max_datetime_to_local,

        min(CAST(datetime AS TIMESTAMP)) as first_record,
        max(CAST(datetime AS TIMESTAMP)) as last_record

    from {{ source('air_quality', 'openaq_daily') }}
    group by
        sensor_id,
        city,
        country,
        parameter,
        date_trunc('month', CAST(datetime AS TIMESTAMP)),
        extract(year from CAST(datetime AS TIMESTAMP)),
        extract(month from CAST(datetime AS TIMESTAMP))
)

select
    year,
    month_num as month,
    country,
    city,
    sensor_id,
    parameter,
    unit,
    num_records,
    days_reported,

    -- Value stats
    avg_value,
    min_value,
    max_value,
    stddev_value,
    num_nulls,

    -- Summary statistics
    avg_min,
    min_min,
    max_min,
    avg_q02,
    avg_q25,
    avg_median,
    avg_q75,
    avg_q98,
    avg_max,
    min_max,
    max_max,
    avg_avg,
    stddev_avg,
    avg_sd,
    stddev_sd,

    -- Coverage statistics
    avg_expected_count,
    avg_observed_count,
    avg_percent_complete,
    avg_percent_coverage,
    min_datetime_from_utc,
    min_datetime_from_local,
    max_datetime_to_utc,
    max_datetime_to_local,

    first_record,
    last_record

from base
order by country, city, sensor_id, parameter, year, month_num