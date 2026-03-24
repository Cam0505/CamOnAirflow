{{ config(materialized='table') }}

/*
Daily spectral feature table for downstream fusion with weather and terrain data.

This model standardizes and enriches daily NDSI/NDWI/NDII time series from
satellite-derived ice indices with rolling, anomaly, and quality features.
*/

with src as (
    select
        cast(date as date) as spectral_date
        , location
        , cast(ndsi as double) as ndsi
        , cast(ndsi_smooth as double) as ndsi_smooth
        , cast(ndwi as double) as ndwi
        , cast(ndwi_smooth as double) as ndwi_smooth
        , cast(ndii as double) as ndii
        , cast(ndii_smooth as double) as ndii_smooth
    from {{ source('Spectral_Analysis', 'ice_indices') }}
)

, deduped as (
    select
        *
        , row_number() over (
            partition by location, spectral_date
            order by
                (
                    case when ndsi is not null then 1 else 0 end
                    + case when ndwi is not null then 1 else 0 end
                    + case when ndii is not null then 1 else 0 end
                    + case when ndsi_smooth is not null then 1 else 0 end
                    + case when ndwi_smooth is not null then 1 else 0 end
                    + case when ndii_smooth is not null then 1 else 0 end
                ) desc
        ) as row_num
    from src
)

, base as (
    select
        spectral_date
        , location
        , ndsi
        , ndsi_smooth
        , ndwi
        , ndwi_smooth
        , ndii
        , ndii_smooth
        , coalesce(ndsi_smooth, ndsi) as ndsi_best
        , coalesce(ndwi_smooth, ndwi) as ndwi_best
        , coalesce(ndii_smooth, ndii) as ndii_best
        , extract(year from spectral_date) as year_col
        , extract(month from spectral_date) as month_col
        , extract(day from spectral_date) as day_col
        , extract(doy from spectral_date) as day_of_year
        , extract(isodow from spectral_date) as iso_day_of_week
    from deduped
    where row_num = 1
)

, with_core_features as (
    select
        *
        , ndsi_best - ndwi_best as ndsi_minus_ndwi
        , ndsi_best - ndii_best as ndsi_minus_ndii
        , ndwi_best - ndii_best as ndwi_minus_ndii

        , case
            when ndsi_best is not null and ndsi_best between -1.2 and 1.2 then 0
            when ndsi_best is null then null
            else 1
        end as ndsi_out_of_expected_range_flag
        , case
            when ndwi_best is not null and ndwi_best between -1.2 and 1.2 then 0
            when ndwi_best is null then null
            else 1
        end as ndwi_out_of_expected_range_flag
        , case
            when ndii_best is not null and ndii_best between -1.2 and 1.2 then 0
            when ndii_best is null then null
            else 1
        end as ndii_out_of_expected_range_flag

        , case
            when ndsi_best is null and ndwi_best is null and ndii_best is null then 1
            else 0
        end as all_indices_null_flag
    from base
)

, with_lags as (
    select
        *
        , lag(spectral_date) over (partition by location order by spectral_date) as prev_spectral_date
        , lag(ndsi_best) over (partition by location order by spectral_date) as ndsi_prev_day
        , lag(ndwi_best) over (partition by location order by spectral_date) as ndwi_prev_day
        , lag(ndii_best) over (partition by location order by spectral_date) as ndii_prev_day
    from with_core_features
)

, with_deltas as (
    select
        *
        , date_diff('day', prev_spectral_date, spectral_date) as days_since_prev_observation
        , ndsi_best - ndsi_prev_day as ndsi_change_vs_prev_obs
        , ndwi_best - ndwi_prev_day as ndwi_change_vs_prev_obs
        , ndii_best - ndii_prev_day as ndii_change_vs_prev_obs
    from with_lags
)

, with_climatology as (
    select
        location
        , day_of_year
        , avg(ndsi_best) as ndsi_dayofyear_avg
        , avg(ndwi_best) as ndwi_dayofyear_avg
        , avg(ndii_best) as ndii_dayofyear_avg
    from with_deltas
    group by location, day_of_year
)


, final as (
    select
        r.spectral_date
        , r.location
        , r.year_col
        , r.month_col
        , r.day_col
        , r.day_of_year
        , r.iso_day_of_week

        , r.ndsi
        , r.ndsi_smooth
        , r.ndsi_best
        , r.ndwi
        , r.ndwi_smooth
        , r.ndwi_best
        , r.ndii
        , r.ndii_smooth
        , r.ndii_best

        , r.ndsi_minus_ndwi
        , r.ndsi_minus_ndii
        , r.ndwi_minus_ndii

        , r.days_since_prev_observation
        , r.ndsi_prev_day
        , r.ndwi_prev_day
        , r.ndii_prev_day
        , r.ndsi_change_vs_prev_obs
        , r.ndwi_change_vs_prev_obs
        , r.ndii_change_vs_prev_obs

        , c.ndsi_dayofyear_avg
        , c.ndwi_dayofyear_avg
        , c.ndii_dayofyear_avg
        , r.ndsi_best - c.ndsi_dayofyear_avg as ndsi_dayofyear_anomaly
        , r.ndwi_best - c.ndwi_dayofyear_avg as ndwi_dayofyear_anomaly
        , r.ndii_best - c.ndii_dayofyear_avg as ndii_dayofyear_anomaly

        , r.ndsi_out_of_expected_range_flag
        , r.ndwi_out_of_expected_range_flag
        , r.ndii_out_of_expected_range_flag
        , r.all_indices_null_flag
    from with_deltas as r
    left join with_climatology as c
        on r.location = c.location
        and r.day_of_year = c.day_of_year
)

select *
from final
order by location, spectral_date