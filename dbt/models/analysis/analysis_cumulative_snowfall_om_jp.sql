with daily_winter_snowfall as (
    select
        ski_field
        , date as datecol
        , country
        , extract(month from date) as month_col
        , case
            when extract(month from date) in (11, 12) then extract(year from date) + 1
            else extract(year from date)
        end as year_col
        , coalesce(om_direct_snowfall_cm, 0.0) as om_direct_daily_snowfall_cm
        , coalesce(depth_precip_blend_cm, 0.0) as depth_precip_blend_daily_snowfall_cm
    from {{ ref('base_ski_field_snowfall_modeled') }}
    where country = 'JP'
      and extract(month from date) in (11, 12, 1, 2, 3, 4)
)

, labelled as (
    select
        *
        , concat(country, ' - ', ski_field) as facet_label
        , row_number() over (
            partition by ski_field, year_col
            order by datecol
        ) as day_of_season
    from daily_winter_snowfall
)

, cumulative as (
    select
        ski_field
        , country
        , year_col
        , month_col
        , datecol
        , facet_label
        , day_of_season
        , om_direct_daily_snowfall_cm
        , depth_precip_blend_daily_snowfall_cm
        , sum(om_direct_daily_snowfall_cm) over (
            partition by ski_field, year_col
            order by datecol
            rows between unbounded preceding and current row
        ) as om_direct_cumulative_snowfall_cm
        , sum(depth_precip_blend_daily_snowfall_cm) over (
            partition by ski_field, year_col
            order by datecol
            rows between unbounded preceding and current row
        ) as depth_precip_blend_cumulative_snowfall_cm
    from labelled
)

select
    ski_field
    , country
    , year_col
    , month_col
    , datecol
    , day_of_season
    , facet_label
    , om_direct_daily_snowfall_cm
    , depth_precip_blend_daily_snowfall_cm
    , om_direct_cumulative_snowfall_cm
    , depth_precip_blend_cumulative_snowfall_cm
from cumulative
order by ski_field, year_col, datecol
