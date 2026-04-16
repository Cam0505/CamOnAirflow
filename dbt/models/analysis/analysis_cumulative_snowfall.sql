with daily_winter_snowfall as (
    select
        location as ski_field
        , date as datecol
        , elev_adjusted_snowfall_cm as daily_snowfall_cm
        , country
        , extract(year from date) as calendar_year
        , extract(month from date) as month_col
        , season_year as year_col
    from {{ ref('base_jp_ski_field_weather') }}
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
        , daily_snowfall_cm
        , sum(daily_snowfall_cm) over (
            partition by ski_field, year_col
            order by datecol
            rows between unbounded preceding and current row
        ) as cumulative_snowfall_cm
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
    , daily_snowfall_cm
    , cumulative_snowfall_cm
from cumulative
order by ski_field, year_col, datecol
