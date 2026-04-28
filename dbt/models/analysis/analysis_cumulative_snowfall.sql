{% set snowfall_models = [
    ('ECMWF IFS', 'snowfall_ecmwf_ifs_cm'),
    ('JMA Seamless', 'snowfall_jma_seamless_cm')
] %}

with daily_seasonal_snowfall as (
    select
        ski_field
        , date as datecol
        , country
        , extract(month from date) as month_col
        , case
            when country in ('NZ', 'AU') then extract(year from date)
            when extract(month from date) in (11, 12) then extract(year from date) + 1
            else extract(year from date)
        end as year_col
        , snowfall_ecmwf_ifs_cm
        , snowfall_jma_seamless_cm
    from {{ ref('base_ski_field_snowfall_modeled') }}
    where (
        country in ('JP')
        and extract(month from date) in (11, 12, 1, 2, 3, 4)
    ) or (
        country in ('NZ', 'AU')
        and extract(month from date) in (6, 7, 8, 9, 10, 11)
    )
)

, model_daily as (
    {% for model_name, column_name in snowfall_models %}
    select
        ski_field
        , country
        , year_col
        , month_col
        , datecol
        , '{{ model_name }}' as model_name
        , coalesce({{ column_name }}, 0.0) as daily_snowfall_cm
    from daily_seasonal_snowfall
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

, labelled as (
    select
        *
        , concat(country, ' - ', ski_field) as facet_label
        , row_number() over (
            partition by ski_field, country, year_col, model_name
            order by datecol
        ) as day_of_season
    from model_daily
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
        , model_name
        , daily_snowfall_cm
        , sum(daily_snowfall_cm) over (
            partition by ski_field, country, year_col, model_name
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
    , model_name
    , daily_snowfall_cm
    , cumulative_snowfall_cm
from cumulative
order by country, ski_field, year_col, model_name, datecol
