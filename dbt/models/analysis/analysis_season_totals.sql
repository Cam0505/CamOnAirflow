{% set models = [
    ('ECMWF IFS',             'ecmwf_ifs'),
    ('JMA Seamless',          'jma_seamless')
] %}

with seasonal_filtered as (
    select
        cast(date as date)                    as date
        , location                            as ski_field
        , country
        , extract(month from cast(date as date)) as month_col
        , case
            when country in ('NZ', 'AU') then extract(year from cast(date as date))
            when extract(month from cast(date as date)) in (11, 12)
                then extract(year from cast(date as date)) + 1
            else extract(year from cast(date as date))
        end as year_col
        {% for _, model_key in models %}
        , cast(coalesce(snowfall_{{ model_key }}_cm,           0.0) as double) as snowfall_{{ model_key }}_cm
        , cast(coalesce(precipitation_sum_{{ model_key }}_mm,  0.0) as double) as precip_{{ model_key }}_mm
        , cast(temperature_mean_{{ model_key }}_c                   as double) as temp_{{ model_key }}_c
        {% endfor %}
    from {{ source('snowfall', 'ski_field_snowfall') }}
    where (
        country in ('JP')
        and extract(month from cast(date as date)) in (11, 12, 1, 2, 3, 4)
    ) or (
        country in ('NZ', 'AU')
        and extract(month from cast(date as date)) in (6, 7, 8, 9, 10, 11)
    )
)

, model_season as (
    {% for model_name, model_key in models %}
    select
        ski_field
        , country
        , year_col
        , '{{ model_name }}' as model_name
        , sum(snowfall_{{ model_key }}_cm)                                  as season_snowfall_cm
        , sum(precip_{{ model_key }}_mm)                                    as season_precip_mm
        , avg(temp_{{ model_key }}_c)                                       as avg_temp_c
        , count_if(temp_{{ model_key }}_c is not null)                      as temp_obs_count
    from seasonal_filtered
    group by ski_field, country, year_col
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select
    ski_field
    , country
    , year_col
    , model_name
    , season_snowfall_cm
    , season_precip_mm
    , avg_temp_c
    , temp_obs_count
from model_season
order by country, ski_field, year_col, model_name
