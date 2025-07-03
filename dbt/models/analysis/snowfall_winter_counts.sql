with categories as (
    from {{ source('snowfall', 'ski_field_snowfall') }}
    select
        date
        , location
        , country
        , case
            when snowfall <= 1 then '1cm or less'
            when snowfall > 1 and snowfall <= 5 then 'Between 1 & 5cm'
            when snowfall > 5 and snowfall <= 15 then 'Between 5 & 15cm'
            when snowfall > 15 and snowfall <= 25 then 'Between 15 & 25cm'
            when snowfall > 25 and snowfall <= 50 then 'Between 25 & 50cm'
            when snowfall > 50 and snowfall <= 100 then 'Between 50 & 100cm'
            when snowfall > 100 then 'More than 100cm'
            else 'NULL or Error'
        end as snowfall_category
        , case
            when snowfall <= 1 then 1
            when snowfall > 1 and snowfall <= 5 then 2
            when snowfall > 5 and snowfall <= 15 then 3
            when snowfall > 15 and snowfall <= 25 then 4
            when snowfall > 25 and snowfall <= 50 then 5
            when snowfall > 50 and snowfall <= 100 then 6
            when snowfall > 100 then 7
            else 8
        end as snowfall_order
)

select
    country
    , location as ski_field
    , snowfall_category
    , year(date) as season
    , count(*) as countx
from categories
group by year(date), country, location, snowfall_category, snowfall_order
order by year(date) asc, country, location asc, snowfall_order asc