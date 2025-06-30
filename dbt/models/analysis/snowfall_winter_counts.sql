

with categories as (
FROM {{ source('snowfall', 'ski_field_snowfall') }}
select
  date,
	location,
  case 
    when snowfall <= 1 then '<=1'
    when snowfall > 1 and snowfall <= 5 then '>1and<=5'
    when snowfall > 5 and snowfall <= 15 then '>5and<=15'
    when snowfall > 15 and snowfall <= 25 then '>15and<=25'
    when snowfall > 25 and snowfall <= 50 then '>25and<=50'
    when snowfall > 50 and snowfall <= 100 then '>50and<=100'
    when snowfall > 100 then '>100'
    else 'NULL Entry or Error' end as snowfall_category,
  case 
    when snowfall <= 1 then 1
    when snowfall > 1 and snowfall <= 5 then 2
    when snowfall > 5 and snowfall <= 15 then 3
    when snowfall > 15 and snowfall <= 25 then 4
    when snowfall > 25 and snowfall <= 50 then 5
    when snowfall > 50 and snowfall <= 100 then 6
    when snowfall > 100 then 7
    else 8 end as snowfall_order
)

select 
  year(date) as Season, location as ski_field, snowfall_category,
  count(*) as countx
  from categories
  where location = 'Cardrona'
  group by year(date), location, snowfall_category, snowfall_order
order by year(date) asc, location asc, snowfall_order asc