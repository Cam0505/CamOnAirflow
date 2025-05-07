SELECT id, stock, price::numeric::money, TO_TIMESTAMP(date_time, 'MM/DD/YYYY HH24:MI:SS')::timestamp without time zone as date_time,
round((max(price) over(partition by stock) - min(price) over(partition by stock))::numeric, 2)::numeric::money as price_spread,
round((last_value(price) over(partition by stock order by date_time)::numeric - first_value(price) over(partition by stock)::numeric), 2)::numeric::money as relative_price_movement,
round((last_value(price) over(partition by stock)::numeric - first_value(price) over(partition by stock)::numeric), 2)::numeric::money as abs_price_movement,
Count(id) over(partition by stock)::integer as Num_Stock_Entries
	-- FROM google_sheets.gsheet_finance
    From {{ source("gsheets", "gsheet_finance") }}