
select
	osm_id,
	resort,
	country_code,
	run_name,
	point_index,
	lat,
	lon,
	distance_along_run_m,
	elevation_m,
	elevation_smoothed_m,
	gradient_smoothed,
	node_id
FROM {{ source('ski_runs', 'ski_run_points') }}