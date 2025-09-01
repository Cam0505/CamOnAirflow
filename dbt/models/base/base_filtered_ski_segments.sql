
select
	run_osm_id,
	segment_index,
	from_node_id,
	to_node_id,
	from_lat,
	from_lon,
	to_lat,
	to_lon,
	length_m,
	gradient,
	resort,
	run_name,
	difficulty
from {{ source('ski_runs', 'ski_run_segments') }}