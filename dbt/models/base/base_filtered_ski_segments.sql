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

union all

-- 🔧 Manual connector: link 1394841139 → 1394841137
select
    1394841139 as run_osm_id,        -- upstream run
    999 as segment_index,            -- dummy high index
    12911386761 as from_node_id,     -- end node of 1394841139
    12911386741 as to_node_id,       -- start node of 1394841137
    null as from_lat,
    null as from_lon,
    null as to_lat,
    null as to_lon,
    0.0 as length_m,                 -- assume no distance (manual join)
    0.0 as gradient,                 -- flat connector
    'Cardrona Alpine Resort' as resort,
    'manual_connector' as run_name,
    null as difficulty
