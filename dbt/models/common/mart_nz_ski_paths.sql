select 
    path_id,
    resort,
    starting_lift,
    starting_lift_id,
    run_count,
    total_distance_m,
    total_vertical_m * -1 as total_vertical_m,
    DEGREES(ATAN((avg_gradient_pct * -1) / 100.0)) as avg_gradient_deg,
    DEGREES(ATAN((max_gradient_pct * -1) / 100.0)) as max_gradient_deg,
    run_path,
    node_ids,
    ending_type,
    ending_name,
    ending_id
from {{ ref('staging_northisland_paths') }}

union all

select 
    path_id,
    resort,
    starting_lift,
    starting_lift_id,
    run_count,
    total_distance_m,
    total_vertical_m * -1 as total_vertical_m,
    DEGREES(ATAN((avg_gradient_pct * -1) / 100.0)) as avg_gradient_deg,
    DEGREES(ATAN((max_gradient_pct ) / 100.0)) as max_gradient_deg,
    run_path,
    node_ids,
    ending_type,
    ending_name,
    ending_id
from {{ ref('staging_southisland_paths') }}
