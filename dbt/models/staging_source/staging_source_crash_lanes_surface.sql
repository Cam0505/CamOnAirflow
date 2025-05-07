

select roadlane, roadsurface,
{{ dbt_utils.generate_surrogate_key(['roadlane', 'roadsurface'])}} as dim_lanes_surface_sk,
1 as dbt_invoke
--FROM public.base_crash_data
from {{ref('base_crash_data')}}
group by roadlane, roadsurface