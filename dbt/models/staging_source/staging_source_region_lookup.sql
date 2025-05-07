

select region, --tlaid,
-- {{ dbt_utils.generate_surrogate_key(['tlaid'])}} as dim_tla_sk,
{{ dbt_utils.generate_surrogate_key(['region'])}} as dim_region_sk
--FROM public.base_crash_data
from {{ref('base_crash_data')}}
--where tlaid is not null 
group by region--, tlaid