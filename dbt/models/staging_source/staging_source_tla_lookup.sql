select region, tlaid, tlaname,
{{ dbt_utils.generate_surrogate_key(['tlaid', 'region'])}} as dim_tla_sk,
{{ dbt_utils.generate_surrogate_key(['region'])}} as dim_region_sk
--FROM public.crash_data
from {{ref('base_crash_data')}}
where tlaid is not null
group by region, tlaid, tlaname