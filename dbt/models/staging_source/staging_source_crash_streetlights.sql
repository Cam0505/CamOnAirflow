

select light as light_conditions, streetlight,
{{ dbt_utils.generate_surrogate_key(['light', 'streetlight'])}} as dim_streetlight_sk
--FROM public.base_crash_data
from {{ref('base_crash_data')}}
group by light, streetlight

