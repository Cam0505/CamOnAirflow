select distinct weathera, weatherb,
{{ dbt_utils.generate_surrogate_key(['weathera', 'weatherb'])}} as dim_weather_sk
-- FROM public.crash_data;
from {{ref('base_crash_data')}}