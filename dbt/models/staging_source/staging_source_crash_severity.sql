/*
    dbt run --select +staging_source_crash_severity
    Created: 03/11/2023 : Cam Martin - Initial Creation

*/

select crashseverity,
{{ dbt_utils.generate_surrogate_key(['crashseverity'])}} as dim_crashseverity_sk
--FROM public.base_crash_data
from {{ref('base_crash_data')}}
group by crashseverity
