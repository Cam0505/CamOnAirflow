select light_conditions, streetlight, dim_streetlight_sk
--FROM public_staging_source.staging_source_crash_streetlights
from {{ref('staging_source_crash_streetlights')}}