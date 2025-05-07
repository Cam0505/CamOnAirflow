select crashseverity,
dim_crashseverity_sk
--FROM public_staging_source.staging_source_crash_severity
from {{ref('staging_source_crash_severity')}}