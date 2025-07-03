select
    country_code
    , country
    , country_sk
from {{ ref('staging_geo') }}
group by country_code, country, country_sk