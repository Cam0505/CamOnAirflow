select
    city
    , latitude
    , longitude
    , region
    , city_sk
    , country_sk
from {{ ref('staging_geo') }}
group by city, latitude, longitude, region, city_sk, country_sk