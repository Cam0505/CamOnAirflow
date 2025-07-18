select
    city_id
    , city
    , latitude
    , longitude
    , country_code
    , country
    , region
    , {{ dbt_utils.generate_surrogate_key(["city", "country"]) }} as city_sk
    , {{ dbt_utils.generate_surrogate_key(["country"]) }} as country_sk
from {{ ref('base_geo') }}