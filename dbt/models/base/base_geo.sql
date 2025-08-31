SELECT
    city_id
    , city
    , latitude
    , longitude
    , country_code
    , country
    , region
    , continent
FROM {{ source("geo", "geo_cities") }}
WHERE country IN ('New Zealand', 'United Kingdom', 'Australia', 'Canada')