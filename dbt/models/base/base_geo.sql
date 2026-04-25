-- ==============================================================================
-- [INTENT — DO NOT REMOVE] base_geo
-- Thin filter over geo_cities: keep only the four countries this project tracks.
-- All downstream geo joins (dim_city, dim_country, staging_geo) depend on this
-- allowlist. Adding a new country means adding it here AND to the
-- accepted_values test in base_geo.yml.
-- ==============================================================================

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