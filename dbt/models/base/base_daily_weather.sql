SELECT
    location
    , country
    , date
    , timezone
    , COUNT(*) AS hours_count
    , AVG(temperature_2m) AS mean_temperature
    , SUM(precipitation) AS total_precip
    , SUM(snowfall) AS total_snow
    , AVG(cloudcover) AS mean_cloud
    , AVG(windspeed_10m) AS mean_wind
    , AVG(dew_point_2m) AS mean_dewpoint
    , AVG(surface_pressure) AS mean_pressure
    , AVG(relative_humidity_2m) AS mean_rh
    , AVG(shortwave_radiation) AS mean_shortwave
    , SUM(sunshine_duration) / 3600 AS sunshine_hours
    , SUM(is_day) AS daylight_hours
FROM {{ source('ice_climbing', 'weather_hourly_raw') }}
GROUP BY location, country, date, timezone