

SELECT
  location,
  country,
  date,
  timezone,
  COUNT(*) as hours_count,
  AVG(temperature_2m) as mean_temperature,
  SUM(precipitation) as total_precip,
  SUM(snowfall) as total_snow,
  AVG(cloudcover) as mean_cloud,
  AVG(windspeed_10m) as mean_wind,
  AVG(dew_point_2m) as mean_dewpoint,
  AVG(surface_pressure) as mean_pressure,
  AVG(relative_humidity_2m) as mean_rh,
  AVG(shortwave_radiation) as mean_shortwave,
  SUM(sunshine_duration) / 3600 as sunshine_hours,
  SUM(is_day) as daylight_hours
FROM {{ source('ice_climbing', 'weather_hourly_raw') }}
GROUP BY location, country, date, timezone