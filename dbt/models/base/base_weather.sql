-- ------------------------------------------------------------------------------
-- Model: Base_uv
-- Description: weather data from global api
-- ------------------------------------------------------------------------------
-- Change Log:
-- Date       | Author   | Description
-- -----------|----------|-------------------------------------------------------
-- 2025-05-07 | Cam      | Initial creation
-- YYYY-MM-DD | NAME     | [Add future changes here]
-- ------------------------------------------------------------------------------

SELECT date("date") as weather_date, city, temperature_max, temperature_min, windspeed_max, windgusts_max, 
location__lat as latitude, location__lng as longitude, temperature_mean, precipitation_sum, 
sunshine_duration
From {{ source("weather", "daily_weather") }} 