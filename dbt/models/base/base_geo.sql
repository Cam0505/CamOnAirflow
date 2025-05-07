SELECT city_id, city, latitude, longitude, country_code
From {{ source("geo", "geo_cities") }} 