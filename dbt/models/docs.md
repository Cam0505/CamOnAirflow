# A

{% docs arrival_airport %}
The ICAO code of the airport where the flight landed. Null if the arrival airport could not be determined.
{% enddocs %}

{% docs arrival_airport_flights %}
The number of flights arriving at the most popular arrival airport for the given period.
{% enddocs %}

{% docs arrival_datetime %}
The UTC datetime when the flight arrived, derived from the last seen timestamp.
{% enddocs %}

{% docs avg_distance_km %}
The average great-circle distance (in kilometers) flown by flights in the given period.
{% enddocs %}

{% docs avg_flight_time_minutes %}
The average flight time (in minutes) for flights in the given period.
{% enddocs %}

# B
{% docs base_geo_description %}
    The `base_geo` model contains foundational geographic data that can be used to enrich other datasets with location context. It includes information like city name, country, coordinates, and region grouping.

    This model is useful for joining geographic metadata into analytical datasets.
{% enddocs %}


# C
{% docs callsign %}
The callsign used by the aircraft for this flight, typically assigned by air traffic control and visible in ADS-B data.
{% enddocs %}

{% docs city %}
The name of the city. This field is typically used as a primary identifier for a location in the dataset.
{% enddocs %}

{% docs continent %}
The continent where the city is located. This field helps in broader geographic aggregations and global segmentation.
{% enddocs %}

{% docs country %}
The country where the city is located. Useful for aggregating or filtering by national boundaries.
{% enddocs %}

{% docs coverage__datetime_from__local %}
The local datetime of the first measurement for the day.
{% enddocs %}

{% docs coverage__datetime_from__utc %}
The UTC datetime of the first measurement for the day.
{% enddocs %}

{% docs coverage__datetime_to__local %}
The local datetime of the last measurement for the day.
{% enddocs %}

{% docs coverage__datetime_to__utc %}
The UTC datetime of the last measurement for the day.
{% enddocs %}

{% docs coverage__expected_count %}
The expected number of measurements for the day, based on the sensor's reporting frequency.
{% enddocs %}

{% docs coverage__expected_interval %}
The expected interval (in seconds) between measurements for the sensor.
{% enddocs %}

{% docs coverage__observed_count %}
The actual number of measurements observed for the day.
{% enddocs %}

{% docs coverage__observed_interval %}
The observed average interval (in seconds) between measurements for the sensor on this day.
{% enddocs %}

{% docs coverage__percent_complete %}
The percentage of expected measurements that were actually observed for the day.
{% enddocs %}

{% docs coverage__percent_coverage %}
The percentage of the day for which measurements were available.
{% enddocs %}

# D
{% docs departure_airport %}
The ICAO code of the airport where the flight departed. Null if the departure airport could not be determined.
{% enddocs %}

{% docs departure_airport_flights %}
The number of flights departing from the most popular departure airport for the given period.
{% enddocs %}

{% docs departure_datetime %}
The UTC datetime when the flight departed, derived from the first seen timestamp.
{% enddocs %}

{% docs distance_km %}
The great-circle distance between the departure and arrival airports, in kilometers. Null if either airport's coordinates are missing.
{% enddocs %}
# E
# F
# G
# H
# I
{% docs icao24 %}
The unique ICAO 24-bit address assigned to the aircraft's transponder.
{% enddocs %}
# J
# K
# L
{% docs latitude %}
The latitude coordinate of the city in decimal degrees. Positive values indicate locations north of the equator.
{% enddocs %}

{% docs longitude %}
The longitude coordinate of the city in decimal degrees. Positive values indicate locations east of the Prime Meridian.
{% enddocs %}
# M

{% docs month %}
The first day of the month (UTC) for the aggregation period.
{% enddocs %}

{% docs most_popular_arrival_airport %}
The ICAO code of the most popular arrival airport for the given period.
{% enddocs %}

{% docs most_popular_departure_airport %}
The ICAO code of the most popular departure airport for the given period.
{% enddocs %}
# N
# O
# P

{% docs pct_flights_arrival_airport %}
The percentage of flights in the period that arrived at the most popular arrival airport.
{% enddocs %}

{% docs pct_flights_departure_airport %}
The percentage of flights in the period that departed from the most popular departure airport.
{% enddocs %}
# Q
# R
{% docs region %}
A higher-level geographic grouping, such as a continent or internal administrative region. Optional but useful for rollups and segmentation.
{% enddocs %}
# S

{% docs summary__avg %}
The average value observed for the parameter in the daily summary.
{% enddocs %}

{% docs summary__max %}
The maximum value observed for the parameter in the daily summary.
{% enddocs %}

{% docs summary__median %}
The median value observed for the parameter in the daily summary.
{% enddocs %}

{% docs summary__min %}
The minimum value observed for the parameter in the daily summary.
{% enddocs %}

{% docs summary__q02 %}
The 2nd percentile value observed for the parameter in the daily summary.
{% enddocs %}

{% docs summary__q25 %}
The 25th percentile value observed for the parameter in the daily summary.
{% enddocs %}

{% docs summary__q75 %}
The 75th percentile value observed for the parameter in the daily summary.
{% enddocs %}

{% docs summary__q98 %}
The 98th percentile value observed for the parameter in the daily summary.
{% enddocs %}

{% docs summary__sd %}
The standard deviation of values for the parameter in the daily summary.
{% enddocs %}
# T

{% docs total_distance_km %}
The total great-circle distance (in kilometers) flown by all flights in the given period.
{% enddocs %}

{% docs total_flight_time_minutes %}
The total flight time (in minutes) for all flights in the given period.
{% enddocs %}

{% docs total_flights %}
The total number of flights in the given period.
{% enddocs %}
# U
# V
# W

{% docs week %}
The first day of the week (UTC) for the aggregation period.
{% enddocs %}
# X
# Y

{% docs year %}
The first day of the year (UTC) for the aggregation period.
{% enddocs %}
# Z
{% docs _dlt_id %}
The unique identifier assigned by DLT for this record.
{% enddocs %}

{% docs _dlt_load_id %}
The identifier for the DLT load batch that inserted this record.
{% enddocs %}