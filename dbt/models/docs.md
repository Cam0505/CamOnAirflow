# A

{% docs arrival_airport %}
The ICAO code of the airport where the flight landed. Null if the arrival airport could not be determined.
{% enddocs %}

{% docs arrival_datetime %}
The UTC datetime when the flight arrived, derived from the last seen timestamp.
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
# D
{% docs departure_airport %}
The ICAO code of the airport where the flight departed. Null if the departure airport could not be determined.
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
# N
# O
# P
# Q
# R
{% docs region %}
A higher-level geographic grouping, such as a continent or internal administrative region. Optional but useful for rollups and segmentation.
{% enddocs %}
# S
# T
# U
# V
# W
# X
# Y
# Z
{% docs _dlt_id %}
The unique identifier assigned by DLT for this record.
{% enddocs %}

{% docs _dlt_load_id %}
The identifier for the DLT load batch that inserted this record.
{% enddocs %}