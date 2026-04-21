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
{% docs base_filtered_ski_lifts_description %}
Filtered and cleaned ski lift records from OpenStreetMap. Excludes magic carpets, platter lifts, goods/station lifts, lifts named as learner or beginner facilities, and lifts shorter than 100 m. Only lifts with complete top and bottom coordinates are retained.
{% enddocs %}

{% docs base_filtered_ski_points_description %}
GPS waypoints along each ski run, re-indexed so that point_index 0 always corresponds to the top of the run. Uphill-recorded runs are reversed and distance_along_run_m is recalculated accordingly. Used for segment and gradient downstream models.
{% enddocs %}

{% docs base_filtered_ski_runs_description %}
Cleaned and deduplicated ski run records from OpenStreetMap. Applies minimum length and point-count filters, normalises difficulty labels (collapsing extreme → intermediate, expert → advanced), and assigns generated display names to unnamed runs.
{% enddocs %}

{% docs base_filtered_ski_segments_description %}
Per-segment view of each ski run, joining GPS node coordinates and elevations onto the raw segment records. Gradient is blended from smoothed node gradients where available, falling back to elevation-over-length. Includes a manual connector record for a known topology gap.
{% enddocs %}

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
{% docs lift_time_sec %}
Estimated time to ride the lift in seconds. Priority: (1) actual line speed × length, (2) recorded duration tag, (3) type-based typical speed × length, (4) type-based fixed default, (5) 300-second global fallback.
{% enddocs %}

{% docs latitude %}
The latitude coordinate of the city in decimal degrees. Positive values indicate locations north of the equator.
{% enddocs %}

{% docs longitude %}
The longitude coordinate of the city in decimal degrees. Positive values indicate locations east of the Prime Meridian.
{% enddocs %}
# M

{% docs model_name %}
Display name of the numerical weather model (e.g. ERA5, ECMWF IFS, JMA Seamless). Used as the grouping key for per-model comparisons.
{% enddocs %}

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

{% docs season_year %}
The season identifier year. For Southern Hemisphere resorts (NZ, AU) this is the calendar year of the season. For Northern Hemisphere resorts (JP) November and December are attributed to the following calendar year so that a full season is grouped under a single year value.
{% enddocs %}

{% docs base_ski_field_snowfall_modeled_description %}
Daily snowfall per ski field for all tracked weather models. Raw source values are coalesced to 0.0 so every date/field row carries a complete numeric record. Each model column name encodes both the model key and the unit (cm).
{% enddocs %}

{% docs base_lift_run_mapping_description %}
Spatial join mapping lifts to the ski runs they service or connect to, using top and bottom coordinate proximity. Captures two connection types: lifts whose top terminal is near a run's top (lift_services_run) and lifts whose bottom terminal is near a run's top (lift_connects_to_run). Used for route planning and network graph construction.
{% enddocs %}

{% docs base_ski_gradient_stats_description %}
Aggregated gradient statistics per resort and difficulty bucket. Summarises average and maximum steepest gradient values across all runs in each group. Useful for resort-level difficulty profiling.
{% enddocs %}

{% docs base_ski_lift_times_description %}
Estimated ride times for each ski lift, applying a cascading fallback from actual speed data through type-based defaults. Exposes the calculation method used so downstream consumers can assess data quality.
{% enddocs %}

{% docs base_ski_run_gradients_description %}
Per-run gradient summary derived from GPS point elevations and segment distances. Captures average gradient and steepest segment gradient for each run, with steepness capped at a difficulty-appropriate threshold.
{% enddocs %}

{% docs base_ski_run_turniness_description %}
Turniness score per run from the filtered runs layer, with a per-metre density metric added. Filters to runs with a non-null turniness score only.
{% enddocs %}

{% docs analysis_cumulative_snowfall_description %}
Daily cumulative snowfall by ski field, season, and weather model. Applies season-window filtering (Nov–Apr for JP, Jun–Nov for NZ/AU), assigns an ordinal day-of-season to each row, and computes a running sum so each row carries the total snowfall accumulated from season start to that date.
{% enddocs %}

{% docs analysis_season_totals_description %}
Season-level aggregate snowfall, precipitation, and temperature per ski field and weather model. Applies the same season-window and hemisphere-aware year logic as the cumulative view, then summarises to one row per ski_field / year_col / model_name.
{% enddocs %}

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

{% docs time_calculation_method %}
Indicates how lift_time_sec was derived: actual_speed (from recorded line speed), actual_duration (from OSM duration tag), calculated_from_type (type-based speed × length), type_default (type-based fixed seconds), or fallback_default (global 300-second constant).
{% enddocs %}

{% docs turniness_score %}
A composite score measuring the total angular deviation along a ski run's GPS trace. Higher values indicate more twisting or winding runs. Calculated from direction changes between consecutive GPS waypoints.
{% enddocs %}

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