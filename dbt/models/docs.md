# A
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

# C
# D
# E
# F
# G
# H
# I
# J
# K
# L
{% docs lift_time_sec %}
Estimated time to ride the lift in seconds. Priority: (1) actual line speed × length, (2) recorded duration tag, (3) type-based typical speed × length, (4) type-based fixed default, (5) 300-second global fallback.
{% enddocs %}

# M

{% docs model_name %}
Display name of the numerical weather model (e.g. ERA5, ECMWF IFS, JMA Seamless). Used as the grouping key for per-model comparisons.
{% enddocs %}

# N
# O
# P
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

# T

{% docs time_calculation_method %}
Indicates how lift_time_sec was derived: actual_speed (from recorded line speed), actual_duration (from OSM duration tag), calculated_from_type (type-based speed × length), type_default (type-based fixed seconds), or fallback_default (global 300-second constant).
{% enddocs %}

{% docs turniness_score %}
A composite score measuring the total angular deviation along a ski run's GPS trace. Higher values indicate more twisting or winding runs. Calculated from direction changes between consecutive GPS waypoints.
{% enddocs %}

# U
# V
# W
# X
# Y
# Z

