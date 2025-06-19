{% macro get_edge_weight(driving_distance_km) %}
    -- Calculate edge weight based on driving distance (inverse relationship)
    -- This makes closer stations have thicker lines
    CASE
        WHEN {{ driving_distance_km }} IS NULL THEN 1  -- Default for missing data
        WHEN {{ driving_distance_km }} <= 1 THEN 5     -- Very close: thickest
        WHEN {{ driving_distance_km }} <= 3 THEN 4     -- Close
        WHEN {{ driving_distance_km }} <= 5 THEN 3     -- Moderate
        WHEN {{ driving_distance_km }} <= 8 THEN 2     -- Far
        ELSE 1                                         -- Very far: thinnest
    END
{% endmacro %}