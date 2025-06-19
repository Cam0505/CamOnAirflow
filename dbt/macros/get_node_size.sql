{% macro get_node_size(number_of_points) %}
    -- Calculate node size based on number of points
    CASE
        WHEN {{ number_of_points }} IS NULL THEN 30    -- Default size
        WHEN {{ number_of_points }} <= 2 THEN 30       -- Smallest
        WHEN {{ number_of_points }} <= 6 THEN 40       -- Small-Medium
        WHEN {{ number_of_points }} <= 10 THEN 50      -- Medium
        WHEN {{ number_of_points }} <= 14 THEN 60      -- Medium-Large
        ELSE 70                                        -- Largest
    END
{% endmacro %}