{% macro get_node_color(usage_type, number_of_points) %}
    CASE
        -- Color by usage type (primary colors)
        WHEN {{ usage_type }} = 'Public' THEN '#4CAF50'  -- Green
        WHEN {{ usage_type }} = 'Public - Pay At Location' THEN '#8BC34A'  -- Light Green
        WHEN {{ usage_type }} = 'Public - Membership Required' THEN '#CDDC39'  -- Lime
        WHEN {{ usage_type }} = 'Public - Notice Required' THEN '#FFC107'  -- Amber
        WHEN {{ usage_type }} = 'Private - For Staff, Visitors or Customers' THEN '#FF9800'  -- Orange
        WHEN {{ usage_type }} = 'Private - Restricted Access' THEN '#F44336'  -- Red
        WHEN {{ usage_type }} = 'Privately Owned - Notice Required' THEN '#E91E63'  -- Pink
        WHEN {{ usage_type }} = '(Unknown)' THEN '#9E9E9E'  -- Gray
        
        -- Fall back to coloring by number of points (if usage type not matched)
        ELSE
            CASE
                WHEN {{ number_of_points }} IS NULL THEN '#9E9E9E'  -- Gray
                WHEN {{ number_of_points }} <= 2 THEN '#E3F2FD'  -- Very light blue
                WHEN {{ number_of_points }} <= 4 THEN '#BBDEFB'  -- Light blue
                WHEN {{ number_of_points }} <= 6 THEN '#90CAF9'  -- Light-medium blue
                WHEN {{ number_of_points }} <= 8 THEN '#64B5F6'  -- Medium blue
                WHEN {{ number_of_points }} <= 10 THEN '#42A5F5'  -- Medium-dark blue
                WHEN {{ number_of_points }} <= 12 THEN '#2196F3'  -- Dark blue
                WHEN {{ number_of_points }} <= 14 THEN '#1E88E5'  -- Darker blue
                WHEN {{ number_of_points }} <= 16 THEN '#1976D2'  -- Very dark blue
                ELSE '#0D47A1'  -- Extremely dark blue
            END
    END
{% endmacro %}