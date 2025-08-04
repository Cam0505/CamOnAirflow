-- models/base/base_filtered_ski_lifts.sql


SELECT *
FROM {{ source('ski_runs', 'ski_lifts') }}
WHERE 1=1
    -- Filter out magic carpets and platter lifts
    AND LOWER(COALESCE(lift_type, '')) NOT IN ('magic_carpet', 'platter', 'goods', 'station')
    
    -- Filter out learner lifts by name
    AND NOT (
        LOWER(COALESCE(name, '')) LIKE '%learner%' 
        OR LOWER(COALESCE(name, '')) LIKE '%beginner%'
    )
    
    -- Filter out very short lifts (likely not main infrastructure)
    AND COALESCE(lift_length_m, 0) >= 100
    
    -- Ensure we have coordinates
    AND top_lat IS NOT NULL 
    AND top_lon IS NOT NULL
    AND bottom_lat IS NOT NULL 
    AND bottom_lon IS NOT NULL