-- ==============================================================================
-- [INTENT — DO NOT REMOVE] base_filtered_ski_lifts
-- Removes lift types that are not meaningful for route-planning:
--   magic_carpet, platter, goods, station — too slow / non-public
--   learner / beginner lifts — excluded by name pattern match
--   lifts shorter than 100m — likely connector infrastructure, not ski lifts
-- osm_id 1394839385 is a known bad record and is hard-excluded.
-- Coordinate NULLs are filtered because downstream spatial joins require all
-- four terminal positions (top + bottom lat/lon).
-- ==============================================================================

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
    AND osm_id <> 1394839385