WITH run_details AS (
    SELECT
        osm_id as run_osm_id,
        resort,
        COALESCE(run_name, 'Unnamed Run') as run_name,
        COALESCE(difficulty, 'unknown') as difficulty,
        run_length_m,
        top_elevation_m,
        bottom_elevation_m,
        (top_elevation_m - bottom_elevation_m) as total_vertical_drop_m
    FROM {{ ref('base_filtered_ski_runs') }}
),
run_points AS (
    SELECT
        osm_id as run_osm_id,
        resort,
        lat,
        lon,
        distance_along_run_m,
        point_index as point_order,
        -- Calculate max point order for each run using window function
        MAX(point_index) OVER (PARTITION BY osm_id) as max_point_order
    FROM {{ ref('base_filtered_ski_points') }}
),
run_points_with_position AS (
    SELECT
        run_osm_id,
        resort,
        lat,
        lon,
        point_order,
        max_point_order,
        distance_along_run_m
        -- Calculate position based on actual point numbers (not percentages)
        CASE 
            WHEN point_order <= 0 THEN 'start'  -- First point is "start"
            WHEN point_order >= (max_point_order - 2) THEN 'end'  -- Last point is "end" 
            ELSE 'middle'
        END as point_position,
        -- Calculate percentage through the run
        CASE 
            WHEN max_point_order > 0 THEN point_order::FLOAT / max_point_order::FLOAT
            ELSE 0
        END as progress_through_run
    FROM run_points
),
potential_intersections AS (
    -- Find connections between runs
    SELECT
        r1_pts.run_osm_id as from_run_osm_id,
        r1_details.run_name as from_run_name,
        r2_pts.run_osm_id as to_run_osm_id,
        r2_details.run_name as to_run_name,
        r1_pts.resort,
        r1_pts.lat as from_lat,
        r1_pts.lon as from_lon,
        r2_pts.lat as to_lat,
        r2_pts.lon as to_lon,
        r1_pts.point_position as from_point_position,
        r2_pts.point_position as to_point_position,
        
        -- CRITICAL: Track progress on BOTH runs
        r1_pts.progress_through_run as from_run_exit_progress,  -- Where we EXIT the first run
        r2_pts.progress_through_run as to_run_entry_progress,   -- Where we ENTER the second run
        
        -- Distance between the points
        SQRT(
            POW(69.1 * (r1_pts.lat - r2_pts.lat), 2) + 
            POW(69.1 * (r1_pts.lon - r2_pts.lon) * COS(r2_pts.lat / 57.3), 2)
        ) * 1609.34 AS connection_distance_m,
        
        -- Calculate SKIED distance on "from" run (from start to exit point)
        r1_details.run_length_m * r1_pts.progress_through_run as skied_distance_from_run_m,
        r1_details.total_vertical_drop_m * r1_pts.progress_through_run as skied_elevation_from_run_m,
        
        -- Calculate remaining distance on "to" run from entry point to end
        r2_details.run_length_m * (1.0 - r2_pts.progress_through_run) as remaining_distance_to_run_m,
        r2_details.total_vertical_drop_m * (1.0 - r2_pts.progress_through_run) as remaining_elevation_to_run_m,
        
        -- Elevation difference (positive means from_run is higher - going downhill)
        (r1_pts.lat - r2_pts.lat) * 111000 as elevation_difference_m,
        
        -- Add row number to pick the closest connection between each pair of runs
        ROW_NUMBER() OVER (
            PARTITION BY r1_pts.run_osm_id, r2_pts.run_osm_id, r1_pts.point_position
            ORDER BY 
                -- Prioritize connections to start of runs first
                CASE WHEN r2_pts.point_position = 'start' THEN 0 ELSE 1 END,
                -- Then by distance (closest first)
                SQRT(
                    POW(69.1 * (r1_pts.lat - r2_pts.lat), 2) + 
                    POW(69.1 * (r1_pts.lon - r2_pts.lon) * COS(r2_pts.lat / 57.3), 2)
                ) * 1609.34 ASC
        ) as closest_connection_rank
    FROM run_points_with_position r1_pts
    INNER JOIN run_details r1_details ON r1_pts.run_osm_id = r1_details.run_osm_id
    INNER JOIN run_points_with_position r2_pts ON (
        r1_pts.resort = r2_pts.resort
        AND r1_pts.run_osm_id != r2_pts.run_osm_id  -- Can't join itself
    )
    INNER JOIN run_details r2_details ON r2_pts.run_osm_id = r2_details.run_osm_id
    -- Get first points of each run for comparison
    LEFT JOIN (
        SELECT 
            run_osm_id,
            lat as first_lat,
            lon as first_lon
        FROM run_points_with_position 
        WHERE point_order = 0
    ) r1_first ON r1_pts.run_osm_id = r1_first.run_osm_id
    LEFT JOIN (
        SELECT 
            run_osm_id,
            lat as first_lat,
            lon as first_lon
        FROM run_points_with_position 
        WHERE point_order = 0
    ) r2_first ON r2_pts.run_osm_id = r2_first.run_osm_id
    WHERE 1=1
        -- FROM run can only connect from middle or end (NOT start)
        AND r1_pts.point_position IN ('middle', 'end')
        -- Distance must be within reasonable range (30m for intersection)
        AND SQRT(
            POW(69.1 * (r1_pts.lat - r2_pts.lat), 2) + 
            POW(69.1 * (r1_pts.lon - r2_pts.lon) * COS(r2_pts.lat / 57.3), 2)
        ) * 1609.34 <= 30
        -- Prevent impossible connections: start of one run to end of another (uphill)
        AND NOT (r1_pts.point_position = 'start' AND r2_pts.point_position = 'end')
        -- Allow slight uphill for GPS inaccuracy, but prevent major uphill connections
        AND (r1_pts.lat - r2_pts.lat) * 111000 > -10  -- Allow up to 10m uphill for GPS error
        -- Prevent connections between runs that start at exactly the same coordinates
        AND NOT (r1_first.first_lat = r2_first.first_lat AND r1_first.first_lon = r2_first.first_lon)
),
deduplicated_intersections AS (
    -- Keep only the closest connection between each pair of runs for each connection type
    SELECT *
    FROM potential_intersections
    WHERE closest_connection_rank = 1  -- Only the closest connection between each run pair
),
merge_connections AS (
    -- MERGE: Run at its END merges into another run (one-to-one)
    SELECT 
        from_run_osm_id,
        from_run_name,
        to_run_osm_id,
        to_run_name,
        resort,
        'merge_into' as connection_type,
        connection_distance_m,
        elevation_difference_m,
        to_point_position as connection_point_type,
        skied_distance_from_run_m,           -- How much we ski of the FROM run
        skied_elevation_from_run_m,          -- Elevation drop on the FROM run
        remaining_distance_to_run_m,         -- Distance remaining on the "to" run
        remaining_elevation_to_run_m,        -- Elevation drop remaining on the "to" run
        from_run_exit_progress,              -- Where we exit the FROM run (0-1)
        to_run_entry_progress,               -- Where we enter the TO run (0-1)
        ROW_NUMBER() OVER (
            PARTITION BY from_run_osm_id 
            ORDER BY connection_distance_m ASC
        ) as connection_rank
    FROM deduplicated_intersections
    WHERE from_point_position = 'end'  -- From run must be at its end
),
split_connections AS (
    -- SPLIT: Run at its MIDDLE splits off to start of another run (one-to-many)
    SELECT 
        from_run_osm_id,
        from_run_name,
        to_run_osm_id,
        to_run_name,
        resort,
        'splits_to' as connection_type,
        connection_distance_m,
        elevation_difference_m,
        from_point_position as connection_point_type,
        skied_distance_from_run_m,           -- How much we ski of the FROM run
        skied_elevation_from_run_m,          -- Elevation drop on the FROM run
        remaining_distance_to_run_m,         -- Full distance of the "to" run (since it starts at beginning)
        remaining_elevation_to_run_m,        -- Full elevation drop of the "to" run (since it starts at beginning)
        from_run_exit_progress,              -- Where we exit the FROM run (0-1)
        to_run_entry_progress                -- Where we enter the TO run (0-1)
    FROM deduplicated_intersections
    WHERE from_point_position = 'middle'  -- From run splits at middle
        AND to_point_position = 'start'    -- To run starts at this point
),
final_connections AS (
    -- Merge connections (one per run - only the closest)
    SELECT
        from_run_osm_id,
        from_run_name,
        to_run_osm_id,
        to_run_name,
        resort,
        connection_type,
        connection_distance_m,
        elevation_difference_m,
        connection_point_type,
        skied_distance_from_run_m,           -- How much of the FROM run we actually ski
        skied_elevation_from_run_m,          -- How much elevation we drop on the FROM run
        remaining_distance_to_run_m,         -- How much of the TO run is left after entry
        remaining_elevation_to_run_m,        -- How much elevation is left on the TO run
        from_run_exit_progress,              -- Where we exit the FROM run (0-1)
        to_run_entry_progress               -- Where we enter the TO run (0-1)
    FROM merge_connections
    WHERE connection_rank = 1
        AND remaining_distance_to_run_m > 0
        AND remaining_elevation_to_run_m >= 0
    
    UNION ALL
    
    -- Split connections (multiple allowed)
    SELECT
        from_run_osm_id,
        from_run_name,
        to_run_osm_id,
        to_run_name,
        resort,
        connection_type,
        connection_distance_m,
        elevation_difference_m,
        connection_point_type,
        skied_distance_from_run_m,
        skied_elevation_from_run_m,
        remaining_distance_to_run_m,
        remaining_elevation_to_run_m,
        from_run_exit_progress,
        to_run_entry_progress
    FROM split_connections
    WHERE remaining_distance_to_run_m > 0
        AND remaining_elevation_to_run_m >= 0
)

SELECT * FROM final_connections
ORDER BY resort, from_run_name, connection_type, connection_distance_m