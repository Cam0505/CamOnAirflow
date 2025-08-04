WITH run_details AS (
    SELECT
        osm_id as run_osm_id,
        resort,
        COALESCE(run_name, 'Unnamed Run') as run_name,
        COALESCE(difficulty, 'unknown') as difficulty,
        run_length_m
    FROM {{ ref('base_filtered_ski_runs') }}
),
run_points AS (
    SELECT
        osm_id as run_osm_id,
        resort,
        lat,
        lon,
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
        r2_pts.progress_through_run as merge_point_progress,
        -- Distance between the points
        SQRT(
            POW(69.1 * (r1_pts.lat - r2_pts.lat), 2) + 
            POW(69.1 * (r1_pts.lon - r2_pts.lon) * COS(r2_pts.lat / 57.3), 2)
        ) * 1609.34 AS connection_distance_m,
        -- Calculate remaining distance on "to" run from merge point to end
        r2_details.run_length_m * (1.0 - r2_pts.progress_through_run) as remaining_run_distance_m,
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
        remaining_run_distance_m,  -- NEW: Distance remaining on the "to" run
        merge_point_progress,      -- NEW: How far through the "to" run the merge happens
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
        remaining_run_distance_m,  -- NEW: Full distance of the "to" run (since it starts at beginning)
        merge_point_progress       -- NEW: Should be 0 for splits since connecting to start
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
        remaining_run_distance_m,
        merge_point_progress
    FROM merge_connections
    WHERE connection_rank = 1  -- Only one merge per run end
        AND remaining_run_distance_m > 0  -- Filter out connections with no remaining distance
    
    UNION ALL
    
    -- Split connections (multiple allowed - runs can split into many)
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
        remaining_run_distance_m,
        merge_point_progress
    FROM split_connections
    WHERE remaining_run_distance_m > 0  -- Filter out connections with no remaining distance
)

SELECT * FROM final_connections
ORDER BY resort, from_run_name, connection_type, connection_distance_m