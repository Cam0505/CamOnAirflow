"""
Find all possible ski paths using tree traversal
"""
import pandas as pd
import networkx as nx
from typing import Dict, List

def model(dbt, session):
    """
    Build a tree of all possible ski paths from lift tops to endpoints.
    Uses actual distance calculation based on entry and exit points.
    """
    # Load data
    lift_run_mapping = dbt.ref("base_lift_run_mapping").df()
    ski_runs = dbt.ref("base_filtered_ski_runs").df()
    ski_points = dbt.ref("base_filtered_ski_points").df()

    # Process each resort separately
    all_paths = []

    for resort in lift_run_mapping['resort'].unique():
        print(f"Processing resort: {resort}")

        # Filter data for this resort
        resort_lifts = lift_run_mapping[lift_run_mapping['resort'] == resort]
        resort_runs = ski_runs[ski_runs['resort'] == resort]
        resort_points = ski_points[ski_points['resort'] == resort]

        print("Checking resort_lifts:", type(resort_lifts), resort_lifts.shape)
        print("Checking resort_runs:", type(resort_runs), resort_runs.shape)
        print("Checking resort_points:", type(resort_points), resort_points.shape)

        # Debug: print first few rows
        print("resort_lifts head:\n", resort_lifts.head())
        print("resort_runs head:\n", resort_runs.head())

        if len(resort_lifts) == 0 or len(resort_runs) == 0:
            print(f"Skipping {resort}: No lifts or runs")
            continue

        # Build the network
        print("Building network...")
        G = build_resort_network(resort_lifts, resort_runs, resort_points)

        print("Network nodes:", G.number_of_nodes())
        if G.number_of_nodes() == 0:
            print(f"Skipping {resort}: Empty network")
            continue

        # Find all paths
        print("Finding all paths...")
        resort_paths = find_all_paths(G, resort, resort_runs, resort_points)

        print(f"Found {len(resort_paths)} paths for {resort}")
        all_paths.extend(resort_paths)

    # Convert to DataFrame
    print("Converting all_paths to DataFrame...")
    if not all_paths:
        paths_df = pd.DataFrame(columns=[
            'resort', 'country_code', 'path_id', 'starting_lift_name', 'starting_lift_osm_id',
            'ending_point_type', 'ending_point_name', 'ending_point_osm_id', 'run_path_array',
            'run_path_names', 'run_count', 'total_path_length_m', 'total_vertical_drop_m',
            'avg_gradient', 'total_turniness_score', 'difficulty_mix', 'hardest_difficulty'
        ])
    else:
        paths_df = pd.DataFrame(all_paths)

    print("Returning paths_df with shape:", paths_df.shape)
    return paths_df

def build_resort_network(lift_run_mapping: pd.DataFrame, 
                         ski_runs: pd.DataFrame,
                         ski_points: pd.DataFrame) -> nx.DiGraph:
    """Build a network of ski runs and connections for the resort"""
    G = nx.DiGraph()

    # Create dictionaries for fast lookups
    run_points_dict = {}
    for _, row in ski_points.iterrows():
        osm_id = row['osm_id']
        if osm_id not in run_points_dict:
            run_points_dict[osm_id] = []
        run_points_dict[osm_id].append(row)

    # Add lift nodes
    for _, row in lift_run_mapping.iterrows():
        lift_id = f"lift_{row['lift_osm_id']}"

        if lift_id not in G:
            G.add_node(lift_id, 
                      node_type='lift',
                      resort=row['resort'],
                      osm_id=row['lift_osm_id'],
                      name=row['lift_name'])

    # Add run nodes
    for _, row in ski_runs.iterrows():
        run_id = f"run_{row['osm_id']}"

        if run_id not in G:
            G.add_node(run_id,
                      node_type='run',
                      resort=row['resort'],
                      osm_id=row['osm_id'],
                      name=row.get('run_name', 'Unknown'),
                      difficulty=row.get('difficulty', 'unknown'),
                      length_m=row.get('run_length_m', 0),
                      vertical_drop=row.get('top_elevation_m', 0) - row.get('bottom_elevation_m', 0),
                      turniness=row.get('turniness_score', 0),
                      points=run_points_dict.get(row['osm_id'], []))

    # Add lift-to-run connections
    for _, row in lift_run_mapping.iterrows():
        if row['connection_type'] == 'lift_services_run':
            lift_id = f"lift_{row['lift_osm_id']}"
            run_id = f"run_{row['run_osm_id']}"

            if lift_id in G and run_id in G:
                G.add_edge(lift_id, run_id,
                          connection_type='lift_services_run',
                          entry_point_index=row.get('run_start_point_index', 0),
                          entry_distance_m=row.get('run_start_distance_m', 0))

    # Add run-to-lift connections
    for _, row in lift_run_mapping.iterrows():
        if row['connection_type'] == 'run_feeds_lift':
            run_id = f"run_{row['run_osm_id']}"
            lift_id = f"lift_{row['lift_osm_id']}"

            if run_id in G and lift_id in G:
                G.add_edge(run_id, lift_id,
                          connection_type='run_feeds_lift',
                          exit_point_index=row.get('run_end_point_index', 0),
                          exit_distance_m=row.get('run_end_distance_m', 0))

    # Now find and add run-to-run connections by comparing all points
    # This is computationally expensive but will be accurate
    find_run_connections(G, ski_runs, ski_points)

    return G

def find_run_connections(G: nx.DiGraph, ski_runs: pd.DataFrame, ski_points: pd.DataFrame):
    """Find connections between runs by comparing points"""
    # Use KDTree for efficient spatial queries
    from scipy.spatial import cKDTree
    import numpy as np

    # Group points by resort for faster processing
    points_by_resort = {}
    for _, row in ski_points.iterrows():
        if row['resort'] not in points_by_resort:
            points_by_resort[row['resort']] = []
        points_by_resort[row['resort']].append(row)

    for resort, resort_points in points_by_resort.items():
        # Prepare arrays for KDTree
        coords = np.array([[p['lat'], p['lon']] for p in resort_points])
        kdtree = cKDTree(coords)

        # Map index to point
        idx_to_point = {i: p for i, p in enumerate(resort_points)}

        # For each point, find nearby points within 30m
        for i, point in enumerate(resort_points):
            point_idx = point['point_index']
            osm_id = point['osm_id']
            # Get max point index for this run
            run_points = [p for p in resort_points if p['osm_id'] == osm_id]
            max_idx = max([p['point_index'] for p in run_points])
            if point_idx == 0:
                continue
            if point_idx >= (max_idx - 2):
                point_position = 'end'
            else:
                point_position = 'middle'
            if point_position not in ['middle', 'end']:
                continue

            # Query KDTree for neighbors within 30m (0.00027 deg ~ 30m at equator)
            # But use meters for accuracy
            latlon = np.array([point['lat'], point['lon']])
            # Use 0.00027 as a rough degree window, but filter by meters below
            idxs = kdtree.query_ball_point(latlon, r=0.00027)
            for j in idxs:
                if j == i:
                    continue
                nearby = idx_to_point[j]
                if nearby['osm_id'] == osm_id:
                    continue
                # Calculate distance in meters
                distance = ((point['lat'] - nearby['lat'])**2 + (point['lon'] - nearby['lon'])**2)**0.5 * 111000
                if distance > 30:
                    continue
                # Get position of nearby point
                nearby_idx = nearby['point_index']
                nearby_osm = nearby['osm_id']
                nearby_run_points = [p for p in resort_points if p['osm_id'] == nearby_osm]
                nearby_max_idx = max([p['point_index'] for p in nearby_run_points])
                if nearby_idx == 0:
                    nearby_position = 'start'
                elif nearby_idx >= (nearby_max_idx - 2):
                    nearby_position = 'end'
                else:
                    nearby_position = 'middle'
                if point_position == 'start' and nearby_position == 'end':
                    continue
                elevation_diff = point.get('elevation_m', 0) - nearby.get('elevation_m', 0)
                if elevation_diff < -10:
                    continue
                from_run_id = f"run_{osm_id}"
                to_run_id = f"run_{nearby_osm}"
                from_run_points = [p for p in resort_points if p['osm_id'] == osm_id]
                to_run_points = [p for p in resort_points if p['osm_id'] == nearby_osm]
                if from_run_points and to_run_points:
                    from_start = next((p for p in from_run_points if p['point_index'] == 0), None)
                    to_start = next((p for p in to_run_points if p['point_index'] == 0), None)
                    if from_start is not None and to_start is not None:
                        start_distance = ((from_start['lat'] - to_start['lat'])**2 + (from_start['lon'] - to_start['lon'])**2)**0.5
                        if start_distance < 0.00001:
                            continue
                if from_run_id in G and to_run_id in G:
                    connection_type = 'splits_to' if point_position == 'middle' else 'merge_into'
                    G.add_edge(from_run_id, to_run_id,
                        connection_type=connection_type,
                        exit_point_index=point_idx,
                        entry_point_index=nearby_idx,
                        exit_distance_m=point['distance_along_run_m'],
                        entry_distance_m=nearby['distance_along_run_m'],
                        elevation_diff=elevation_diff,
                        connection_distance_m=distance)

# NOTE: For further optimization, consider precomputing and materializing features such as:
# - Run start/end points and their spatial index
# - Run-to-lift and run-to-run connection candidates
# - Efficiency metrics (e.g., time from run to lift, average speed, etc.)
# These can be stored in separate SQL or Python models for reuse in analysis.

def find_all_paths(G: nx.DiGraph, resort: str, 
                  ski_runs: pd.DataFrame, ski_points: pd.DataFrame, 
                  max_path_depth: int = 15) -> List[Dict]:
    """
    Find all possible ski paths using tree traversal.
    Correctly calculates distance as (exit_distance - entry_distance) for each segment.
    """
    paths = []
    path_id = 0

    # Find all lift tops as starting points
    start_nodes = [n for n in G.nodes() if (
        n.startswith('lift_') and 
        G.nodes[n].get('resort') == resort and 
        any(list(G.successors(n)))
    )]

    print(f"Found {len(start_nodes)} starting lifts")

    # For each lift top, find all possible paths
    for start_node in start_nodes:
        # Get lift info
        lift_name = G.nodes[start_node].get('name', 'Unknown Lift')
        lift_osm_id = G.nodes[start_node].get('osm_id', 0)

        # Get all runs serviced by this lift
        first_runs = list(G.successors(start_node))

        for first_run in first_runs:
            # Start the traversal
            path = [start_node, first_run]
            current_distance = 0
            current_elevation = 0

            # Get first run details
            # run_points = G.nodes[first_run].get('points', [])
            entry_distance = G.edges[start_node, first_run].get('entry_distance_m', 0)

            # Track distance within current run
            current_run_distance = 0

            # Process the path using DFS
            dfs_traverse(G, first_run, path, paths, path_id,
                       current_distance, current_elevation, current_run_distance,
                       entry_distance, lift_name, lift_osm_id,
                       max_path_depth)
            path_id += 1

    return paths

def dfs_traverse(G: nx.DiGraph, current_node: str, 
                current_path: List[str], all_paths: List[Dict], path_id: int,
                total_distance: float, total_elevation: float, 
                current_run_distance: float, entry_distance: float,
                starting_lift_name: str, starting_lift_osm_id: int,
                max_depth: int):
    """
    Traverse the graph depth-first, accurately calculating distances.
    """
    # Check depth limit
    if len(current_path) > max_depth or current_node.startswith('lift_'):
        # We've reached a lift or max depth - end the path
        process_complete_path(G, current_path, all_paths, path_id,
                            total_distance, total_elevation,
                            starting_lift_name, starting_lift_osm_id)
        return

    # Get successors (possible next segments)
    next_nodes = list(G.successors(current_node))

    if not next_nodes:
        # No more connections - this is an end point
        # We need to add the remaining distance in the current run
        run_data = G.nodes[current_node]
        total_run_length = run_data.get('length_m', 0)

        # Add remaining distance (from entry point to end)
        remaining_distance = total_run_length - entry_distance

        if remaining_distance > 0:
            total_distance += remaining_distance

            # Calculate remaining elevation drop
            total_vertical = run_data.get('vertical_drop', 0)
            if total_run_length > 0:
                remaining_elevation = total_vertical * (remaining_distance / total_run_length)
                total_elevation += remaining_elevation

        # Process the completed path
        process_complete_path(G, current_path, all_paths, path_id,
                            total_distance, total_elevation,
                            starting_lift_name, starting_lift_osm_id)
        return

    # For each possible next segment
    for next_node in next_nodes:
        if next_node in current_path:
            # Skip cycles
            continue

        # Calculate distance for this segment
        edge_data = G.edges[current_node, next_node]

        if next_node.startswith('run_'):
            # This is a run-to-run connection
            exit_distance = edge_data.get('exit_distance_m', 0)
            next_entry_distance = edge_data.get('entry_distance_m', 0)

            # Distance skied in this segment is exit_distance - entry_distance (of current run)
            segment_distance = exit_distance - entry_distance

            # Calculate elevation change for this segment
            run_data = G.nodes[current_node]
            total_run_length = run_data.get('length_m', 0)
            total_vertical = run_data.get('vertical_drop', 0)

            if total_run_length > 0:
                segment_elevation = total_vertical * (segment_distance / total_run_length)
            else:
                segment_elevation = 0

            # Update totals
            new_total_distance = total_distance + segment_distance
            new_total_elevation = total_elevation + segment_elevation

            # Continue traversal with updated values
            new_path = current_path + [next_node]

            dfs_traverse(G, next_node, new_path, all_paths, path_id,
                       new_total_distance, new_total_elevation,
                       0, next_entry_distance,  # Reset current run distance, set new entry point
                       starting_lift_name, starting_lift_osm_id,
                       max_depth)
        else:
            # This is a run-to-lift connection (end of path)
            # Calculate distance to lift
            exit_distance = edge_data.get('exit_distance_m', 0)
            segment_distance = exit_distance - entry_distance

            # Calculate elevation change
            run_data = G.nodes[current_node]
            total_run_length = run_data.get('length_m', 0)
            total_vertical = run_data.get('vertical_drop', 0)

            if total_run_length > 0:
                segment_elevation = total_vertical * (segment_distance / total_run_length)
            else:
                segment_elevation = 0

            # Update totals
            new_total_distance = total_distance + segment_distance
            new_total_elevation = total_elevation + segment_elevation

            # Add lift to path and process
            new_path = current_path + [next_node]

            process_complete_path(G, new_path, all_paths, path_id,
                                new_total_distance, new_total_elevation,
                                starting_lift_name, starting_lift_osm_id)

def process_complete_path(G: nx.DiGraph, path: List[str], 
                         all_paths: List[Dict], path_id: int,
                         total_distance: float, total_elevation: float,
                         starting_lift_name: str, starting_lift_osm_id: int):
    """Process a completed path and add to results."""
    # Skip trivial paths
    if len(path) < 2 or total_distance <= 0:
        return

    # Extract run nodes
    run_nodes = [n for n in path if n.startswith('run_')]

    if not run_nodes:
        return

    # Calculate metrics
    run_count = len(run_nodes)
    avg_gradient = (total_elevation / total_distance * 100) if total_distance > 0 else 0

    # Get ending point info
    last_node = path[-1]
    if last_node.startswith('lift_'):
        ending_point_type = 'lift'
        ending_point_name = G.nodes[last_node].get('name', 'Unknown')
        ending_point_osm_id = G.nodes[last_node].get('osm_id', 0)
    else:
        ending_point_type = 'end_of_run'
        ending_point_name = G.nodes[last_node].get('name', 'Unknown')
        ending_point_osm_id = G.nodes[last_node].get('osm_id', 0)

    # Get run details
    run_names = []
    difficulties = []
    total_turniness = 0

    for node in run_nodes:
        run_data = G.nodes[node]
        run_names.append(run_data.get('name', 'Unknown'))
        difficulties.append(run_data.get('difficulty', 'unknown'))
        total_turniness += run_data.get('turniness', 0)

    # Get difficulty metrics
    valid_difficulties = [d for d in difficulties if d not in [None, 'unknown']]
    difficulty_mix = ', '.join(sorted(set(valid_difficulties))) if valid_difficulties else 'unknown'

    # Calculate hardest difficulty
    difficulty_order = ['novice', 'easy', 'intermediate', 'advanced', 'expert', 'freeride']
    hardest_difficulty = 'unknown'

    if valid_difficulties:
        hardest_idx = -1
        for diff in valid_difficulties:
            if diff in difficulty_order:
                diff_idx = difficulty_order.index(diff)
                hardest_idx = max(hardest_idx, diff_idx)

        if hardest_idx >= 0:
            hardest_difficulty = difficulty_order[hardest_idx]

    # Record the path
    path_record = {
        'resort': G.nodes[path[0]].get('resort', 'Unknown'),
        'country_code': '',  # Add if available
        'path_id': f"{path_id}",
        'starting_lift_name': starting_lift_name,
        'starting_lift_osm_id': starting_lift_osm_id,
        'ending_point_type': ending_point_type,
        'ending_point_name': ending_point_name,
        'ending_point_osm_id': ending_point_osm_id,
        'run_path_array': [int(n.split('_')[1]) for n in run_nodes],
        'run_path_names': ' → '.join(run_names),
        'run_count': run_count,
        'total_path_length_m': round(total_distance, 1),
        'total_vertical_drop_m': round(total_elevation, 1),
        'avg_gradient': round(avg_gradient, 1),
        'total_turniness_score': round(total_turniness, 1),
        'difficulty_mix': difficulty_mix,
        'hardest_difficulty': hardest_difficulty
    }

    all_paths.append(path_record)