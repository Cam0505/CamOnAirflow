import pandas as pd
import numpy as np
from typing import Dict, List, Set
import networkx as nx

def model(dbt, session):
    """
    Find the longest possible ski paths from any lift top to any endpoint.
    This is for exploring maximum path possibilities, not efficiency.
    """

    # Load data with error handling
    try:
        lift_run_mapping = dbt.ref("base_lift_run_mapping").df()
        run_to_run_connections = dbt.ref("base_run_to_run_connections").df()
        ski_runs = dbt.ref("base_filtered_ski_runs").df()
    except Exception as e:
        print(f"Error loading data: {e}")
        return pd.DataFrame([{
            'country_code': 'ERROR',
            'resort': 'NO_DATA',
            'path_id': 'error',
            'starting_lift_name': 'Data loading failed',
            'starting_lift_osm_id': 0,
            'ending_point_type': 'unknown',
            'ending_point_name': 'unknown',
            'ending_point_osm_id': 0,
            'run_path_array': [],
            'run_path_names': '',
            'run_count': 0,
            'total_path_length_m': 0,
            'total_vertical_drop_m': 0,
            'avg_gradient': 0,
            'total_turniness_score': 0,
            'difficulty_mix': 'unknown',
            'hardest_difficulty': 'unknown',
            'error_message': f'Data loading failed: {e}'
        }])

    results = []

    if lift_run_mapping.empty:
        return pd.DataFrame(results)

    # Process each resort separately
    for resort in lift_run_mapping['resort'].unique():
        resort_lift_run = lift_run_mapping[lift_run_mapping['resort'] == resort]
        resort_run_connections = run_to_run_connections[run_to_run_connections['resort'] == resort] if not run_to_run_connections.empty else pd.DataFrame()
        resort_runs = ski_runs[ski_runs['resort'] == resort] if not ski_runs.empty else pd.DataFrame()

        try:
            # Build network using local functions
            G = build_resort_network(resort_lift_run, resort_run_connections, resort_runs)

            if G.number_of_nodes() == 0:
                continue

            # Find ALL paths (not filtered to same lift)
            paths = find_complete_ski_paths(G, resort)

            # Process each path
            for path_idx, path_data in enumerate(paths):
                path_id = f"{resort}_{path_data['start_lift_osm_id']}_{path_idx}"

                # Calculate path metrics
                path_metrics = calculate_path_metrics(path_data, G, resort_runs, resort)

                # Skip if no valid metrics
                if path_metrics['total_length'] <= 0:
                    continue

                # Get starting lift info
                start_lift_info = resort_lift_run[
                    (resort_lift_run['lift_osm_id'] == path_data['start_lift_osm_id']) &
                    (resort_lift_run['connection_type'] == 'lift_services_run')
                ]

                if start_lift_info.empty:
                    continue

                start_lift_info = start_lift_info.iloc[0]

                # Get country code
                country_code = start_lift_info.get('country_code') or (
                    resort_runs.iloc[0].get('country_code') if not resort_runs.empty else 'unknown'
                )

                results.append({
                    'country_code': country_code or 'unknown',
                    'resort': resort,
                    'path_id': path_id,
                    'starting_lift_name': start_lift_info.get('lift_name') or 'Unknown Lift',
                    'starting_lift_osm_id': path_data['start_lift_osm_id'],
                    'ending_point_type': path_data['ending_point_type'],
                    'ending_point_name': path_data['ending_point_name'],
                    'ending_point_osm_id': path_data['ending_point_osm_id'],
                    'run_path_array': path_data['run_osm_ids'],
                    'run_path_names': ', '.join(path_metrics['run_names']),
                    'run_count': len(path_data['run_osm_ids']),
                    'total_path_length_m': path_metrics['total_length'],
                    'total_vertical_drop_m': path_metrics['total_vertical_drop'],
                    'avg_gradient': path_metrics['avg_gradient'],
                    'total_turniness_score': path_metrics['total_turniness'],
                    'difficulty_mix': path_metrics['difficulty_mix'],
                    'hardest_difficulty': path_metrics['hardest_difficulty'],
                })

        except Exception as e:
            print(f"Error processing resort {resort}: {e}")
            continue

    return pd.DataFrame(results)

def build_resort_network(lift_run_mapping: pd.DataFrame, run_connections: pd.DataFrame, 
                        ski_runs: pd.DataFrame) -> 'nx.DiGraph':
    """Build a network graph for a resort."""
    G = nx.DiGraph()

    # Add lift nodes
    lifts_added = set()
    for _, row in lift_run_mapping.iterrows():
        lift_osm_id = row['lift_osm_id']
        lift_id = f"lift_{lift_osm_id}"

        if lift_id not in lifts_added:
            G.add_node(lift_id, 
                      node_type='lift', 
                      osm_id=lift_osm_id,
                      name=row.get('lift_name', 'Unknown Lift'))
            lifts_added.add(lift_id)

    # Add run nodes
    runs_added = set()
    for _, row in ski_runs.iterrows():
        run_osm_id = row['osm_id']
        run_id = f"run_{run_osm_id}"

        if run_id not in runs_added:
            G.add_node(run_id,
                      node_type='run',
                      osm_id=run_osm_id,
                      name=row.get('run_name', 'Unknown Run'),
                      difficulty=row.get('difficulty', 'unknown'),
                      length_m=row.get('run_length_m', 0),
                      turniness=row.get('turniness_score', 0),
                      vertical_drop=row.get('top_elevation_m', 0) - row.get('bottom_elevation_m', 0))
            runs_added.add(run_id)

    # Add lift-to-run connections with FIRST SEGMENT info
    for _, row in lift_run_mapping.iterrows():
        lift_id = f"lift_{row['lift_osm_id']}"
        run_id = f"run_{row['run_osm_id']}"

        if lift_id in G and run_id in G:
            if row['connection_type'] == 'lift_services_run':
                G.add_edge(lift_id, run_id, 
                          connection_type='lift_to_run',
                          # FIRST SEGMENT DATA
                          first_segment_length_m=row.get('segment_length_m', 0),
                          first_segment_vertical_drop_m=row.get('segment_vertical_drop_m', 0),
                          segment_ending_type=row.get('segment_ending_type', 'ends_at_base'))
            elif row['connection_type'] == 'run_feeds_lift':
                G.add_edge(run_id, lift_id, connection_type='run_to_lift')

    # Add run-to-run connections (for continuing after first segment)
    # Add run-to-run connections (for continuing after first segment)
    if not run_connections.empty:
        for _, row in run_connections.iterrows():
            from_run_id = f"run_{row['from_run_osm_id']}"
            to_run_id = f"run_{row['to_run_osm_id']}"

            if from_run_id in G and to_run_id in G:
                G.add_edge(from_run_id, to_run_id,
                        connection_type=row['connection_type'],
                        # Add skied distances - critical for calculating middle segments
                        skied_distance_from_run_m=row.get('skied_distance_from_run_m', 0),
                        skied_elevation_from_run_m=row.get('skied_elevation_from_run_m', 0),
                        # Keep remaining distances - needed for last segment
                        remaining_distance_to_run_m=row.get('remaining_distance_to_run_m', 0),
                        remaining_elevation_to_run_m=row.get('remaining_elevation_to_run_m', 0),
                        merge_point_progress=row.get('merge_point_progress', 0))

    return G

def find_complete_ski_paths(G: 'nx.DiGraph', resort: str) -> List[Dict]:
    """Find all complete ski paths from lift tops to ANY endpoint."""
    paths = []

    # Find lift nodes that service runs (start points)
    start_lifts = []
    for node in G.nodes():
        if G.nodes[node]['node_type'] == 'lift':
            successors = list(G.successors(node))
            if any(G.nodes[succ]['node_type'] == 'run' for succ in successors):
                start_lifts.append(node)

    # Find all paths from each starting lift
    for start_lift in start_lifts:
        start_lift_osm_id = G.nodes[start_lift]['osm_id']

        def dfs_paths(current_node: str, path: List[str], visited_runs: Set[str], depth: int = 0):
            if depth > 15:  # Prevent infinite recursion
                return

            successors = list(G.successors(current_node))

            # Filter out cycles
            valid_successors = []
            for successor in successors:
                if successor in visited_runs:
                    continue
                valid_successors.append(successor)

            # If no valid successors or reached a lift (terminal), record path
            if not valid_successors or any(G.nodes[succ]['node_type'] == 'lift' for succ in valid_successors):
                # Find the ending point
                ending_lift = None
                for succ in valid_successors:
                    if G.nodes[succ]['node_type'] == 'lift':
                        ending_lift = succ
                        break

                if ending_lift:
                    ending_point_type = 'lift'
                    ending_point_name = G.nodes[ending_lift]['name']
                    ending_point_osm_id = G.nodes[ending_lift]['osm_id']
                else:
                    # Path ends at a run
                    ending_point_type = 'run_end'
                    if current_node.startswith('run_'):
                        ending_point_name = G.nodes[current_node]['name']
                        ending_point_osm_id = G.nodes[current_node]['osm_id']
                    else:
                        ending_point_name = 'Unknown'
                        ending_point_osm_id = 0

                # Extract run OSM IDs from path
                run_osm_ids = []
                for node in path:
                    if node.startswith('run_'):
                        run_osm_ids.append(G.nodes[node]['osm_id'])

                if run_osm_ids:  # Only record paths with runs
                    paths.append({
                        'start_lift_osm_id': start_lift_osm_id,
                        'ending_point_type': ending_point_type,
                        'ending_point_name': ending_point_name,
                        'ending_point_osm_id': ending_point_osm_id,
                        'run_osm_ids': run_osm_ids,
                        'full_path': path.copy()
                    })
                return

            # Continue DFS
            for successor in valid_successors:
                new_visited_runs = visited_runs.copy()
                if G.nodes[successor]['node_type'] == 'run':
                    new_visited_runs.add(successor)

                path.append(successor)
                dfs_paths(successor, path, new_visited_runs, depth + 1)
                path.pop()

        # Start DFS from each lift
        dfs_paths(start_lift, [start_lift], set(), 0)

    return paths

def calculate_path_metrics(path_data: Dict, G: 'nx.DiGraph', 
                          ski_runs: pd.DataFrame, resort: str) -> Dict:
    """Calculate path metrics using SKIED distance for all but the final segment."""
    metrics = {
        'total_length': 0,
        'total_vertical_drop': 0,
        'total_turniness': 0,
        'run_names': [],
        'difficulties': [],
        'avg_gradient': 0,
        'difficulty_mix': 'unknown',
        'hardest_difficulty': 'unknown'
    }

    full_path = path_data['full_path']
    run_nodes = [node for node in full_path if node.startswith('run_')]

    # Process each run in the path
    for i, node in enumerate(run_nodes):
        # Get run details from graph
        run_data = G.nodes[node]

        # Check if this is the LAST run in the path
        is_last_run = i == len(run_nodes) - 1

        # Find the next run in the path (if any)
        next_run = run_nodes[i+1] if i < len(run_nodes) - 1 else None

        # DEFAULT: use full run length and drop (for standalone runs)
        skied_distance = run_data.get('length_m', 0)
        skied_elevation = run_data.get('vertical_drop', 0)

        # SPECIAL CASE: First run from lift may have custom length
        if i == 0:
            # Find lift predecessor
            for pred in G.predecessors(node):
                if pred.startswith('lift_') and pred in full_path:
                    edge_data = G.edges[pred, node]
                    if 'first_segment_length_m' in edge_data:
                        skied_distance = edge_data.get('first_segment_length_m', skied_distance)
                        skied_elevation = edge_data.get('first_segment_vertical_drop_m', skied_elevation)
                    break

        # CRITICAL LOGIC: For all except the last run, use SKIED distance, NOT remaining
        if not is_last_run and next_run:
            # Check if there's an edge from this run to the next
            if G.has_edge(node, next_run):
                edge_data = G.edges[node, next_run]

                # Use SKIED distance - how much of THIS run we actually ski
                if 'skied_distance_from_run_m' in edge_data and edge_data['skied_distance_from_run_m'] > 0:
                    skied_distance = edge_data['skied_distance_from_run_m']
                    skied_elevation = edge_data.get('skied_elevation_from_run_m', skied_elevation)

        # LAST RUN: use remaining distance (from where we entered to the end)
        elif is_last_run:
            # Find the previous run
            prev_run = run_nodes[i-1] if i > 0 else None

            if prev_run and G.has_edge(prev_run, node):
                # Get edge data from previous run to this one
                edge_data = G.edges[prev_run, node]

                # Use REMAINING distance - how much of the LAST run we have left
                if 'remaining_distance_to_run_m' in edge_data and edge_data['remaining_distance_to_run_m'] > 0:
                    skied_distance = edge_data['remaining_distance_to_run_m']
                    skied_elevation = edge_data.get('remaining_elevation_to_run_m', skied_elevation)

        # Add to totals
        metrics['total_length'] += skied_distance
        metrics['total_vertical_drop'] += skied_elevation

        # Calculate turniness proportionally
        total_run_length = run_data.get('length_m', 0)
        if total_run_length > 0:
            turniness_factor = skied_distance / total_run_length
            metrics['total_turniness'] += run_data.get('turniness', 0) * turniness_factor

        # Add run info with percentage indicator if partial run
        run_name = run_data.get('name', 'Unknown')
        if total_run_length > 0 and skied_distance < total_run_length * 0.95:
            percentage = (skied_distance / total_run_length) * 100
            run_name += f" ({percentage:.0f}%)"
        metrics['run_names'].append(run_name)

        difficulty = run_data.get('difficulty', 'unknown')
        metrics['difficulties'].append(difficulty)

    # Calculate averages
    if metrics['total_length'] > 0:
        metrics['avg_gradient'] = (metrics['total_vertical_drop'] / metrics['total_length']) * 100

    # Difficulty analysis
    valid_difficulties = [d for d in metrics['difficulties'] if d not in [None, 'unknown']]
    if valid_difficulties:
        unique_difficulties = list(set(valid_difficulties))
        metrics['difficulty_mix'] = ', '.join(sorted(unique_difficulties))

        difficulty_order = ['novice', 'easy', 'intermediate', 'advanced', 'expert', 'freeride']
        hardest_idx = -1
        for diff in valid_difficulties:
            if diff in difficulty_order:
                hardest_idx = max(hardest_idx, difficulty_order.index(diff))
        metrics['hardest_difficulty'] = difficulty_order[hardest_idx] if hardest_idx >= 0 else 'unknown'

    return metrics