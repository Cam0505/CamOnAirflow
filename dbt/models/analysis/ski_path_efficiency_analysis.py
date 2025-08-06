import pandas as pd
from typing import Dict, List, Set
import networkx as nx

def model(dbt, session):
    """
    Analyze ski path efficiency using lift-run mappings and run-to-run connections.
    Creates complete paths from lift tops to lift bases using the new connection logic.
    """

    # Load data with error handling
    try:
        lift_run_mapping = dbt.ref("base_lift_run_mapping").df()
    except Exception as e:
        print(f"Error loading lift_run_mapping: {e}")
        lift_run_mapping = pd.DataFrame()

    try:
        run_to_run_connections = dbt.ref("base_run_to_run_connections").df()
    except Exception as e:
        print(f"Error loading run_to_run_connections: {e}")
        run_to_run_connections = pd.DataFrame()

    try:
        ski_runs = dbt.ref("base_filtered_ski_runs").df()
    except Exception as e:
        print(f"Error loading ski_runs: {e}")
        ski_runs = pd.DataFrame()

    try:
        lift_times = dbt.ref("base_ski_lift_times").df()
    except Exception as e:
        print(f"Error loading lift_times: {e}")
        lift_times = pd.DataFrame()

    results = []

    # Check if we have required data
    if lift_run_mapping.empty:
        results.append({
            'country_code': 'ERROR',
            'resort': 'NO_DATA',
            'path_id': 'error',
            'starting_lift_name': 'No lift-run mapping data available',
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
            'path_ski_time_slow_sec': 0,
            'path_ski_time_intermediate_sec': 0,
            'path_ski_time_fast_sec': 0,
            'starting_lift_time_sec': 0,
            'lift_to_path_ratio_slow': 0,
            'lift_to_path_ratio_intermediate': 0,
            'lift_to_path_ratio_fast': 0,
            'total_experience_time_slow_sec': 0,
            'total_experience_time_intermediate_sec': 0,
            'total_experience_time_fast_sec': 0,
            'ski_time_percentage_slow': 0,
            'ski_time_percentage_intermediate': 0,
            'ski_time_percentage_fast': 0,
            'difficulty_mix': 'unknown',
            'hardest_difficulty': 'unknown',
            'efficiency_rating_slow': 'unknown',
            'efficiency_rating_intermediate': 'unknown',
            'efficiency_rating_fast': 'unknown',
            'error_message': 'Required data not available'
        })
        return pd.DataFrame(results)

    # Process each resort separately
    for resort in lift_run_mapping['resort'].unique():
        resort_lift_run = lift_run_mapping[lift_run_mapping['resort'] == resort]
        resort_run_connections = run_to_run_connections[run_to_run_connections['resort'] == resort] if not run_to_run_connections.empty else pd.DataFrame()
        resort_runs = ski_runs[ski_runs['resort'] == resort] if not ski_runs.empty else pd.DataFrame()
        resort_lift_times = lift_times[lift_times['resort'] == resort] if not lift_times.empty else pd.DataFrame()

        # Build comprehensive network for this resort
        try:
            G = build_resort_network(resort_lift_run, resort_run_connections, resort_runs, resort_lift_times)

            if G.number_of_nodes() == 0:
                continue

            # Find all paths from lift tops to lift bases
            paths = find_complete_ski_paths(G, resort)

            # Process each path
            for path_idx, path_data in enumerate(paths):
                path_id = f"{resort}_{path_data['start_lift_osm_id']}_{path_idx}"

                # Calculate path metrics
                path_metrics = calculate_comprehensive_path_metrics(
                    path_data, G, resort_runs, resort
                )

                # Skip if no valid metrics
                if path_metrics['total_ski_time_intermediate'] <= 0:
                    continue

                # Get starting lift info
                start_lift_info = resort_lift_run[
                    (resort_lift_run['lift_osm_id'] == path_data['start_lift_osm_id']) &
                    (resort_lift_run['connection_type'] == 'lift_services_run')
                ]

                if start_lift_info.empty:
                    continue

                start_lift_info = start_lift_info.iloc[0]

                # Get lift time
                lift_time = G.nodes[f"lift_{path_data['start_lift_osm_id']}"]['lift_time']

                # Calculate ratios and percentages
                lift_to_path_ratio_slow = lift_time / path_metrics['total_ski_time_slow'] if path_metrics['total_ski_time_slow'] > 0 else 0
                lift_to_path_ratio_intermediate = lift_time / path_metrics['total_ski_time_intermediate'] if path_metrics['total_ski_time_intermediate'] > 0 else 0
                lift_to_path_ratio_fast = lift_time / path_metrics['total_ski_time_fast'] if path_metrics['total_ski_time_fast'] > 0 else 0

                # Total experience times
                total_exp_slow = lift_time + path_metrics['total_ski_time_slow']
                total_exp_intermediate = lift_time + path_metrics['total_ski_time_intermediate']
                total_exp_fast = lift_time + path_metrics['total_ski_time_fast']

                # Ski time percentages
                ski_pct_slow = (path_metrics['total_ski_time_slow'] / total_exp_slow * 100) if total_exp_slow > 0 else 0
                ski_pct_intermediate = (path_metrics['total_ski_time_intermediate'] / total_exp_intermediate * 100) if total_exp_intermediate > 0 else 0
                ski_pct_fast = (path_metrics['total_ski_time_fast'] / total_exp_fast * 100) if total_exp_fast > 0 else 0

                # Efficiency ratings
                efficiency_slow = get_efficiency_rating(lift_to_path_ratio_slow)
                efficiency_intermediate = get_efficiency_rating(lift_to_path_ratio_intermediate)
                efficiency_fast = get_efficiency_rating(lift_to_path_ratio_fast)

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
                    'path_ski_time_slow_sec': path_metrics['total_ski_time_slow'],
                    'path_ski_time_intermediate_sec': path_metrics['total_ski_time_intermediate'],
                    'path_ski_time_fast_sec': path_metrics['total_ski_time_fast'],
                    'starting_lift_time_sec': lift_time,
                    'lift_to_path_ratio_slow': lift_to_path_ratio_slow,
                    'lift_to_path_ratio_intermediate': lift_to_path_ratio_intermediate,
                    'lift_to_path_ratio_fast': lift_to_path_ratio_fast,
                    'total_experience_time_slow_sec': total_exp_slow,
                    'total_experience_time_intermediate_sec': total_exp_intermediate,
                    'total_experience_time_fast_sec': total_exp_fast,
                    'ski_time_percentage_slow': ski_pct_slow,
                    'ski_time_percentage_intermediate': ski_pct_intermediate,
                    'ski_time_percentage_fast': ski_pct_fast,
                    'difficulty_mix': path_metrics['difficulty_mix'],
                    'hardest_difficulty': path_metrics['hardest_difficulty'],
                    'efficiency_rating_slow': efficiency_slow,
                    'efficiency_rating_intermediate': efficiency_intermediate,
                    'efficiency_rating_fast': efficiency_fast,
                })

        except Exception as e:
            print(f"Error processing resort {resort}: {e}")
            continue

    return pd.DataFrame(results)

def build_resort_network(lift_run_mapping: pd.DataFrame, run_connections: pd.DataFrame, 
                        ski_runs: pd.DataFrame, lift_times: pd.DataFrame) -> nx.DiGraph:
    """Build a comprehensive network graph for a resort using new connection logic."""


    G = nx.DiGraph()

    # Add lift nodes with times
    lifts_added = set()
    for _, row in lift_run_mapping.iterrows():
        lift_osm_id = row['lift_osm_id']
        lift_id = f"lift_{lift_osm_id}"

        if lift_id not in lifts_added:
            # Get lift time (prioritize proportional calculations)
            lift_time = None
            if not lift_times.empty:
                matching_lifts = lift_times[lift_times['osm_id'] == lift_osm_id]
                if not matching_lifts.empty:
                    lift_time = matching_lifts['lift_time_sec'].iloc[0]
                    # Only use if it's a valid calculation
                    if pd.isna(lift_time) or lift_time <= 0:
                        lift_time = None

            # Calculate proportional time if needed
            if lift_time is None:
                lift_length = row.get('lift_length_m', 0)
                lift_type = row.get('lift_type', '').lower()
                if lift_length > 0 and lift_type:
                    lift_time = calculate_proportional_lift_time(lift_length, lift_type)
                else:
                    continue  # Skip lifts without valid data

            G.add_node(lift_id, 
                      node_type='lift', 
                      osm_id=lift_osm_id,
                      name=row.get('lift_name', 'Unknown Lift'),
                      lift_time=lift_time)
            lifts_added.add(lift_id)

    # Add run nodes with times
    runs_added = set()
    for _, row in ski_runs.iterrows():
        run_osm_id = row['osm_id']
        run_id = f"run_{run_osm_id}"

        if run_id not in runs_added:
            # Only add runs with valid ski times
            if (pd.notna(row.get('ski_time_slow_sec')) and 
                pd.notna(row.get('ski_time_medium_sec')) and 
                pd.notna(row.get('ski_time_fast_sec')) and
                    row.get('ski_time_slow_sec', 0) > 0):

                G.add_node(run_id,
                          node_type='run',
                          osm_id=run_osm_id,
                          name=row.get('run_name', 'Unknown Run'),
                          difficulty=row.get('difficulty', 'unknown'),
                          length_m=row.get('run_length_m', 0),
                          ski_time_slow=row.get('ski_time_slow_sec', 0),
                          ski_time_intermediate=row.get('ski_time_medium_sec', 0),
                          ski_time_fast=row.get('ski_time_fast_sec', 0),
                          turniness=row.get('turniness_score', 0),
                          vertical_drop=row.get('top_elevation_m', 0) - row.get('bottom_elevation_m', 0))
                runs_added.add(run_id)

    # Add lift-run connections
    for _, row in lift_run_mapping.iterrows():
        lift_id = f"lift_{row['lift_osm_id']}"
        run_id = f"run_{row['run_osm_id']}"

        if lift_id in G and run_id in G:
            if row['connection_type'] == 'lift_services_run':
                G.add_edge(lift_id, run_id, 
                          connection_type='lift_to_run',
                          distance_m=row.get('min_connection_distance_m', 0))
            elif row['connection_type'] == 'run_feeds_lift':
                G.add_edge(run_id, lift_id,
                          connection_type='run_to_lift', 
                          distance_m=row.get('min_connection_distance_m', 0))

    # Add run-to-run connections with remaining distance tracking
    if not run_connections.empty:
        for _, row in run_connections.iterrows():
            from_run_id = f"run_{row['from_run_osm_id']}"
            to_run_id = f"run_{row['to_run_osm_id']}"

            if from_run_id in G and to_run_id in G:
                G.add_edge(from_run_id, to_run_id,
                          connection_type=row['connection_type'],
                          distance_m=row.get('connection_distance_m', 0),
                          remaining_run_distance_m=row.get('remaining_run_distance_m', 0),
                          merge_point_progress=row.get('merge_point_progress', 0))

    return G

def find_complete_ski_paths(G: 'nx.DiGraph', resort: str) -> List[Dict]:
    """Find ski paths from lift tops to SAME lift bottom for efficiency analysis."""
    paths = []

    # Find lift nodes that both service runs AND receive runs (complete lift cycles)
    complete_lifts = []
    for node in G.nodes():
        if G.nodes[node]['node_type'] == 'lift':
            lift_osm_id = G.nodes[node]['osm_id']

            # Check if this lift both services runs (has outgoing run edges) AND receives runs (has incoming run edges)
            outgoing_runs = [succ for succ in G.successors(node) if G.nodes[succ]['node_type'] == 'run']
            incoming_runs = [pred for pred in G.predecessors(node) if G.nodes[pred]['node_type'] == 'run']

            if outgoing_runs and incoming_runs:
                complete_lifts.append((node, lift_osm_id))

    # Find paths from each lift top back to the SAME lift bottom
    for start_lift, lift_osm_id in complete_lifts:

        # Use DFS to find paths that return to the same lift
        def dfs_lift_cycle_paths(current_node: str, path: List[str], visited_runs: Set[str], 
                                visited_progress: Dict[str, float], depth: int = 0):
            if depth > 15:  # Prevent infinite recursion
                return

            successors = list(G.successors(current_node))

            # Filter out invalid successors based on downhill constraint
            valid_successors = []
            for successor in successors:
                if successor in visited_runs:
                    continue  # Avoid cycles in runs

                # Check if we've reached our target lift
                if (successor == start_lift and 
                        len([n for n in path if n.startswith('run_')]) > 0):  # Must have at least one run
                    # Found a complete cycle back to the same lift!
                    run_osm_ids = []
                    for node in path:
                        if node.startswith('run_'):
                            run_osm_ids.append(G.nodes[node]['osm_id'])

                    if run_osm_ids:  # Only record paths with runs
                        paths.append({
                            'start_lift_osm_id': lift_osm_id,
                            'ending_point_type': 'lift',
                            'ending_point_name': G.nodes[start_lift]['name'],
                            'ending_point_osm_id': lift_osm_id,
                            'run_osm_ids': run_osm_ids,
                            'full_path': path.copy() + [successor]  # Include the return to lift
                        })
                    return

                # Check downhill constraint for run-to-run connections
                if (G.nodes[current_node]['node_type'] == 'run' and 
                        G.nodes[successor]['node_type'] == 'run'):

                    edge_data = G.edges[current_node, successor]
                    merge_progress = edge_data.get('merge_point_progress', 0)

                    # If we've already visited this run at a later point, skip
                    if successor in visited_progress:
                        if visited_progress[successor] > merge_progress:
                            continue  # Would be going uphill

                valid_successors.append(successor)

            # Continue DFS for each valid successor (except if it's the target lift, which we handled above)
            for successor in valid_successors:
                if successor == start_lift:
                    continue  # Already handled above

                new_visited_runs = visited_runs.copy()
                new_visited_progress = visited_progress.copy()

                if G.nodes[successor]['node_type'] == 'run':
                    new_visited_runs.add(successor)
                    # Track progress for run-to-run connections
                    if current_node.startswith('run_'):
                        edge_data = G.edges[current_node, successor]
                        merge_progress = edge_data.get('merge_point_progress', 0)
                        new_visited_progress[successor] = merge_progress

                path.append(successor)
                dfs_lift_cycle_paths(successor, path, new_visited_runs, new_visited_progress, depth + 1)
                path.pop()

        # Start DFS from each complete lift
        dfs_lift_cycle_paths(start_lift, [start_lift], set(), {}, 0)

    return paths

def calculate_comprehensive_path_metrics(path_data: Dict, G: 'nx.DiGraph', 
                                       ski_runs: pd.DataFrame, resort: str) -> Dict:
    """Calculate comprehensive metrics for a ski path including partial run distances."""
    metrics = {
        'total_length': 0,
        'total_vertical_drop': 0,
        'total_turniness': 0,
        'total_ski_time_slow': 0,
        'total_ski_time_intermediate': 0,
        'total_ski_time_fast': 0,
        'run_names': [],
        'difficulties': [],
        'avg_gradient': 0,
        'difficulty_mix': 'unknown',
        'hardest_difficulty': 'unknown'
    }

    full_path = path_data['full_path']

    # Process each run in the path
    for i, node in enumerate(full_path):
        if not node.startswith('run_'):
            continue

        # Get run details from graph
        run_data = G.nodes[node]

        # Calculate distance factor based on remaining distance
        distance_factor = 1.0  # Default to full run

        # Check if this run is connected from another run
        predecessors = [pred for pred in G.predecessors(node) if pred.startswith('run_')]
        if predecessors:
            # Find the edge that brought us to this run
            for pred in predecessors:
                if pred in full_path[:i]:  # Make sure it's actually the path we took
                    edge_data = G.edges[pred, node]
                    remaining_distance = edge_data.get('remaining_run_distance_m', run_data.get('length_m', 0))
                    total_distance = run_data.get('length_m', 0)
                    if total_distance > 0:
                        distance_factor = remaining_distance / total_distance
                    break

        # Apply distance factor to metrics
        run_length = run_data.get('length_m', 0) * distance_factor
        metrics['total_length'] += run_length
        metrics['total_turniness'] += run_data.get('turniness', 0) * distance_factor
        metrics['total_vertical_drop'] += run_data.get('vertical_drop', 0) * distance_factor

        # Apply distance factor to ski times
        metrics['total_ski_time_slow'] += run_data.get('ski_time_slow', 0) * distance_factor
        metrics['total_ski_time_intermediate'] += run_data.get('ski_time_intermediate', 0) * distance_factor
        metrics['total_ski_time_fast'] += run_data.get('ski_time_fast', 0) * distance_factor

        # Add run info
        metrics['run_names'].append(run_data.get('name', 'Unknown'))
        difficulty = run_data.get('difficulty', 'unknown')
        metrics['difficulties'].append(difficulty)

    # Calculate averages and summaries
    if metrics['total_length'] > 0:
        metrics['avg_gradient'] = (metrics['total_vertical_drop'] / metrics['total_length']) * 100

    # Difficulty analysis
    valid_difficulties = [d for d in metrics['difficulties'] if d not in [None, 'unknown']]
    if valid_difficulties:
        unique_difficulties = list(set(valid_difficulties))
        metrics['difficulty_mix'] = ', '.join(sorted(unique_difficulties))

        # Hardest difficulty
        difficulty_order = ['novice', 'easy', 'intermediate', 'advanced', 'expert', 'freeride']
        hardest_idx = -1
        for diff in valid_difficulties:
            if diff in difficulty_order:
                hardest_idx = max(hardest_idx, difficulty_order.index(diff))
        metrics['hardest_difficulty'] = difficulty_order[hardest_idx] if hardest_idx >= 0 else 'unknown'

    return metrics

def calculate_proportional_lift_time(length_m: float, lift_type: str) -> float:
    """Calculate lift time based on length and type (proportional only)."""
    speed_mapping = {
        'gondola': 5.0,
        'chair_lift': 2.5,
        'yes': 2.5,
        'j-bar': 3.0,
        'drag_lift': 3.0,
        'platter': 3.0,
        't-bar': 3.0,
        'rope_tow': 3.0,
        'magic_carpet': 1.5,
        'mixed_lift': 3.5,
        'station': 4.0,
        'goods': 4.0
    }

    speed = speed_mapping.get(lift_type, 2.5)
    return length_m / speed

def get_efficiency_rating(ratio: float) -> str:
    """Get efficiency rating based on lift-to-ski ratio."""
    if ratio is None or pd.isna(ratio):
        return 'unknown'

    if ratio <= 0.5:
        return 'excellent'
    elif ratio <= 0.8:
        return 'very_good'
    elif ratio <= 1.2:
        return 'good'
    elif ratio <= 1.8:
        return 'fair'
    else:
        return 'poor'