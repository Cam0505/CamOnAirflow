import pandas as pd
import numpy as np
from typing import Dict, List

def model(dbt, session):
    """
    Analyze ski path efficiency - complete paths from lift top to base/lift bottom.
    Returns paths as arrays of runs with total metrics.
    """

    try:
        import networkx as nx
    except ImportError:
        # Return error if NetworkX not available
        results = [{
            'country_code': 'ERROR',
            'resort': 'NO_NETWORKX',
            'path_id': 'error',
            'starting_lift_name': 'NetworkX not installed',
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
            'error_message': 'NetworkX not installed. Run: pip3 install networkx'
        }]
        return pd.DataFrame(results)

    # Load data with error handling
    try:
        lift_run_mapping = dbt.ref("base_lift_run_mapping").df()
    except Exception as e:
        print(f"Error loading lift_run_mapping: {e}")
        lift_run_mapping = pd.DataFrame()

    try:
        ski_times = dbt.ref("staging_ski_time_estimate").df()
    except Exception as e:
        print(f"Error loading ski_times: {e}")
        ski_times = pd.DataFrame()

    try:
        lift_times = dbt.ref("base_ski_lift_times").df()
    except Exception as e:
        print(f"Error loading lift_times: {e}")
        lift_times = pd.DataFrame()

    try:
        ski_runs = dbt.ref("base_filtered_ski_runs").df()
    except Exception as e:
        print(f"Error loading ski_runs: {e}")
        ski_runs = pd.DataFrame()

    results = []

    # Check if we have any data
    if lift_run_mapping.empty or ski_times.empty:
        results.append({
            'country_code': 'ERROR',
            'resort': 'NO_DATA',
            'path_id': 'error',
            'starting_lift_name': 'No data available',
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
        resort_mapping = lift_run_mapping[lift_run_mapping['resort'] == resort]
        resort_ski_times = ski_times[ski_times['resort'] == resort] if not ski_times.empty else pd.DataFrame()
        resort_lift_times = lift_times[lift_times['resort'] == resort] if not lift_times.empty else pd.DataFrame()
        resort_runs = ski_runs[ski_runs['resort'] == resort] if not ski_runs.empty else pd.DataFrame()

        # Build graph for this resort
        G = nx.DiGraph()

        # Add nodes and edges - only include lifts with valid proportional times
        valid_lifts = set()
        valid_runs = set()

        for _, row in resort_mapping.iterrows():
            lift_id = f"lift_{row['lift_osm_id']}"
            run_id = f"run_{row['run_osm_id']}"

            # Add lift node only if we have proportional time calculation
            if not G.has_node(lift_id):
                # Get lift time from base_ski_lift_times model
                matching_lifts = resort_lift_times[resort_lift_times['osm_id'] == row['lift_osm_id']] if not resort_lift_times.empty else pd.DataFrame()

                lift_time = None
                if len(matching_lifts) > 0:
                    lift_time = matching_lifts['lift_time_sec'].iloc[0]
                    # Only use if it's a valid proportional calculation
                    calculation_method = matching_lifts['time_calculation_method'].iloc[0] if 'time_calculation_method' in matching_lifts.columns else None
                    if pd.isna(lift_time) or lift_time <= 0 or calculation_method in ['type_default', 'fallback_default']:
                        lift_time = None

                # If no valid lift time from model, calculate proportionally
                if lift_time is None:
                    lift_length = row.get('lift_length_m', 0)
                    lift_type = row.get('lift_type', '').lower()
                    if lift_length > 0 and lift_type:
                        lift_time = calculate_proportional_lift_time(lift_length, lift_type)
                    else:
                        continue  # Skip lifts without proportional data

                G.add_node(lift_id, node_type='lift', osm_id=row['lift_osm_id'], lift_time=lift_time)
                valid_lifts.add(lift_id)

            # Add run node only if we have valid ski times
            if not G.has_node(run_id):
                matching_runs = resort_ski_times[resort_ski_times['osm_id'] == row['run_osm_id']] if not resort_ski_times.empty else pd.DataFrame()

                if len(matching_runs) > 0:
                    run_times = matching_runs.iloc[0]
                    if (not pd.isna(run_times.get('ski_time_slow_sec')) and 
                        not pd.isna(run_times.get('ski_time_intermediate_sec')) and 
                        not pd.isna(run_times.get('ski_time_fast_sec')) and
                            run_times.get('ski_time_slow_sec', 0) > 0):

                        G.add_node(run_id, node_type='run', osm_id=row['run_osm_id'])
                        valid_runs.add(run_id)
                    else:
                        continue  # Skip runs without valid times
                else:
                    continue  # Skip runs without data

            # Add edges only if both nodes are valid
            if lift_id in valid_lifts and run_id in valid_runs:
                if row['connection_type'] == 'lift_services_run':
                    G.add_edge(lift_id, run_id, connection_type='lift_to_run')
                elif row['connection_type'] == 'run_feeds_lift':
                    G.add_edge(run_id, lift_id, connection_type='run_to_lift')

        # Find all paths from lifts to terminal points (lifts or dead ends)
        lift_nodes = [n for n in G.nodes() if G.nodes[n]['node_type'] == 'lift']

        for start_lift in lift_nodes:
            # Find all possible paths from this lift
            paths = find_ski_paths_from_lift(G, start_lift, resort_mapping, max_depth=10)

            for path_idx, path in enumerate(paths):
                path_id = f"{resort}_{start_lift}_{path_idx}"

                # Extract run information from path
                run_nodes = [node for node in path if node.startswith('run_')]
                if not run_nodes:
                    continue

                # Get starting lift info
                start_lift_osm_id = G.nodes[start_lift]['osm_id']
                start_lift_info = resort_mapping[resort_mapping['lift_osm_id'] == start_lift_osm_id]
                if start_lift_info.empty:
                    continue
                start_lift_info = start_lift_info.iloc[0]
                start_lift_name = start_lift_info.get('lift_name', 'Unknown Lift') or 'Unknown Lift'

                # Get lift time (guaranteed to be proportional)
                lift_time = G.nodes[start_lift]['lift_time']

                # Determine ending point
                end_node = path[-1]
                if end_node.startswith('lift_'):
                    ending_point_type = 'lift'
                    end_osm_id = G.nodes[end_node]['osm_id']
                    end_info = resort_mapping[resort_mapping['lift_osm_id'] == end_osm_id]
                    ending_point_name = (end_info.iloc[0].get('lift_name') or 'Unknown Lift') if not end_info.empty else 'Unknown Lift'
                    ending_point_osm_id = end_osm_id
                else:
                    ending_point_type = 'run_end'
                    end_osm_id = G.nodes[end_node]['osm_id']
                    end_info = resort_runs[resort_runs['osm_id'] == end_osm_id]
                    ending_point_name = (end_info.iloc[0].get('run_name') or 'Unknown Run') if not end_info.empty else 'Unknown Run'
                    ending_point_osm_id = end_osm_id

                # Calculate path metrics
                path_metrics = calculate_path_metrics(run_nodes, G, resort_runs, resort_ski_times, resort)

                # Skip if no valid metrics
                if path_metrics['total_ski_time_slow'] <= 0:
                    continue

                # Calculate ratios
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
                country_code = (start_lift_info.get('country_code') or 
                               (resort_runs.iloc[0].get('country_code') if not resort_runs.empty else 'unknown'))

                results.append({
                    'country_code': country_code or 'unknown',
                    'resort': resort,
                    'path_id': path_id,
                    'starting_lift_name': start_lift_name,
                    'starting_lift_osm_id': start_lift_osm_id,
                    'ending_point_type': ending_point_type,
                    'ending_point_name': ending_point_name,
                    'ending_point_osm_id': ending_point_osm_id,
                    'run_path_array': [G.nodes[node]['osm_id'] for node in run_nodes],
                    'run_path_names': ', '.join(path_metrics['run_names']),
                    'run_count': len(run_nodes),
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

    return pd.DataFrame(results)

def find_ski_paths_from_lift(G, start_lift: str, resort_mapping: pd.DataFrame, max_depth: int = 10) -> List[List[str]]:
    """Find all possible ski paths from a lift to terminal points."""
    paths = []

    def dfs_paths(current_node: str, path: List[str], depth: int):
        if depth > max_depth:
            return

        successors = list(G.successors(current_node))
        if not successors or all(node in path for node in successors):  # Terminal or cycle
            if len(path) > 1:  # Must have at least lift + run
                paths.append(path.copy())
            return

        for next_node in successors:
            if next_node not in path:  # Avoid cycles
                path.append(next_node)
                dfs_paths(next_node, path, depth + 1)
                path.pop()

    dfs_paths(start_lift, [start_lift], 0)
    return paths

def calculate_path_metrics(run_nodes: List[str], G, resort_runs: pd.DataFrame, 
                          resort_ski_times: pd.DataFrame, resort: str) -> Dict:
    """Calculate aggregated metrics for a path of runs."""
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

    gradients = []

    for run_node in run_nodes:
        run_osm_id = G.nodes[run_node]['osm_id']

        # Get run details
        run_detail = resort_runs[resort_runs['osm_id'] == run_osm_id]
        if not run_detail.empty:
            run_info = run_detail.iloc[0]
            metrics['total_length'] += run_info.get('run_length_m') or 0
            metrics['total_turniness'] += run_info.get('turniness_score') or 0
            metrics['run_names'].append(run_info.get('run_name') or 'Unknown')
            difficulty = run_info.get('difficulty') or 'unknown'
            metrics['difficulties'].append(difficulty)
        else:
            metrics['run_names'].append('Unknown')
            metrics['difficulties'].append('unknown')

        # Get ski times
        ski_time_info = resort_ski_times[resort_ski_times['osm_id'] == run_osm_id]
        if not ski_time_info.empty:
            time_info = ski_time_info.iloc[0]
            metrics['total_ski_time_slow'] += time_info.get('ski_time_slow_sec') or 0
            metrics['total_ski_time_intermediate'] += time_info.get('ski_time_intermediate_sec') or 0
            metrics['total_ski_time_fast'] += time_info.get('ski_time_fast_sec') or 0
            gradient = time_info.get('avg_gradient')
            if gradient is not None:
                gradients.append(gradient)

    # Calculate averages and summaries
    if gradients:
        metrics['avg_gradient'] = np.mean(gradients)

    # Difficulty analysis - filter out None values
    valid_difficulties = [d for d in metrics['difficulties'] if d is not None and d != 'unknown']
    if valid_difficulties:
        unique_difficulties = list(set(valid_difficulties))
        metrics['difficulty_mix'] = ', '.join(sorted(unique_difficulties))

        # Hardest difficulty (ordered by difficulty)
        difficulty_order = ['novice', 'easy', 'intermediate', 'advanced', 'freeride']
        hardest_idx = -1
        for diff in valid_difficulties:
            if diff in difficulty_order:
                hardest_idx = max(hardest_idx, difficulty_order.index(diff))
        metrics['hardest_difficulty'] = difficulty_order[hardest_idx] if hardest_idx >= 0 else 'unknown'

    return metrics

def calculate_proportional_lift_time(length_m: float, lift_type: str) -> float:
    """Calculate lift time based on length and type (proportional only)."""
    # Speed mapping in m/s based on lift type
    speed_mapping = {
        'gondola': 5.0,
        'chair_lift': 2.5,
        'yes': 2.5,  # Generic lift, assume chairlift
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

    speed = speed_mapping.get(lift_type, 2.5)  # Default to chairlift speed
    return length_m / speed

def get_efficiency_rating(ratio: float) -> str:
    """Get efficiency rating based on lift-to-ski ratio."""
    if ratio is None or pd.isna(ratio):
        return 'unknown'

    if ratio <= 0.5:
        return 'excellent'  # Less than 30 sec lift per 60 sec ski
    elif ratio <= 0.8:
        return 'very_good'  # 30-48 sec lift per 60 sec ski
    elif ratio <= 1.2:
        return 'good'  # 48-72 sec lift per 60 sec ski
    elif ratio <= 1.8:
        return 'fair'  # 72-108 sec lift per 60 sec ski
    else:
        return 'poor'  # More than 108 sec lift per 60 sec ski