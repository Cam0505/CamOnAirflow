import pandas as pd

def model(dbt, session):
    """
    Analyze ski resort connectivity and calculate total ski vs lift times.
    """
    try:
        import networkx as nx
    except ImportError:
        # Fallback if networkx is not available
        results = [{
            'resort': 'ERROR',
            'total_lifts': 0,
            'total_runs': 0,
            'total_connections': 0,
            'total_lift_time_sec': 0,
            'total_run_time_slow_sec': 0,
            'total_run_time_intermediate_sec': 0,
            'total_run_time_fast_sec': 0,
            'lift_to_ski_ratio_slow': 0,
            'lift_to_ski_ratio_intermediate': 0,
            'lift_to_ski_ratio_fast': 0,
            'avg_lift_time_sec': 0,
            'avg_run_time_slow_sec': 0,
            'avg_run_time_intermediate_sec': 0,
            'avg_run_time_fast_sec': 0,
            'longest_ski_path_time_sec': 0,
            'total_ski_area_time_slow_sec': 0,
            'total_ski_area_time_intermediate_sec': 0,
            'total_ski_area_time_fast_sec': 0,
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

    results = []

    # Check if we have any data
    if lift_run_mapping.empty:
        results.append({
            'resort': 'NO_DATA',
            'total_lifts': 0,
            'total_runs': 0,
            'total_connections': 0,
            'total_lift_time_sec': 0,
            'total_run_time_slow_sec': 0,
            'total_run_time_intermediate_sec': 0,
            'total_run_time_fast_sec': 0,
            'lift_to_ski_ratio_slow': 0,
            'lift_to_ski_ratio_intermediate': 0,
            'lift_to_ski_ratio_fast': 0,
            'avg_lift_time_sec': 0,
            'avg_run_time_slow_sec': 0,
            'avg_run_time_intermediate_sec': 0,
            'avg_run_time_fast_sec': 0,
            'longest_ski_path_time_sec': 0,
            'total_ski_area_time_slow_sec': 0,
            'total_ski_area_time_intermediate_sec': 0,
            'total_ski_area_time_fast_sec': 0,
            'error_message': 'No lift-run mapping data available'
        })
        return pd.DataFrame(results)

    # Process each resort
    for resort in lift_run_mapping['resort'].unique():
        resort_mapping = lift_run_mapping[lift_run_mapping['resort'] == resort]
        resort_ski_times = ski_times[ski_times['resort'] == resort] if not ski_times.empty else pd.DataFrame()
        resort_lift_times = lift_times[lift_times['resort'] == resort] if not lift_times.empty else pd.DataFrame()

        # Build graph for this resort
        G = nx.DiGraph()

        # Add lift nodes and run nodes
        lifts = {}
        runs = {}

        for _, row in resort_mapping.iterrows():
            lift_id = f"lift_{row['lift_osm_id']}"
            run_id = f"run_{row['run_osm_id']}"

            # Add lift info - ONLY use length-proportional calculations
            if lift_id not in lifts:
                # Get lift time from the base_ski_lift_times model
                matching_lifts = resort_lift_times[resort_lift_times['osm_id'] == row['lift_osm_id']] if not resort_lift_times.empty else pd.DataFrame()

                if len(matching_lifts) > 0:
                    lift_time = matching_lifts['lift_time_sec'].iloc[0]
                    # Only use the lift time if it's not null/nan
                    if pd.isna(lift_time) or lift_time is None or lift_time <= 0:
                        # Calculate proportional time if we have length and type
                        lift_length = row.get('lift_length_m', 0)
                        lift_type = row.get('lift_type', '').lower()
                        if lift_length > 0 and lift_type:
                            lift_time = calculate_proportional_lift_time(lift_length, lift_type)
                        else:
                            # Skip this lift if we can't calculate proportional time
                            continue
                else:
                    # Calculate proportional time if we have length and type
                    lift_length = row.get('lift_length_m', 0)
                    lift_type = row.get('lift_type', '').lower()
                    if lift_length > 0 and lift_type:
                        lift_time = calculate_proportional_lift_time(lift_length, lift_type)
                    else:
                        # Skip this lift if we can't calculate proportional time
                        continue

                lifts[lift_id] = {
                    'osm_id': row['lift_osm_id'],
                    'name': row.get('lift_name', 'Unknown Lift'),
                    'type': row.get('lift_type', 'unknown'),
                    'time_sec': lift_time,
                    'length_m': row.get('lift_length_m', 0)
                }
                G.add_node(lift_id, **lifts[lift_id], node_type='lift')

            # Add run info
            if run_id not in runs:
                # Safe lookup for run times
                matching_runs = resort_ski_times[resort_ski_times['osm_id'] == row['run_osm_id']] if not resort_ski_times.empty else pd.DataFrame()
                if len(matching_runs) > 0:
                    run_time_slow = matching_runs['ski_time_slow_sec'].iloc[0] if not pd.isna(matching_runs['ski_time_slow_sec'].iloc[0]) else None
                    run_time_intermediate = matching_runs['ski_time_intermediate_sec'].iloc[0] if not pd.isna(matching_runs['ski_time_intermediate_sec'].iloc[0]) else None
                    run_time_fast = matching_runs['ski_time_fast_sec'].iloc[0] if not pd.isna(matching_runs['ski_time_fast_sec'].iloc[0]) else None
                else:
                    run_time_slow = None
                    run_time_intermediate = None
                    run_time_fast = None

                # Only use runs that have valid ski times
                if run_time_slow is None or run_time_intermediate is None or run_time_fast is None:
                    continue

                runs[run_id] = {
                    'osm_id': row['run_osm_id'],
                    'name': row.get('run_name', 'Unknown Run'),
                    'difficulty': row.get('difficulty', 'unknown'),
                    'time_slow_sec': run_time_slow,
                    'time_intermediate_sec': run_time_intermediate,
                    'time_fast_sec': run_time_fast,
                    'length_m': row.get('run_length_m', 0)
                }
                G.add_node(run_id, **runs[run_id], node_type='run')

            # Add edges based on connection type
            if row.get('connection_type') == 'lift_services_run' and lift_id in lifts and run_id in runs:
                # Lift top connects to run top
                G.add_edge(lift_id, run_id, connection_type='lift_to_run', 
                          distance_m=row.get('min_connection_distance_m', 0))
            elif row.get('connection_type') == 'run_feeds_lift' and run_id in runs and lift_id in lifts:
                # Run bottom connects to lift bottom
                G.add_edge(run_id, lift_id, connection_type='run_to_lift', 
                          distance_m=row.get('min_connection_distance_m', 0))

        # Calculate metrics for this resort
        total_lifts = len([n for n in G.nodes() if G.nodes[n].get('node_type') == 'lift'])
        total_runs = len([n for n in G.nodes() if G.nodes[n].get('node_type') == 'run'])
        total_connections = G.number_of_edges()

        # Calculate total ski time vs lift time for different abilities
        total_lift_time = sum(lifts[lift_id]['time_sec'] for lift_id in lifts.keys())
        total_run_time_slow = sum(runs[run_id]['time_slow_sec'] for run_id in runs.keys())
        total_run_time_intermediate = sum(runs[run_id]['time_intermediate_sec'] for run_id in runs.keys())
        total_run_time_fast = sum(runs[run_id]['time_fast_sec'] for run_id in runs.keys())

        # Calculate run-to-lift ratios
        lift_to_ski_ratio_slow = total_lift_time / total_run_time_slow if total_run_time_slow > 0 else 0
        lift_to_ski_ratio_intermediate = total_lift_time / total_run_time_intermediate if total_run_time_intermediate > 0 else 0
        lift_to_ski_ratio_fast = total_lift_time / total_run_time_fast if total_run_time_fast > 0 else 0

        # Find longest possible runs (paths through the graph) - simplified to avoid timeouts
        longest_path_time = 0
        try:
            if len(lifts) > 0 and len(runs) > 0:
                # Just find the longest single run time as a proxy
                longest_path_time = max([runs[run_id]['time_slow_sec'] for run_id in runs.keys()]) if runs else 0
        except Exception:
            longest_path_time = 0

        results.append({
            'resort': resort,
            'total_lifts': total_lifts,
            'total_runs': total_runs,
            'total_connections': total_connections,
            'total_lift_time_sec': total_lift_time,
            'total_run_time_slow_sec': total_run_time_slow,
            'total_run_time_intermediate_sec': total_run_time_intermediate,
            'total_run_time_fast_sec': total_run_time_fast,
            'lift_to_ski_ratio_slow': lift_to_ski_ratio_slow,
            'lift_to_ski_ratio_intermediate': lift_to_ski_ratio_intermediate,
            'lift_to_ski_ratio_fast': lift_to_ski_ratio_fast,
            'avg_lift_time_sec': total_lift_time / total_lifts if total_lifts > 0 else 0,
            'avg_run_time_slow_sec': total_run_time_slow / total_runs if total_runs > 0 else 0,
            'avg_run_time_intermediate_sec': total_run_time_intermediate / total_runs if total_runs > 0 else 0,
            'avg_run_time_fast_sec': total_run_time_fast / total_runs if total_runs > 0 else 0,
            'longest_ski_path_time_sec': longest_path_time,
            'total_ski_area_time_slow_sec': total_lift_time + total_run_time_slow,
            'total_ski_area_time_intermediate_sec': total_lift_time + total_run_time_intermediate,
            'total_ski_area_time_fast_sec': total_lift_time + total_run_time_fast,
        })

    return pd.DataFrame(results)

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