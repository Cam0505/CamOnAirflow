import pandas as pd
import networkx as nx

def model(dbt, session):
    """
    Analyze ski resort connectivity using new lift-run mappings and run-to-run connections.
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
            'run_to_run_connections': 0,
            'avg_path_length_m': 0,
            'total_network_length_m': 0,
            'error_message': 'No lift-run mapping data available'
        })
        return pd.DataFrame(results)

    # Process each resort
    for resort in lift_run_mapping['resort'].unique():
        resort_lift_run = lift_run_mapping[lift_run_mapping['resort'] == resort]
        resort_run_connections = run_to_run_connections[run_to_run_connections['resort'] == resort] if not run_to_run_connections.empty else pd.DataFrame()
        resort_runs = ski_runs[ski_runs['resort'] == resort] if not ski_runs.empty else pd.DataFrame()
        resort_lift_times = lift_times[lift_times['resort'] == resort] if not lift_times.empty else pd.DataFrame()

        # Build comprehensive network
        G = build_resort_network_analysis(resort_lift_run, resort_run_connections, resort_runs, resort_lift_times)

        # Calculate basic metrics
        lift_nodes = [n for n in G.nodes() if G.nodes[n]['node_type'] == 'lift']
        run_nodes = [n for n in G.nodes() if G.nodes[n]['node_type'] == 'run']

        total_lifts = len(lift_nodes)
        total_runs = len(run_nodes)
        total_connections = G.number_of_edges()

        # Calculate total times
        total_lift_time = sum(G.nodes[lift]['lift_time'] for lift in lift_nodes if 'lift_time' in G.nodes[lift])
        total_run_time_slow = sum(G.nodes[run]['ski_time_slow'] for run in run_nodes if 'ski_time_slow' in G.nodes[run])
        total_run_time_intermediate = sum(G.nodes[run]['ski_time_intermediate'] for run in run_nodes if 'ski_time_intermediate' in G.nodes[run])
        total_run_time_fast = sum(G.nodes[run]['ski_time_fast'] for run in run_nodes if 'ski_time_fast' in G.nodes[run])

        # Calculate ratios
        lift_to_ski_ratio_slow = total_lift_time / total_run_time_slow if total_run_time_slow > 0 else 0
        lift_to_ski_ratio_intermediate = total_lift_time / total_run_time_intermediate if total_run_time_intermediate > 0 else 0
        lift_to_ski_ratio_fast = total_lift_time / total_run_time_fast if total_run_time_fast > 0 else 0

        # Calculate network metrics
        run_to_run_connection_count = len(resort_run_connections) if not resort_run_connections.empty else 0

        # Calculate path metrics using simplified approach
        total_network_length = sum(G.nodes[run].get('length_m', 0) for run in run_nodes)

        # Find longest path (simplified to avoid performance issues)
        longest_path_time = 0
        if total_runs > 0:
            try:
                # Find longest single run as approximation
                longest_path_time = max(G.nodes[run].get('ski_time_slow', 0) for run in run_nodes)
            except Exception as e:
                print(f"Error finding longest path time: {e}")
                longest_path_time = 0

        # Calculate average path length
        avg_path_length = total_network_length / total_runs if total_runs > 0 else 0

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
            'run_to_run_connections': run_to_run_connection_count,
            'avg_path_length_m': avg_path_length,
            'total_network_length_m': total_network_length,
        })

    return pd.DataFrame(results)

def build_resort_network_analysis(lift_run_mapping: pd.DataFrame, run_connections: pd.DataFrame, 
                                 ski_runs: pd.DataFrame, lift_times: pd.DataFrame) -> 'nx.DiGraph':
    """Build network for analysis - same as ski_path_efficiency_analysis but separated for clarity."""

    G = nx.DiGraph()

    # Add lift nodes
    lifts_added = set()
    for _, row in lift_run_mapping.iterrows():
        lift_osm_id = row['lift_osm_id']
        lift_id = f"lift_{lift_osm_id}"

        if lift_id not in lifts_added:
            # Get lift time
            lift_time = None
            if not lift_times.empty:
                matching_lifts = lift_times[lift_times['osm_id'] == lift_osm_id]
                if not matching_lifts.empty:
                    lift_time = matching_lifts['lift_time_sec'].iloc[0]
                    if pd.isna(lift_time) or lift_time <= 0:
                        lift_time = None

            if lift_time is None:
                lift_length = row.get('lift_length_m', 0)
                lift_type = row.get('lift_type', '').lower()
                if lift_length > 0 and lift_type:
                    lift_time = calculate_proportional_lift_time_analysis(lift_length, lift_type)
                else:
                    continue

            G.add_node(lift_id, 
                      node_type='lift', 
                      osm_id=lift_osm_id,
                      name=row.get('lift_name', 'Unknown Lift'),
                      lift_time=lift_time)
            lifts_added.add(lift_id)

    # Add run nodes
    runs_added = set()
    for _, row in ski_runs.iterrows():
        run_osm_id = row['osm_id']
        run_id = f"run_{run_osm_id}"

        if run_id not in runs_added:
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
                          ski_time_fast=row.get('ski_time_fast_sec', 0))
                runs_added.add(run_id)

    # Add lift-run connections
    for _, row in lift_run_mapping.iterrows():
        lift_id = f"lift_{row['lift_osm_id']}"
        run_id = f"run_{row['run_osm_id']}"

        if lift_id in G and run_id in G:
            if row['connection_type'] == 'lift_services_run':
                G.add_edge(lift_id, run_id, connection_type='lift_to_run')
            elif row['connection_type'] == 'run_feeds_lift':
                G.add_edge(run_id, lift_id, connection_type='run_to_lift')

    # Add run-to-run connections
    if not run_connections.empty:
        for _, row in run_connections.iterrows():
            from_run_id = f"run_{row['from_run_osm_id']}"
            to_run_id = f"run_{row['to_run_osm_id']}"

            if from_run_id in G and to_run_id in G:
                G.add_edge(from_run_id, to_run_id, connection_type=row['connection_type'])

    return G

def calculate_proportional_lift_time_analysis(length_m: float, lift_type: str) -> float:
    """Calculate lift time for analysis."""
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