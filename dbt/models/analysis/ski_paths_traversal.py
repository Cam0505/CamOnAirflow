"""
Find all possible ski paths using tree traversal with segment-based connections
"""
import pandas as pd
import networkx as nx
from typing import Dict, List, Tuple, Set, Optional
import numpy as np
import logging

def model(dbt, session):
    """
    Build a network of all possible ski paths from lift tops to endpoints.
    Uses node IDs for precise connections between ski runs and segments.
    """
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s'
    )
    logger = logging.getLogger('ski_paths')
    
    # Load data
    print("Loading data...")
    lift_run_mapping = dbt.ref("base_lift_run_mapping").df()
    ski_runs = dbt.ref("base_filtered_ski_runs").df()
    ski_points = dbt.ref("base_filtered_ski_points").df()
    ski_segments = dbt.ref("base_filtered_ski_segments").df()

    # CAPTAINS EXPRESS FILTER - Process only Captain's Express for debugging
    resort_filter = "Cardrona Alpine Resort"
    lift_filter = "Captain's Express"
    print(f"FOCUSED TESTING: Only processing {lift_filter} at {resort_filter}")
    
    # Filter to only Cardrona
    resorts = [resort_filter]
    print(f"Processing {len(resorts)} resort")

    all_paths = []
    for resort in resorts:
        print(f"\nProcessing resort: {resort}")

        # Filter data for this resort
        resort_lifts = lift_run_mapping[lift_run_mapping['resort'] == resort]
        resort_runs = ski_runs[ski_runs['resort'] == resort]
        resort_points = ski_points[ski_points['resort'] == resort]
        resort_segments = ski_segments[ski_segments['resort'] == resort]

        print(f"  Found {len(resort_lifts)} lift-run connections")
        print(f"  Found {len(resort_runs)} ski runs")
        print(f"  Found {len(resort_points)} ski points")
        print(f"  Found {len(resort_segments)} ski segments")

        # Build the network
        print("  Building network...")
        G = build_resort_network(resort_lifts, resort_runs, resort_points, resort_segments, lift_filter)

        print(f"  Network built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
        
        # Find all paths
        print("  Finding all paths...")
        resort_paths = find_all_paths(G, resort, lift_filter)

        print(f"  Found {len(resort_paths)} paths for {resort}")
        all_paths.extend(resort_paths)

    # Convert to DataFrame
    print("\nConverting all paths to DataFrame...")
    if not all_paths:
        print("No paths found!")
        paths_df = pd.DataFrame(columns=[
            'resort', 'country_code', 'path_id', 'starting_lift_name', 'starting_lift_osm_id',
            'ending_point_type', 'ending_point_name', 'ending_point_osm_id', 'run_path_array',
            'run_path_names', 'run_count', 'total_path_length_m', 'total_vertical_drop_m',
            'avg_gradient', 'total_turniness_score', 'difficulty_mix', 'hardest_difficulty',
            'ending_category'
        ])
    else:
        paths_df = pd.DataFrame(all_paths)
        
        # Add ending_category field
        paths_df['ending_category'] = 'Non-Lift Ending'  # Default
        
        # Paths ending at the same lift they started from
        same_lift = (paths_df['ending_point_type'] == 'lift') & (paths_df['ending_point_osm_id'] == paths_df['starting_lift_osm_id'])
        paths_df.loc[same_lift, 'ending_category'] = 'Same Lift'
        
        # Paths ending at a different lift
        different_lift = (paths_df['ending_point_type'] == 'lift') & (paths_df['ending_point_osm_id'] != paths_df['starting_lift_osm_id'])
        paths_df.loc[different_lift, 'ending_category'] = 'Different Lift'
        
        print(f"Final dataset: {len(paths_df)} paths across {paths_df['resort'].nunique()} resorts")
        print(f"Ending categories: {paths_df['ending_category'].value_counts().to_dict()}")
        
        # Print some path statistics for Cardrona
        if resort_filter in paths_df['resort'].values:
            cardrona_paths = paths_df[paths_df['resort'] == resort_filter]
            print(f"\nCardrona paths by starting lift:")
            for lift, count in cardrona_paths['starting_lift_name'].value_counts().items():
                print(f"  {lift}: {count} paths")
                
            print(f"\nCardrona paths by ending category:")
            for cat, count in cardrona_paths['ending_category'].value_counts().items():
                print(f"  {cat}: {count} paths")
                
            # Check for Captain's Express specifically
            captains_paths = cardrona_paths[cardrona_paths['starting_lift_name'].str.contains("Captain")]
            if not captains_paths.empty:
                print(f"\nCaptain's Express paths: {len(captains_paths)}")
                print(f"  Ending categories: {captains_paths['ending_category'].value_counts().to_dict()}")
                print(f"  Longest path: {captains_paths['total_path_length_m'].max():.1f}m")
                print(f"  Most runs: {captains_paths['run_count'].max()} runs")

    return paths_df

def build_resort_network(
    lift_run_mapping: pd.DataFrame, 
    ski_runs: pd.DataFrame,
    ski_points: pd.DataFrame,
    ski_segments: pd.DataFrame,
    lift_filter: str = None
) -> nx.DiGraph:
    """
    Build a directed graph network of ski runs, lifts, and connections for a resort.
    Uses segment node IDs for precise connections between runs.
    """
    G = nx.DiGraph()

    # Create dictionaries for fast lookups
    run_dict = {row['osm_id']: row for _, row in ski_runs.iterrows()}
    
    # Store points by run with sorting by point_index for downhill traversal
    points_by_run = {}
    for _, row in ski_points.iterrows():
        osm_id = row['osm_id']
        if osm_id not in points_by_run:
            points_by_run[osm_id] = []
        points_by_run[osm_id].append(row)
    
    # Sort points by point_index to ensure downhill traversal
    for run_id, points in points_by_run.items():
        points_by_run[run_id] = sorted(points, key=lambda p: p['point_index'])
    
    # Create a dictionary for node_id to run_osm_id mapping
    node_to_run = {}
    for _, row in ski_points.iterrows():
        if pd.notna(row['node_id']):
            node_to_run[row['node_id']] = row['osm_id']
    
    # DEBUG - Print all lifts to verify Captain's Express exists
    if lift_filter:
        print("\nDEBUG: Available lifts in data:")
        for _, row in lift_run_mapping.drop_duplicates('lift_osm_id').iterrows():
            print(f"  Lift: '{row['lift_name']}' (ID: {row['lift_osm_id']})")
        
        # Also debug lift-run connections
        captain_connections = lift_run_mapping[
            (lift_run_mapping['lift_name'].str.lower().str.contains('captain')) & 
            (lift_run_mapping['connection_type'] == 'lift_services_run')
        ]
        print("\nDEBUG: Captain's Express connections:")
        for _, row in captain_connections.iterrows():
            print(f"  Connection: lift_id={row['lift_osm_id']}, run_id={row['run_osm_id']}, type={row['connection_type']}")
    
    # Add lifts
    lift_count = 0
    lift_ids_by_name = {}  # Store lift IDs by name for lookup
    
    for _, row in lift_run_mapping.drop_duplicates('lift_osm_id').iterrows():
        lift_id = f"lift_{row['lift_osm_id']}"
        lift_name = row['lift_name']
        
        # Skip if we're filtering and this isn't Captain's Express
        if lift_filter and lift_filter.lower() not in lift_name.lower():
            continue
            
        if lift_id not in G:
            lift_count += 1
            G.add_node(lift_id, 
                      node_type='lift',
                      resort=row['resort'],
                      osm_id=row['lift_osm_id'],
                      name=row['lift_name'],
                      lift_type=row['lift_type'])
            
            # Store lift ID by name for easy lookup
            lift_ids_by_name[lift_name.lower()] = lift_id
    
    # Add runs
    run_count = 0
    for _, row in ski_runs.iterrows():
        run_id = f"run_{row['osm_id']}"
        
        if run_id not in G:
            run_count += 1
            # Calculate vertical drop
            vertical_drop = row.get('top_elevation_m', 0) - row.get('bottom_elevation_m', 0)
            
            G.add_node(run_id,
                      node_type='run',
                      resort=row['resort'],
                      osm_id=row['osm_id'],
                      name=row.get('run_name', 'Unknown'),
                      difficulty=row.get('difficulty', 'unknown'),
                      length_m=row.get('run_length_m', 0),
                      vertical_drop=vertical_drop,
                      turniness=row.get('turniness_score', 0))
    
    # CRITICAL FIX - Get Captain's Express lift ID from our lookup
    captain_lift_id = None
    captain_osm_id = None
    
    if lift_filter:
        # First try exact lookup
        captain_lift_id = lift_ids_by_name.get(lift_filter.lower())
        
        # If not found, try fuzzy lookup
        if not captain_lift_id:
            for name, node_id in lift_ids_by_name.items():
                if lift_filter.lower() in name:
                    captain_lift_id = node_id
                    captain_osm_id = int(node_id.split('_')[1])
                    break
        else:
            captain_osm_id = int(captain_lift_id.split('_')[1])
            
        if captain_lift_id:
            print(f"\nFound Captain's Express lift ID: {captain_lift_id}, OSM ID: {captain_osm_id}")
        else:
            print(f"\n⚠️ WARNING: Could not find Captain's Express lift in network!")
    
    # Add lift-to-run connections (lift services run)
    lift_run_count = 0
    
    # IMPORTANT: Print all lift-services-run connections for debugging
    print("\nDEBUG: All lift_services_run connections in data:")
    for _, row in lift_run_mapping[lift_run_mapping['connection_type'] == 'lift_services_run'].iterrows():
        print(f"  Connection: {row['lift_name']} → {row['run_name']} (IDs: {row['lift_osm_id']} → {row['run_osm_id']})")
    
    for _, row in lift_run_mapping[lift_run_mapping['connection_type'] == 'lift_services_run'].iterrows():
        lift_id = f"lift_{row['lift_osm_id']}"
        run_id = f"run_{row['run_osm_id']}"
        
        # If we're filtering for Captain's Express, only add its connections
        if lift_filter:
            # FIXED LOGIC: Check if this row is for Captain's Express
            if captain_osm_id and row['lift_osm_id'] != captain_osm_id:
                continue
                
            print(f"  Adding Captain's connection: {row['lift_name']} → {row['run_name']}")
        
        # Skip if either endpoint doesn't exist in the graph
        if lift_id not in G or run_id not in G:
            print(f"  ⚠️ Skipping connection - missing endpoint: {lift_id} → {run_id}")
            continue
            
        lift_run_count += 1
        G.add_edge(lift_id, run_id,
                  connection_type='lift_services_run',
                  entry_point_index=row.get('point_index', 0),
                  entry_distance_m=row.get('distance_m', 0))
    
    # Add run-to-lift connections (run feeds lift)
    run_lift_count = 0
    
    # First get all runs connected to Captain's if filtering
    captain_runs = set()
    if lift_filter and captain_lift_id:
        for _, row in lift_run_mapping[lift_run_mapping['lift_osm_id'] == G.nodes[captain_lift_id]['osm_id']].iterrows():
            captain_runs.add(row['run_osm_id'])
            
        print(f"  Captain's Express services these run IDs: {captain_runs}")
    
    for _, row in lift_run_mapping[lift_run_mapping['connection_type'] == 'run_feeds_lift'].iterrows():
        run_id = f"run_{row['run_osm_id']}"
        lift_id = f"lift_{row['lift_osm_id']}"
        
        # If filtering, only add connections related to Captain's runs
        if lift_filter and row['run_osm_id'] not in captain_runs:
            continue
            
        if run_id in G and lift_id in G:
            run_lift_count += 1
            # Use point_index instead of run_end_point_index for consistency
            G.add_edge(run_id, lift_id,
                      connection_type='run_feeds_lift',
                      exit_point_index=row.get('point_index', 0),
                      exit_distance_m=row.get('distance_m', 0))
    
    # Add run-to-run connections using segment node IDs
    added_connections = set()
    run_run_count = 0
    
    # For each run segment
    for _, segment in ski_segments.iterrows():
        if pd.isna(segment['from_node_id']) or pd.isna(segment['to_node_id']):
            continue
            
        from_node_id = segment['from_node_id']
        to_node_id = segment['to_node_id']
        current_run_id = segment['run_osm_id']
        
        # If filtering, only process segments for Captain's runs
        if lift_filter and current_run_id not in captain_runs:
            continue
        
        # Find segments where this segment's to_node_id matches another segment's from_node_id
        # These are potential connections between runs
        connecting_segments = ski_segments[
            (ski_segments['from_node_id'] == to_node_id) & 
            (ski_segments['run_osm_id'] != current_run_id)
        ]
        
        for _, conn_segment in connecting_segments.iterrows():
            if pd.isna(conn_segment['run_osm_id']):
                continue
                
            from_run_id = f"run_{current_run_id}"
            to_run_id = f"run_{conn_segment['run_osm_id']}"
            connection_key = f"{from_run_id}:{to_run_id}"
            
            if connection_key in added_connections:
                continue
            
            # Skip gradient filtering entirely
            
            if from_run_id in G and to_run_id in G:
                # Find the points corresponding to the nodes
                from_run_points = points_by_run.get(current_run_id, [])
                to_run_points = points_by_run.get(conn_segment['run_osm_id'], [])
                
                # Find exit point on current run
                exit_point = next((p for p in from_run_points if p['node_id'] == to_node_id), None)
                
                # Find entry point on next run
                entry_point = next((p for p in to_run_points if p['node_id'] == to_node_id), None)
                
                if exit_point is not None and entry_point is not None:
                    added_connections.add(connection_key)
                    run_run_count += 1
                    
                    G.add_edge(from_run_id, to_run_id,
                              connection_type='run_to_run',
                              exit_point_index=exit_point['point_index'],
                              entry_point_index=entry_point['point_index'],
                              exit_distance_m=exit_point['distance_along_run_m'],
                              entry_distance_m=entry_point['distance_along_run_m'],
                              node_id=to_node_id)
    
    # Print network stats
    print(f"  Network statistics: {lift_count} lifts, {run_count} runs")
    print(f"  Connections: {lift_run_count} lift→run, {run_lift_count} run→lift, {run_run_count} run→run")
    
    # Additional debugging
    print("\nDEBUG: Lift connectivity check:")
    for node in G.nodes():
        if node.startswith('lift_'):
            lift_name = G.nodes[node].get('name', 'Unknown')
            successors = list(G.successors(node))
            print(f"  Lift '{lift_name}' connects to {len(successors)} runs")
            
            if lift_filter and lift_filter.lower() in lift_name.lower():
                print(f"  CAPTAIN'S DETAILS:")
                for successor in successors:
                    run_name = G.nodes[successor].get('name', 'Unknown')
                    print(f"    → Run: {run_name}")
    
    return G

def find_all_paths(G: nx.DiGraph, resort: str, lift_filter: str = None, max_path_depth: int = 17) -> List[Dict]:
    """
    Find all possible ski paths in a resort using tree traversal.
    Uses precise distance calculations based on segment data.
    """
    paths = []
    path_id = 0

    # Find all lift tops as starting points - ONLY get lifts that service runs
    start_nodes = [n for n in G.nodes() if (
        n.startswith('lift_') and 
        any(succ for succ in G.successors(n) if succ.startswith('run_'))
    )]
    
    # Filter to only Captain's Express if specified - FIXED: make case insensitive
    if lift_filter:
        original_start_nodes = start_nodes.copy()
        start_nodes = [n for n in start_nodes if lift_filter.lower() in G.nodes[n].get('name', '').lower()]
        
        # If no matching lift found, show debug info and fall back to all lifts
        if not start_nodes and original_start_nodes:
            print(f"\n⚠️ WARNING: No lift matching '{lift_filter}' with run connections found")
            print("Available starting lifts:")
            for node in original_start_nodes:
                print(f"  - {G.nodes[node].get('name', 'Unknown')}")
            # Fall back to all lifts as a last resort
            start_nodes = original_start_nodes

    # Check if we have any starting points
    if not start_nodes:
        print("  ❌ ERROR: No valid lift starting points found!")
        return []
        
    print(f"  Found {len(start_nodes)} starting lift points")
    
    # Rest of the function remains the same