"""
Simple analysis to find all ski paths from Captain's Express lift at Cardrona
"""
import pandas as pd
import networkx as nx
from typing import Dict, List, Set

def model(dbt, session):
    """
    Build a simple network of all ski paths from Captain's Express lift.
    """
    print("\n===== CAPTAIN'S EXPRESS SKI PATH FINDER =====")
    
    # Step 1: Load data
    print("\nStep 1: Loading data...")
    lift_run_mapping = dbt.ref("base_lift_run_mapping").df()
    ski_runs = dbt.ref("base_filtered_ski_runs").df()
    ski_points = dbt.ref("base_filtered_ski_points").df()
    ski_segments = dbt.ref("base_filtered_ski_segments").df()
    
    # Step 2: Filter to Cardrona Resort only
    resort = "Cardrona Alpine Resort"
    lift_name = "Captain's Express"
    
    print(f"\nStep 2: Filtering data for {resort} and {lift_name}")
    lift_run_mapping = lift_run_mapping[lift_run_mapping['resort'] == resort]
    ski_runs = ski_runs[ski_runs['resort'] == resort]
    ski_points = ski_points[ski_points['resort'] == resort]
    ski_segments = ski_segments[ski_segments['resort'] == resort]
    
    # Print summary of data
    print(f"  • Found {len(lift_run_mapping)} lift-run connections")
    print(f"  • Found {len(ski_runs)} ski runs")
    print(f"  • Found {len(ski_points)} ski points")
    print(f"  • Found {len(ski_segments)} ski segments")
    
    # Step 3: Find Captain's Express lift
    print(f"\nStep 3: Finding {lift_name} lift")
    captains_mapping = lift_run_mapping[
        lift_run_mapping['lift_name'].str.lower().str.contains('captain')
    ]
    
    if len(captains_mapping) == 0:
        print(f"  ❌ ERROR: Could not find {lift_name}")
        return pd.DataFrame()
    
    captains_lift_id = captains_mapping['lift_osm_id'].iloc[0]
    print(f"  • Found Captain's Express lift with ID: {captains_lift_id}")
    
    # Step 4: Find runs serviced by Captain's Express
    print(f"\nStep 4: Finding runs serviced by {lift_name}")
    captains_runs = lift_run_mapping[
        (lift_run_mapping['lift_osm_id'] == captains_lift_id) &
        (lift_run_mapping['connection_type'] == 'lift_services_run')
    ]
    
    if len(captains_runs) == 0:
        print(f"  ❌ ERROR: No runs found for {lift_name}")
        return pd.DataFrame()
    
    print(f"  • Found {len(captains_runs)} runs serviced by {lift_name}:")
    for _, row in captains_runs.iterrows():
        print(f"    - {row['run_name']} (ID: {row['run_osm_id']})")
    
    # Step 5: Build network graph
    print("\nStep 5: Building network graph")
    G = nx.DiGraph()
    
    # Add lift node
    lift_node_id = f"lift_{captains_lift_id}"
    G.add_node(lift_node_id, 
              node_type='lift',
              name="Captain's Express",
              osm_id=captains_lift_id)
    print(f"  • Added lift node: Captain's Express")
    
    # Add run nodes from Captain's Express
    for _, row in captains_runs.iterrows():
        run_id = row['run_osm_id']
        run_node_id = f"run_{run_id}"
        
        # Get run details
        run_data = ski_runs[ski_runs['osm_id'] == run_id]
        if len(run_data) == 0:
            print(f"  • Skipping run {run_id} - not found in ski_runs")
            continue
            
        run_data = run_data.iloc[0]
        
        # Add run node
        G.add_node(run_node_id,
                  node_type='run',
                  name=row['run_name'],
                  osm_id=run_id,
                  difficulty=run_data.get('difficulty', 'unknown'),
                  length_m=run_data.get('run_length_m', 0),
                  vertical_drop=(run_data.get('top_elevation_m', 0) - 
                                run_data.get('bottom_elevation_m', 0)))
                  
        # Add lift-to-run edge
        G.add_edge(lift_node_id, run_node_id,
                  connection_type='lift_services_run',
                  point_index=0)  # Start at top of run
                  
        print(f"  • Added run node: {row['run_name']}")
    
    # Step 6: Add run-to-run connections
    print("\nStep 6: Adding run-to-run connections")
    
    # Get all run IDs from Captain's
    captains_run_ids = set(captains_runs['run_osm_id'].tolist())
    
    # Create dictionary to store run points
    run_points = {}
    for run_id in captains_run_ids:
        points = ski_points[ski_points['osm_id'] == run_id]
        run_points[run_id] = points.sort_values('point_index')
    
    # Find connections using segments
    connections_added = 0
    all_run_ids = set()  # Track all runs in the network
    
    # First add Captain's runs
    for run_id in captains_run_ids:
        all_run_ids.add(run_id)
    
    # Expand network to include all reachable runs within reasonable distance
    max_network_size = 100  # Limit to prevent excessive size
    depth = 0
    frontier = captains_run_ids.copy()
    
    while frontier and len(all_run_ids) < max_network_size and depth < 5:
        depth += 1
        new_frontier = set()
        
        for run_id in frontier:
            # Get segments for this run
            run_segments = ski_segments[ski_segments['run_osm_id'] == run_id]
            
            for _, segment in run_segments.iterrows():
                # Skip segments without node IDs
                if pd.isna(segment['to_node_id']):
                    continue
                    
                to_node_id = segment['to_node_id']
                from_point_index = segment.get('from_point_index', 0)
                to_point_index = segment.get('to_point_index', 0)
                
                # Find connecting segments
                connecting_segments = ski_segments[
                    (ski_segments['from_node_id'] == to_node_id) & 
                    (ski_segments['run_osm_id'] != run_id)
                ]
                
                for _, conn_seg in connecting_segments.iterrows():
                    # Add edge between runs
                    from_run_node = f"run_{run_id}"
                    to_run_id = conn_seg['run_osm_id']
                    to_run_node = f"run_{to_run_id}"
                    
                    # Add the destination run to our network
                    if to_run_id not in all_run_ids:
                        all_run_ids.add(to_run_id)
                        new_frontier.add(to_run_id)
                        
                        # Add the node if it's not already in graph
                        if to_run_node not in G:
                            run_data = ski_runs[ski_runs['osm_id'] == to_run_id]
                            if len(run_data) > 0:
                                run_data = run_data.iloc[0]
                                G.add_node(to_run_node,
                                          node_type='run',
                                          name=run_data.get('run_name', 'Unknown'),
                                          osm_id=to_run_id,
                                          difficulty=run_data.get('difficulty', 'unknown'),
                                          length_m=run_data.get('run_length_m', 0),
                                          vertical_drop=(run_data.get('top_elevation_m', 0) - 
                                                        run_data.get('bottom_elevation_m', 0)))
                            else:
                                continue
                    
                    # Skip if this edge already exists
                    if G.has_edge(from_run_node, to_run_node):
                        continue
                        
                    # Get point indices for the connection
                    from_point_idx = to_point_index  # Exit point on source run
                    to_point_idx = conn_seg.get('from_point_index', 0)  # Entry point on destination run
                    
                    # Add the edge
                    G.add_edge(from_run_node, to_run_node, 
                              connection_type='run_to_run',
                              node_id=to_node_id,
                              from_point_idx=from_point_idx,
                              to_point_idx=to_point_idx)
                    connections_added += 1
                    print(f"  • Added connection: {G.nodes[from_run_node].get('name')} → {G.nodes[to_run_node].get('name')}")
        
        frontier = new_frontier
    
    print(f"  • Added {connections_added} run-to-run connections across {len(all_run_ids)} runs")
    
    # Step 7: Add run-to-lift connections
    print("\nStep 7: Adding run-to-lift connections")
    
    run_lift_connections = lift_run_mapping[
        lift_run_mapping['connection_type'] == 'run_feeds_lift'
    ]
    
    # Debug: Show all potential run-lift connections
    print("\n  DEBUG: All potential run-feeds-lift connections in data:")
    for _, row in run_lift_connections.iterrows():
        print(f"    - {row['run_name']} → {row['lift_name']} (IDs: {row['run_osm_id']} → {row['lift_osm_id']})")
    
    lift_connections_added = 0
    
    # Special handling for Over Run - add direct connection to WhiteStar Express
    over_run_node = None
    whitestar_lift_node = None
    
    # Find Over Run node in our graph
    for node in G.nodes():
        if node.startswith('run_') and G.nodes[node].get('name') == "Over Run":
            over_run_node = node
            break
    
    # Special check for Over Run
    if over_run_node:
        print(f"\n  Special handling for Over Run (node: {over_run_node}):")
        
        # Find WhiteStar Express in lift mapping
        whitestar_mapping = lift_run_mapping[
            lift_run_mapping['lift_name'].str.lower().str.contains('whitestar')
        ]
        
        if len(whitestar_mapping) > 0:
            whitestar_lift_id = whitestar_mapping['lift_osm_id'].iloc[0]
            whitestar_lift_node = f"lift_{whitestar_lift_id}"
            
            # Add WhiteStar Express lift node if it doesn't exist
            if whitestar_lift_node not in G:
                G.add_node(whitestar_lift_node,
                          node_type='lift',
                          name="WhiteStar Express",
                          osm_id=whitestar_lift_id)
                print(f"    - Added WhiteStar Express lift node")
            
            # Add direct connection from Over Run to WhiteStar
            if not G.has_edge(over_run_node, whitestar_lift_node):
                G.add_edge(over_run_node, whitestar_lift_node,
                          connection_type='run_feeds_lift',
                          point_index=-1)  # Bottom of run
                lift_connections_added += 1
                print(f"    - Added missing connection: Over Run → WhiteStar Express")
            else:
                print(f"    - Connection already exists: Over Run → WhiteStar Express")
        else:
            print(f"    - Could not find WhiteStar Express in lift mapping")
    
    # Regular run-to-lift connections
    for _, row in run_lift_connections.iterrows():
        run_id = row['run_osm_id']
        lift_id = row['lift_osm_id']
        
        # Skip self-connections to Captain's Express
        if lift_id == captains_lift_id:
            continue
        
        # Create node IDs
        run_node = f"run_{run_id}"
        lift_node = f"lift_{lift_id}"
        
        # Skip if source run node doesn't exist in our graph
        if run_node not in G:
            continue
            
        # Add lift node if it doesn't exist
        if lift_node not in G:
            G.add_node(lift_node,
                      node_type='lift',
                      name=row['lift_name'],
                      osm_id=lift_id)
        
        # Skip if edge already exists
        if G.has_edge(run_node, lift_node):
            continue
            
        # Add edge
        G.add_edge(run_node, lift_node,
                  connection_type='run_feeds_lift',
                  point_index=row.get('point_index', -1))  # Use -1 to indicate bottom of run
        lift_connections_added += 1
        print(f"  • Added lift connection: {G.nodes[run_node].get('name')} → {row['lift_name']}")
    
    print(f"  • Added {lift_connections_added} run-to-lift connections")
    
    # Step 8: Find paths from Captain's Express to all possible endpoints
    print("\nStep 8: Finding all possible paths from Captain's Express")
    
    # Check if graph is built correctly
    print(f"  • Graph has {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
    
    # Get all reachable nodes from Captain's Express for debugging
    print("\n  • Reachable nodes from Captain's Express:")
    captain_reachable = nx.descendants(G, lift_node_id)
    reachable_runs = [n for n in captain_reachable if n.startswith('run_')]
    reachable_lifts = [n for n in captain_reachable if n.startswith('lift_')]
    print(f"    - {len(reachable_runs)} runs reachable")
    print(f"    - {len(reachable_lifts)} lifts reachable")
    
    # Find all paths from Captain's Express to every possible endpoint
    paths = []
    path_id = 0
    
    # Get Captain's runs
    captain_successors = list(G.successors(lift_node_id))
    print(f"  • Captain's Express connects to {len(captain_successors)} runs:")
    
    for run_node in captain_successors:
        run_name = G.nodes[run_node].get('name', 'Unknown')
        print(f"    - Processing run: {run_name}")
        
        # Start path with lift and first run
        path = [lift_node_id, run_node]
        visited = {lift_node_id: True, run_node: True}
        
        # Find all paths from this run
        find_paths(G, run_node, path, visited, paths, path_id, current_point_idx=0)
        path_id += 1
    
    print(f"  • Found {len(paths)} total paths")
    
    # Step 9: Convert paths to DataFrame
    print("\nStep 9: Converting paths to DataFrame")

    if not paths:
        print("  ❌ No paths found!")
        return pd.DataFrame()

    result_rows = []

    # Create lookup for run-to-lift connections
    run_to_lift_map = {}
    lift_run_connections = lift_run_mapping[lift_run_mapping['connection_type'] == 'run_feeds_lift']
    for _, row in lift_run_connections.iterrows():
        run_id = row['run_osm_id']
        lift_id = row['lift_osm_id']
        lift_name = row['lift_name']
        
        if run_id not in run_to_lift_map:
            run_to_lift_map[run_id] = []
        run_to_lift_map[run_id].append((lift_id, lift_name))

    for path_data in paths:
        path = path_data['path']
        
        # Get run nodes only
        run_nodes = [n for n in path if n.startswith('run_')]
        
        # Skip paths with no runs
        if not run_nodes:
            continue
            
        # Get run details
        run_names = []
        run_ids = []
        total_distance = 0
        total_vertical = 0
        
        for node in run_nodes:
            node_data = G.nodes[node]
            run_names.append(node_data.get('name', 'Unknown'))
            run_ids.append(int(node.split('_')[1]))
            total_distance += node_data.get('length_m', 0)
            total_vertical += node_data.get('vertical_drop', 0)
        
        # Get ending point info
        last_node = path[-1]
        
        # IMPROVED ENDING TYPE DETECTION
        if last_node.startswith('lift_'):
            # Path ends at a lift node
            ending_name = G.nodes[last_node].get('name', 'Unknown Lift')
            ending_id = G.nodes[last_node].get('osm_id', 0)
            
            # Check if this is the same as Captain's Express
            if ending_name.lower() == "captain's express":
                ending_type = 'same_lift'
            else:
                ending_type = 'different_lift'
        else:
            # Path ends at a run node
            ending_name = G.nodes[last_node].get('name', 'Unknown Run')
            ending_id = int(last_node.split('_')[1])
            
            # Check if this run connects to any lift according to lift_run_mapping
            if ending_id in run_to_lift_map:
                # Run connects to at least one lift
                for lift_id, lift_name in run_to_lift_map[ending_id]:
                    if lift_name.lower() == "captain's express":
                        ending_type = 'same_lift'
                        ending_name = f"{ending_name} → Captain's Express"
                        break
                else:
                    # Run connects to different lift(s)
                    ending_type = 'different_lift'
                    connected_lifts = [lift_name for _, lift_name in run_to_lift_map[ending_id]]
                    ending_name = f"{ending_name} → {', '.join(connected_lifts)}"
            else:
                # Run doesn't connect to any lift - true run end
                ending_type = 'run_end'
        
        # Add to results
        result_rows.append({
            'path_id': path_data['id'],
            'starting_lift': "Captain's Express",
            'run_count': len(run_nodes),
            'total_distance_m': round(total_distance, 1),
            'total_vertical_m': round(total_vertical, 1),
            'run_path': ' → '.join(run_names),
            'run_ids': run_ids,
            'ending_type': ending_type,
            'ending_name': ending_name,
            'ending_id': ending_id
        })

    # Create DataFrame
    result_df = pd.DataFrame(result_rows)
    print(f"  • Created DataFrame with {len(result_df)} paths")

    # Print some summary info by ending type
    if len(result_df) > 0:
        print("\nPath Statistics by Ending Type:")
        ending_type_counts = result_df['ending_type'].value_counts()
        for ending_type, count in ending_type_counts.items():
            print(f"  • {ending_type}: {count} paths")
        
        print("\nAverage metrics by ending type:")
        for ending_type in ending_type_counts.index:
            filtered = result_df[result_df['ending_type'] == ending_type]
            print(f"  • {ending_type}:")
            print(f"    - Avg distance: {filtered['total_distance_m'].mean():.1f}m")
            print(f"    - Avg vertical: {filtered['total_vertical_m'].mean():.1f}m")
            print(f"    - Avg run count: {filtered['run_count'].mean():.1f}")
    
    return result_df

def find_paths(G, current_node, current_path, visited, all_paths, path_id, depth=0, current_point_idx=0):
    """Recursively find all paths from a starting node."""
    # Indent based on depth for better readability in logs
    indent = "    " + "  " * depth
    node_name = G.nodes[current_node].get('name', 'Unknown')
    node_type = "lift" if current_node.startswith('lift_') else "run"
    
    print(f"{indent}→ Visiting {node_type}: {node_name} (depth {depth})")
    
    # Safety check to prevent infinite recursion
    if depth > 50:
        print(f"{indent}⚠️ Maximum recursion depth exceeded, possible cycle detected")
        return
    
    # Process current path if it's a valid endpoint (reached a lift)
    if current_node.startswith('lift_'):
        # Only save the path if we've gone through at least one run
        if len(current_path) > 2:
            path_desc = " → ".join([G.nodes[n].get('name', 'Unknown') for n in current_path])
            print(f"{indent}✓ Found valid path to lift: {path_desc}")
            
            all_paths.append({
                'id': f"{path_id}_{len(all_paths)}",
                'path': current_path
            })
        return
    
    # Get next possible nodes
    next_nodes = list(G.successors(current_node))
    
    # REMOVED: The "junction path" logic that incorrectly created paths at intermediate points
    
    # Only create an end-of-run path if there are no next nodes (TRUE terminal point)
    if not next_nodes:
        # Always create paths for ending runs if we've gone through at least one lift+run
        if len(current_path) > 1:
            path_desc = " → ".join([G.nodes[n].get('name', 'Unknown') for n in current_path])
            print(f"{indent}✓ Found TRUE end-of-run path (no further connections): {path_desc}")
            
            all_paths.append({
                'id': f"{path_id}_{len(all_paths)}",
                'path': current_path
            })
        return
    
    # Special case for Over Run - it has a direct connection to WhiteStar Express
    # This is a fallback check for Over Run if the WhiteStar connection wasn't captured
    if G.nodes[current_node].get('name') == "Over Run" and not any(G.nodes[n].get('name') == "WhiteStar Express" for n in next_nodes):
        if len(current_path) > 1:
            path_desc = " → ".join([G.nodes[n].get('name', 'Unknown') for n in current_path])
            print(f"{indent}✓ Found special path for Over Run: {path_desc}")
            
            all_paths.append({
                'id': f"{path_id}_{len(all_paths)}",
                'path': current_path
            })
    
    print(f"{indent}  Found {len(next_nodes)} possible next nodes")
    
    # For each possible next node
    for next_node in next_nodes:
        next_name = G.nodes[next_node].get('name', 'Unknown')
        next_type = "lift" if next_node.startswith('lift_') else "run"
        
        # Skip if already visited
        if next_node in visited:
            print(f"{indent}  - Skipping {next_name}: already visited")
            continue
        
        # Handle point indices when traversing runs
        if next_node.startswith('run_'):
            edge_data = G[current_node][next_node]
            
            # When connecting between runs, we need edge data for point indices
            if current_node.startswith('run_'):
                # Get point indices from edge data
                from_point_idx = edge_data.get('from_point_idx', 0)
                to_point_idx = edge_data.get('to_point_idx', 0)
                
                # Check if we're going uphill on the same run (not allowed)
                current_run_id = current_node.split('_')[1]
                next_run_id = next_node.split('_')[1]
                
                if current_run_id == next_run_id and from_point_idx <= current_point_idx:
                    print(f"{indent}  - Skipping {next_name}: would go uphill on same run")
                    continue
                
                # Set current point index for next iteration
                next_point_idx = to_point_idx
                
                print(f"{indent}  - Exploring {next_name} (points: {from_point_idx} → {to_point_idx})")
            else:
                # Coming from a lift, start at beginning of run
                next_point_idx = 0
                print(f"{indent}  - Exploring {next_name} (from lift)")
        else:
            # Going to a lift - no point index needed
            next_point_idx = 0
            print(f"{indent}  - Exploring lift: {next_name}")
        
        # Continue traversal
        new_path = current_path + [next_node]
        new_visited = visited.copy()
        new_visited[next_node] = True
        
        # Recursive call
        find_paths(G, next_node, new_path, new_visited, all_paths, path_id, depth+1, next_point_idx)