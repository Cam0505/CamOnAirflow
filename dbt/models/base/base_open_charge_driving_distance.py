import pandas as pd
import requests
import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from typing import Any



def get_driving_distances_matrix(pairs, api_key):
    """
    pairs: list of (lon1, lat1, lon2, lat2)
    Returns: list of distances in km (order matches pairs)
    """

    # Build unique locations and map to indices
    locations = []
    loc_to_idx = {}
    for lon, lat in set([(p[0], p[1]) for p in pairs] + [(p[2], p[3]) for p in pairs]):
        loc_to_idx[(lon, lat)] = len(locations)
        locations.append([float(lon), float(lat)])

    sources = [loc_to_idx[(p[0], p[1])] for p in pairs]
    destinations = [loc_to_idx[(p[2], p[3])] for p in pairs]

    url = "https://api.openrouteservice.org/v2/matrix/driving-car"
    headers = {
        'Accept': 'application/json',
        'Authorization': api_key
    }
    data = {
        "locations": locations,
        "sources": sources,
        "destinations": destinations,
        "metrics": ["distance"]
    }
    response = requests.post(url, json=data, headers=headers)
    if response.status_code == 200:
        result = response.json()
        # result['distances'] is a matrix: sources x destinations
        # For 1:1 mapping, take diagonal
        return [row[i] / 1000 if row[i] is not None else pd.NA for i, row in enumerate(result['distances'])]
    else:
        print(f"Matrix API error: {response.status_code} - {response.text}")
        return [pd.NA] * len(pairs)

def model(dbt: Any, session: Any):
    dbt.config(
        materialized= "incremental",
        unique_key= ["id_1", "id_2"],
        incremental_strategy="delete+insert"
    )
    
    env_loaded = False
    potential_paths = []
    
    # 1. Try automatic dotenv detection (searches parent directories)
    dotenv_path = find_dotenv(usecwd=True)
    if dotenv_path:
        load_dotenv(dotenv_path)
        print(f"Loaded .env from: {dotenv_path}")
        env_loaded = True
    
    # 2. Try explicit project root locations if automatic detection failed
    if not env_loaded:
        potential_paths = [
            Path("/workspaces/CamOnAirFlow/.env"),  # Dev container path
            Path("/workspaces/camonairflow/.env"),  # Lowercase alternative
            Path(os.getcwd()).parent.parent / ".env",  # Relative to model dir
            Path(os.environ.get("GITHUB_WORKSPACE", "")) / ".env",  # GitHub Actions
        ]
        
        for path in potential_paths:
            if path.exists():
                load_dotenv(dotenv_path=path)
                env_loaded = True
                break
    
    # Get API key (now from environment after .env loading)
    api_key = os.environ.get('OPENROUTESERVICE_API_KEY', '')
    if not api_key:
        print("WARNING: OPENROUTESERVICE_API_KEY not found in environment!")
        for path in potential_paths:
            print(f"  - {path} {'(exists)' if path.exists() else '(not found)'}")
        return pd.DataFrame()  # Return empty DataFrame if no API key
    
    # Maximum pairs to process in one run
    max_requests_per_run = 50
    
    # Get base data differently depending on incremental or full run
    if dbt.is_incremental:
        print("Running incrementally - checking for existing pairs")
        
        # Use SQL to efficiently find only the pairs we need to process
        # This avoids loading the entire tables and doing Python-side filtering
        sql_query = f"""
        SELECT base.*
        FROM public_base.base_open_charge_nearby_stations base
        LEFT JOIN public_base.base_open_charge_driving_distance existing
            ON base.id_1 = existing.id_1 AND base.id_2 = existing.id_2
        WHERE existing.id_1 IS NULL            -- Pairs not in target table
           OR existing.driving_distance_km IS NULL  -- Pairs without distance
        LIMIT {max_requests_per_run}
        """
        
        # Execute the SQL directly through dbt
        process_df = session.sql(sql_query).df()
        
        print(f"Found {len(process_df)} pairs that need processing (limited to {max_requests_per_run})")
        
        # If nothing to process, return existing data
        if len(process_df) == 0:
            print("No new pairs to process")
            return session.table(str(dbt.this)).df()
    else:
        print("Full refresh - loading pairs to process")
        # For full refresh, take pairs directly from source table
        process_df = dbt.ref("base_open_charge_nearby_stations").df().head(max_requests_per_run)
    

    # Process pairs and collect results
    pairs = [
        (row.lon_1, row.lat_1, row.lon_2, row.lat_2)
        for row in process_df.itertuples(index=False)
    ]
    distances = get_driving_distances_matrix(pairs, api_key)

    results = []
    for idx, (row, driving_dist) in enumerate(zip(process_df.itertuples(index=False), distances), 1):
        result = row._asdict()
        result['driving_distance_km'] = driving_dist
        result['processed_at'] = pd.Timestamp.now()
        results.append(result)

    return pd.DataFrame(results)
