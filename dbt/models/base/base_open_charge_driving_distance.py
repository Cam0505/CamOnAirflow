import pandas as pd
import time
import requests
import os
import random
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from typing import Any

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
                print(f"Loaded .env from: {path}")
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
            return dbt.this.df()
    else:
        print("Full refresh - loading pairs to process")
        # For full refresh, take pairs directly from source table
        process_df = dbt.ref("base_open_charge_nearby_stations").df().head(max_requests_per_run)
    
    def get_driving_distance(lon1, lat1, lon2, lat2, max_retries=3):
        """Get driving distance using OpenRouteService API with retries"""
        # Prepare request
        url = "https://api.openrouteservice.org/v2/directions/driving-car"
        headers = {
            'Accept': 'application/json',
            'Authorization': api_key
        }
        data = {
            "coordinates": [[float(lon1), float(lat1)], [float(lon2), float(lat2)]]
        }
        
        # Try with exponential backoff
        for attempt in range(max_retries):
            try:
                print(f"API request for coordinates: [{float(lon1):.5f}, {float(lat1):.5f}] to [{float(lon2):.5f}, {float(lat2):.5f}]")
                response = requests.post(url, json=data, headers=headers)
                print(f"API response status: {response.status_code}")
                
                if response.status_code == 200:
                    result = response.json()
                    # Distance comes back in meters, convert to km
                    distance = result['routes'][0]['summary']['distance'] / 1000
                    print(f"Got distance: {distance} km")
                    return distance
                elif response.status_code == 429 or response.status_code == 503:
                    # Rate limit or service unavailable - exponential backoff
                    wait_time = (3 ** attempt) + random.uniform(6, 6)
                    print(f"Rate limited. Waiting {wait_time:.2f}s before retry {attempt+1}/{max_retries}")
                    time.sleep(wait_time)
                else:
                    print(f"API error: {response.status_code} - {response.text}")
                    return None
            except Exception as e:
                print(f"Error getting driving distance: {e}")
                wait_time = (3 ** attempt) + random.uniform(6, 6)
                time.sleep(wait_time)
        
        # If all retries failed
        print("All retry attempts failed")
        return None

    # Process pairs and collect results
    results = []
    for idx, row in enumerate(process_df.itertuples(index=False), 1):
        print(f"Processing pair {idx}/{len(process_df)}: {row.name_1} to {row.name_2}")

        driving_dist = get_driving_distance(row.lon_1, row.lat_1, row.lon_2, row.lat_2)

        result = row._asdict()
        result['driving_distance_km'] = driving_dist if driving_dist is not None else pd.NA
        result['processed_at'] = pd.Timestamp.now()
        results.append(result)

        time.sleep(6)

    return pd.DataFrame(results)