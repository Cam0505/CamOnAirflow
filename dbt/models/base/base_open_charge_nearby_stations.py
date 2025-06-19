import pandas as pd
import numpy as np

def config():
    return {
        "materialized": "table"  # Full table, recalculated each time
    }

def model(dbt, session):
    # Get source data
    df = dbt.source("opencharge", "opencharge_stations").df()
    
    def haversine(lat1, lon1, lat2, lon2):
        R = 6371  # Earth radius in km
        phi1, phi2 = np.radians(lat1), np.radians(lat2)
        dphi = np.radians(lat2 - lat1)
        dlambda = np.radians(lon2 - lon1)
        a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
        return 2 * R * np.arcsin(np.sqrt(a))

    threshold_km = 10
    nearby = []
    
    # Extract station data
    stations_df = df[['id', 'name', 'lat', 'lon']].copy()
    n = len(stations_df)
    
    print(f"Calculating distances between {n} stations...")
    
    # Calculate all pairwise distances (within threshold)
    for i in range(n):
        station1 = stations_df.iloc[i]
        
        for j in range(i + 1, n):  # Only process each pair once
            station2 = stations_df.iloc[j]
            
            # Calculate straight-line distance
            straight_dist = haversine(station1['lat'], station1['lon'], 
                                     station2['lat'], station2['lon'])
            
            # Only include if within threshold
            if straight_dist <= threshold_km:
                nearby.append({
                    "id_1": station1['id'],
                    "id_2": station2['id'],
                    "name_1": station1['name'],
                    "name_2": station2['name'],
                    "lat_1": float(station1['lat']),
                    "lon_1": float(station1['lon']),
                    "lat_2": float(station2['lat']),
                    "lon_2": float(station2['lon']),
                    "straight_line_distance_km": float(straight_dist)
                })
    
    print(f"Found {len(nearby)} station pairs within {threshold_km}km")
    return pd.DataFrame(nearby)