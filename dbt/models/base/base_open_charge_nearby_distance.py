import pandas as pd
import numpy as np


def model(dbt, session):
    # Use dbt.source to reference a source table
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

    coords = df[['lat', 'lon']].to_numpy()
    ids = df['id'].to_numpy()
    n = len(df)

    for i in range(n):
        lat1, lon1 = coords[i]
        for j in range(i + 1, n):
            lat2, lon2 = coords[j]
            dist = haversine(lat1, lon1, lat2, lon2)
            if dist <= threshold_km:
                nearby.append({
                    "id_1": ids[i],
                    "id_2": ids[j],
                    "distance_km": dist
                })

    return pd.DataFrame(nearby)