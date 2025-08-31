"""
DBT model for ice climbing conditions analysis.
Calculates ice formation, degradation, quality and other metrics.
"""

import pandas as pd
from typing import Any

def count_freeze_thaw(series):
    # Count number of times temperature crosses 0°C
    return int(((series.shift(1) < 0) & (series >= 0)).sum() + ((series.shift(1) > 0) & (series <= 0)).sum())

def freeze_thaw_factor(cycles):
    # Bonus for 1–2 cycles, penalty for >2
    if cycles <= 2:
        return 1.05  # slight bonus
    elif cycles <= 4:
        return 1.0   # neutral
    else:
        return max(0.8, 1 - 0.2 * ((cycles - 4) / 4))  # penalty

def model(dbt: Any, session: Any):
    # DISABLED: This model is temporarily disabled due to incorrect logic
    # TODO: Fix the data sources and column references before re-enabling

    # Return empty DataFrame with expected schema to prevent DBT errors
    result = pd.DataFrame(columns=[
        'location', 'country', 'date', 'timezone',
        'hours_count', 'mean_temperature', 'total_precip', 'total_snow',
        'mean_cloud', 'mean_wind', 'mean_dewpoint', 'mean_pressure',
        'mean_rh', 'mean_shortwave', 'sunshine_hours', 'daylight_hours',
        'freeze_thaw_cycles', 'hours_below_freeze', 'hours_above_degrade',
        'is_forming_day', 'is_ice_forming', 'forming_score',
        'ice_has_formed', 'is_ice_degrading', 'ice_quality'
    ])
    return result


# ORIGINAL LOGIC PRESERVED BELOW (commented out for future reference):
"""
Original ice enrichment model logic - DISABLED

def model(dbt: Any, session: Any):
    # Get daily weather stats

    dbt.config(
        materialized="table",
        unique_key=["location", "date"]
    )

    daily_weather = dbt.ref("base_daily_weather").df()

    # Get thresholds
    thresholds = dbt.source("ice_climbing", "ice_climbing_thresholds").df()

    # Process each location with its specific thresholds
    result_dfs = []

    for location in daily_weather['location'].unique():
        loc_df = daily_weather[daily_weather['location'] == location]

        # Get thresholds for this location
        loc_thresholds = thresholds[thresholds['name'] == location]

        if loc_thresholds.empty:
            print(f"Warning: No thresholds found for {location}, skipping")
            continue

        # Extract threshold values
        forming_temp = loc_thresholds['forming_temp'].iloc[0]
        forming_hours = loc_thresholds['forming_hours'].iloc[0]
        forming_days = int(loc_thresholds['forming_days'].iloc[0])
        formed_days = int(loc_thresholds['formed_days'].iloc[0])
        degrade_temp = loc_thresholds['degrade_temp'].iloc[0]
        degrade_hours = loc_thresholds['degrade_hours'].iloc[0]

        # Calculate additional metrics
        loc_df = loc_df.sort_values('date')

        # Calculate freeze-thaw cycles from the raw hourly data
        hourly = dbt.source("ice_climbing", "weather_hourly_raw").df()
        hourly = hourly[hourly['location'] == location]

        # Calculate freeze-thaw cycles per day
        freeze_thaw_cycles = hourly.groupby('date')['temperature_2m'].apply(count_freeze_thaw)

        # Add freeze-thaw cycles to daily data
        loc_df = loc_df.merge(
            freeze_thaw_cycles.reset_index().rename(columns={'temperature_2m': 'freeze_thaw_cycles'}),
            on='date', how='left'
        )

        # Fill NAs
        loc_df['freeze_thaw_cycles'] = loc_df['freeze_thaw_cycles'].fillna(0)

        # Calculate hours below freezing and above degradation threshold
        hours_below_freeze = hourly[hourly['temperature_2m'] < forming_temp].groupby('date').size()
        hours_above_degrade = hourly[hourly['temperature_2m'] > degrade_temp].groupby('date').size()

        # Add to daily stats
        loc_df = loc_df.merge(
            hours_below_freeze.reset_index().rename(columns={0: 'hours_below_freeze'}),
            on='date', how='left'
        )
        loc_df = loc_df.merge(
            hours_above_degrade.reset_index().rename(columns={0: 'hours_above_degrade'}),
            on='date', how='left'
        )

        # Fill NAs
        loc_df['hours_below_freeze'] = loc_df['hours_below_freeze'].fillna(0)
        loc_df['hours_above_degrade'] = loc_df['hours_above_degrade'].fillna(0)

        # Forming day: at least forming_hours below freezing
        loc_df["is_forming_day"] = (loc_df["hours_below_freeze"] >= forming_hours).astype(int)

        # Fraction of forming days in the last forming_days window
        loc_df["is_ice_forming"] = (
            loc_df["is_forming_day"]
            .rolling(window=forming_days, min_periods=1)
            .mean()
            .clip(0, 1)
        )

        # Weighted score for forming (for quality/has_formed)
        forming_score = (
            # Check for minimum freezing hours and that freezing hours > degrading hours
            ((loc_df["hours_below_freeze"] >= 9) & 
             (loc_df["hours_below_freeze"] > loc_df["hours_above_degrade"] * 1.5)).astype(float) * (
                0.40 * (loc_df["hours_below_freeze"] / 24).clip(0, 1) +
                0.15 * (1 - loc_df["mean_cloud"] / 100).clip(0, 1) +
                0.1 * (loc_df["total_snow"] / 10).clip(0, 1) +
                0.05 * (1 - loc_df["mean_wind"] / 15).clip(0, 1) +
                0.1 * (loc_df["mean_rh"] / 100).clip(0, 1) +
                0.1 * (1 - loc_df["mean_shortwave"] / 200).clip(0, 1) +
                0.1 * (loc_df["sunshine_hours"] / 12).clip(0, 1)
            )
        ).clip(0, 1)

        # Add forming score to dataframe
        loc_df["forming_score"] = forming_score

        # Rolling mean for last N days for "has formed" and "ice_quality"
        loc_df["ice_has_formed"] = (
            forming_score.rolling(window=formed_days, min_periods=1).mean().clip(0, 1)
        )

        # Degrading: combine several conditions, score is fraction of last 7 days with any degrade condition
        degrade_conditions = (
            # Make degradation happen faster by lowering these thresholds
            (loc_df["hours_above_degrade"] >= degrade_hours * 0.8) |  # Was just degrade_hours
            ((loc_df["mean_shortwave"] > 120) & (loc_df["mean_cloud"] < 40)) |  
            ((loc_df["total_precip"] > 0) & (loc_df["hours_above_degrade"] > 0)) |
            ((loc_df["mean_rh"] > 85) & (loc_df["hours_above_degrade"] > 0)) |  
            ((loc_df["mean_wind"] > 8) & (loc_df["hours_above_degrade"] > 0))   
        )
        loc_df["is_ice_degrading"] = (
            degrade_conditions.rolling(window=7, min_periods=1).mean().clip(0, 1)
        )

        # Ice quality: penalize for degrading and for excessive freeze/thaw cycles
        loc_df["ice_quality"] = (
            loc_df["ice_has_formed"] *
            (1 - 0.9 * loc_df["is_ice_degrading"]) *  
            loc_df["freeze_thaw_cycles"].apply(freeze_thaw_factor)
        ).clip(0, 1)

        result_dfs.append(loc_df)

    # Combine all location results
    if result_dfs:
        result = pd.concat(result_dfs, ignore_index=True)
    else:
        # Return empty DataFrame with expected schema
        result = pd.DataFrame(columns=[
            'location', 'country', 'date', 'timezone',
            'hours_count', 'mean_temperature', 'total_precip', 'total_snow',
            'mean_cloud', 'mean_wind', 'mean_dewpoint', 'mean_pressure',
            'mean_rh', 'mean_shortwave', 'sunshine_hours', 'daylight_hours',
            'freeze_thaw_cycles', 'hours_below_freeze', 'hours_above_degrade',
            'is_forming_day', 'is_ice_forming', 'forming_score',
            'ice_has_formed', 'is_ice_degrading', 'ice_quality'
        ])
    return result
"""