#!/usr/bin/env python3
"""Compare cumulative seasonal snowfall across resorts using multiple Open-Meteo methods.

Methods:
0. Direct Open-Meteo snowfall totals.
1. Precipitation + temperature converted to snowfall.
2. A snowpack-adjusted precipitation model.
3. The existing 40% depth + 60% precip consensus blend.

The goal is consistency and transparency rather than best-fit calibration.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests

CHARTS_DIR = "/workspaces/CamOnAirFlow/charts"
os.makedirs(CHARTS_DIR, exist_ok=True)

DIRECT_METHOD = "0. OM direct snowfall"
PT_METHOD = "1. Precip + temp model"
ENHANCED_PT_METHOD = "2. Snowpack-adjusted model"
CONSENSUS_METHOD = "3. Depth-precip blend"
METHOD_COLORS = {
    DIRECT_METHOD: "#377eb8",
    PT_METHOD: "#4daf4a",
    ENHANCED_PT_METHOD: "#1b9e77",
    CONSENSUS_METHOD: "#e41a1c",
}


@dataclass
class ResortConfig:
    name: str
    lat: float
    lon: float
    timezone: str
    season_label: str
    season_start: str
    season_end: str
    resort_close_date: str
    resort_elevation_m: int
    peak_elevation_m: int
    base_elevation_m: int
    lapse_rate_c_per_km: float
    precip_increase_per_100m: float
    max_precip_factor: float
    snowpack_gain_factor: float
    snow_full_below_c: float
    rain_all_above_c: float
    region_profile: str
    month_order: list[str]
    reported_total_cm: float | None = None


BASE_RESORTS = [
    ResortConfig(
        name="Appi Kogen",
        lat=40.0054,
        lon=140.9602,
        timezone="Asia/Tokyo",
        season_label="2024-2025",
        season_start="2024-11-01",
        season_end="2025-05-10",
        resort_close_date="2025-05-06",
        resort_elevation_m=1180,
        peak_elevation_m=1304,
        base_elevation_m=450,
        lapse_rate_c_per_km=5.8,
        precip_increase_per_100m=0.05,
        max_precip_factor=1.45,
        snowpack_gain_factor=1.45,
        snow_full_below_c=-1.5,
        rain_all_above_c=3.0,
        region_profile="japan_maritime_powder",
        month_order=["Nov", "Dec", "Jan", "Feb", "Mar", "Apr"],
        reported_total_cm=1200.0,
    ),
    ResortConfig(
        name="Treble Cone",
        lat=-44.6301,
        lon=168.8806,
        timezone="Pacific/Auckland",
        season_label="2024",
        season_start="2024-06-01",
        season_end="2024-11-30",
        resort_close_date="2024-10-13",
        resort_elevation_m=1300,
        peak_elevation_m=1960,
        base_elevation_m=800,
        lapse_rate_c_per_km=6.5,
        precip_increase_per_100m=0.02,
        max_precip_factor=1.12,
        snowpack_gain_factor=1.10,
        snow_full_below_c=-1.2,
        rain_all_above_c=1.7,
        region_profile="nz_maritime_alpine",
        month_order=["Jun", "Jul", "Aug", "Sep", "Oct", "Nov"],
        reported_total_cm=260.0,
    ),
]


RESORTS = [
    ResortConfig(
        name="Appi Kogen",
        lat=40.0054,
        lon=140.9602,
        timezone="Asia/Tokyo",
        season_label="2022-2023",
        season_start="2022-11-01",
        season_end="2023-05-10",
        resort_close_date="2023-05-07",
        resort_elevation_m=1180,
        peak_elevation_m=1304,
        base_elevation_m=450,
        lapse_rate_c_per_km=5.8,
        precip_increase_per_100m=0.05,
        max_precip_factor=1.45,
        snowpack_gain_factor=1.45,
        snow_full_below_c=-1.5,
        rain_all_above_c=3.0,
        region_profile="japan_maritime_powder",
        month_order=["Nov", "Dec", "Jan", "Feb", "Mar", "Apr"],
        reported_total_cm=820.0,
    ),
    ResortConfig(
        name="Appi Kogen",
        lat=40.0054,
        lon=140.9602,
        timezone="Asia/Tokyo",
        season_label="2023-2024",
        season_start="2023-11-01",
        season_end="2024-05-10",
        resort_close_date="2024-05-06",
        resort_elevation_m=1180,
        peak_elevation_m=1304,
        base_elevation_m=450,
        lapse_rate_c_per_km=5.8,
        precip_increase_per_100m=0.05,
        max_precip_factor=1.45,
        snowpack_gain_factor=1.45,
        snow_full_below_c=-1.5,
        rain_all_above_c=3.0,
        region_profile="japan_maritime_powder",
        month_order=["Nov", "Dec", "Jan", "Feb", "Mar", "Apr"],
        reported_total_cm=700.0,
    ),
    ResortConfig(
        name="Treble Cone",
        lat=-44.6301,
        lon=168.8806,
        timezone="Pacific/Auckland",
        season_label="2022",
        season_start="2022-06-01",
        season_end="2022-11-30",
        resort_close_date="2022-10-16",
        resort_elevation_m=1300,
        peak_elevation_m=1960,
        base_elevation_m=800,
        lapse_rate_c_per_km=6.5,
        precip_increase_per_100m=0.02,
        max_precip_factor=1.12,
        snowpack_gain_factor=1.10,
        snow_full_below_c=-1.2,
        rain_all_above_c=1.7,
        region_profile="nz_maritime_alpine",
        month_order=["Jun", "Jul", "Aug", "Sep", "Oct", "Nov"],
        reported_total_cm=280.0,
    ),
    ResortConfig(
        name="Treble Cone",
        lat=-44.6301,
        lon=168.8806,
        timezone="Pacific/Auckland",
        season_label="2023",
        season_start="2023-06-01",
        season_end="2023-11-30",
        resort_close_date="2023-10-15",
        resort_elevation_m=1300,
        peak_elevation_m=1960,
        base_elevation_m=800,
        lapse_rate_c_per_km=6.5,
        precip_increase_per_100m=0.02,
        max_precip_factor=1.12,
        snowpack_gain_factor=1.10,
        snow_full_below_c=-1.2,
        rain_all_above_c=1.7,
        region_profile="nz_maritime_alpine",
        month_order=["Jun", "Jul", "Aug", "Sep", "Oct", "Nov"],
        reported_total_cm=160.0,
    ),
    ResortConfig(
        name="The Remarkables",
        lat=-45.0578,
        lon=168.7765,
        timezone="Pacific/Auckland",
        season_label="2024",
        season_start="2024-06-01",
        season_end="2024-11-30",
        resort_close_date="2024-10-13",
        resort_elevation_m=1550,
        peak_elevation_m=1943,
        base_elevation_m=1350,
        lapse_rate_c_per_km=6.5,
        precip_increase_per_100m=0.025,
        max_precip_factor=1.15,
        snowpack_gain_factor=1.08,
        snow_full_below_c=-1.3,
        rain_all_above_c=1.8,
        region_profile="nz_maritime_alpine",
        month_order=["Jun", "Jul", "Aug", "Sep", "Oct", "Nov"],
        reported_total_cm=350.0,
    ),
    ResortConfig(
        name="Perisher",
        lat=-36.4042,
        lon=148.4111,
        timezone="Australia/Sydney",
        season_label="2024",
        season_start="2024-06-01",
        season_end="2024-10-31",
        resort_close_date="2024-10-07",
        resort_elevation_m=1800,
        peak_elevation_m=2054,
        base_elevation_m=1720,
        lapse_rate_c_per_km=6.2,
        precip_increase_per_100m=0.02,
        max_precip_factor=1.10,
        snowpack_gain_factor=1.08,
        snow_full_below_c=-1.2,
        rain_all_above_c=1.8,
        region_profile="australian_alps",
        month_order=["Jun", "Jul", "Aug", "Sep", "Oct"],
        reported_total_cm=190.0,
    ),
    *BASE_RESORTS,
]


def fetch_open_meteo_daily(config: ResortConfig, elevation_m: int) -> pd.DataFrame:
    response = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": config.lat,
            "longitude": config.lon,
            "start_date": config.season_start,
            "end_date": config.season_end,
            "daily": "snowfall_sum,precipitation_sum,temperature_2m_mean,relative_humidity_2m_mean,snow_depth_max",
            "timezone": config.timezone,
            "cell_selection": "land",
            "elevation": elevation_m,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if "daily" not in payload:
        raise RuntimeError(f"Open-Meteo response missing daily data for {config.name}: {payload}")

    df = pd.DataFrame(payload["daily"])
    df["date"] = pd.to_datetime(df["time"])
    for col in ["snowfall_sum", "precipitation_sum", "temperature_2m_mean", "relative_humidity_2m_mean", "snow_depth_max"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def fetch_open_meteo_hourly(config: ResortConfig, elevation_m: int) -> pd.DataFrame:
    response = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": config.lat,
            "longitude": config.lon,
            "start_date": config.season_start,
            "end_date": config.season_end,
            "hourly": "temperature_2m,relative_humidity_2m,freezing_level_height,precipitation,rain,snowfall,snow_depth",
            "timezone": config.timezone,
            "cell_selection": "land",
            "elevation": elevation_m,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if "hourly" not in payload:
        raise RuntimeError(f"Open-Meteo hourly response missing data for {config.name}: {payload}")

    df = pd.DataFrame(payload["hourly"])
    df["datetime"] = pd.to_datetime(df["time"])
    for col in ["temperature_2m", "relative_humidity_2m", "freezing_level_height", "precipitation", "rain", "snowfall", "snow_depth"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def snow_fraction(temp_c: float, config: ResortConfig) -> float:
    if pd.isna(temp_c):
        return 0.0
    if temp_c <= config.snow_full_below_c:
        return 1.0
    if temp_c >= config.rain_all_above_c:
        return 0.0
    span = config.rain_all_above_c - config.snow_full_below_c
    return max(0.0, min(1.0, (config.rain_all_above_c - temp_c) / span))


def snow_to_liquid_ratio(temp_c: float, humidity_pct: float, config: ResortConfig) -> float:
    if pd.isna(temp_c):
        return 10.0

    if config.region_profile == "japan_maritime_powder":
        if temp_c <= -10.0:
            base_ratio = 24.0
        elif temp_c <= -8.0:
            base_ratio = 22.0
        elif temp_c <= -6.0:
            base_ratio = 19.0
        elif temp_c <= -4.0:
            base_ratio = 17.0
        elif temp_c <= -2.0:
            base_ratio = 14.0
        elif temp_c <= 0.0:
            base_ratio = 12.0
        elif temp_c <= 1.5:
            base_ratio = 10.0
        else:
            base_ratio = 8.0
    elif config.region_profile == "australian_alps":
        # Australian Alps: frequent marginal-temp snow with compact, wind-affected settlement.
        if temp_c <= -8.0:
            base_ratio = 12.5
        elif temp_c <= -5.0:
            base_ratio = 10.5
        elif temp_c <= -2.0:
            base_ratio = 8.8
        elif temp_c <= 0.0:
            base_ratio = 6.8
        elif temp_c <= 1.5:
            base_ratio = 4.8
        else:
            base_ratio = 3.4
    else:
        # NZ maritime alpine: slightly denser and warmer snow, so keep conversion conservative.
        if temp_c <= -8.0:
            base_ratio = 13.0
        elif temp_c <= -5.0:
            base_ratio = 11.0
        elif temp_c <= -2.0:
            base_ratio = 9.0
        elif temp_c <= 0.0:
            base_ratio = 7.0
        elif temp_c <= 1.5:
            base_ratio = 5.0
        else:
            base_ratio = 3.5

    humidity_adjust = 1.0
    if humidity_pct >= 90:
        humidity_adjust = 0.88
    elif humidity_pct >= 80:
        humidity_adjust = 0.94
    elif humidity_pct <= 60:
        humidity_adjust = 1.08

    return base_ratio * humidity_adjust


def build_method_frame(config: ResortConfig) -> pd.DataFrame:
    resort_df = fetch_open_meteo_daily(config, config.resort_elevation_m)

    temp_adj_c = resort_df["temperature_2m_mean"]
    humidity_pct = resort_df["relative_humidity_2m_mean"]
    precip_adj_mm = resort_df["precipitation_sum"].clip(lower=0.0)
    snow_frac = temp_adj_c.apply(lambda value: snow_fraction(value, config))
    slr = pd.Series(
        [snow_to_liquid_ratio(temp, humidity, config) for temp, humidity in zip(temp_adj_c, humidity_pct)],
        index=resort_df.index,
    )
    direct_model_cm = resort_df["snowfall_sum"].clip(lower=0.0)
    precip_temp_model_cm = precip_adj_mm * snow_frac * slr / 10.0

    depth_m = resort_df["snow_depth_max"].clip(lower=0.0)
    persistent_depth_m = depth_m.rolling(7, min_periods=1).mean()
    depth_gain_cm = depth_m.diff() * 100.0
    snowpack_gain_proxy_cm = depth_gain_cm.where(depth_gain_cm > 0.0) * config.snowpack_gain_factor
    depth_proxy = np.nan_to_num(snowpack_gain_proxy_cm.to_numpy(), nan=0.0)

    if config.region_profile == "japan_maritime_powder":
        rain_sum_mm = (precip_adj_mm - (resort_df["snowfall_sum"].clip(lower=0.0) / 7.0)).clip(lower=0.0)
        rain_ratio = (rain_sum_mm / precip_adj_mm.where(precip_adj_mm > 0, np.nan)).fillna(0.0).clip(0.0, 1.0)
        month_num = resort_df["date"].dt.month
        cold_bonus = np.where(temp_adj_c <= -5.0, 1.10, np.where(temp_adj_c <= -2.0, 1.02, np.where(temp_adj_c <= 0.5, 0.82, 0.22)))
        rain_penalty = np.where(snow_frac < 0.20, 0.24, np.where(snow_frac < 0.50, 0.52, np.where(snow_frac < 0.80, 0.80, 1.0)))
        rain_ratio_penalty = (1.02 - 0.45 * rain_ratio).clip(lower=0.72, upper=1.02)
        storm_duration_bonus = np.where(
            (resort_df["precipitation_sum"] >= 12.0) & (temp_adj_c <= -1.5),
            1.12,
            np.where((resort_df["precipitation_sum"] >= 8.0) & (temp_adj_c <= 0.0), 1.05, 1.0),
        )
        shoulder_season_penalty = np.where(
            month_num.isin([11, 4, 5]),
            np.where(temp_adj_c <= -3.0, 0.96, np.where(temp_adj_c <= -1.0, 0.82, 0.62)),
            1.0,
        )
        deep_pack_bonus = np.where(
            month_num.isin([12, 1, 2]) & (persistent_depth_m >= 1.2),
            1.18,
            np.where(month_num.isin([12, 1, 2]) & (persistent_depth_m >= 0.8), 1.10, 1.0),
        )
        cold_storm_signal_bonus = np.where(
            (resort_df["snowfall_sum"] >= 8.0) & (temp_adj_c <= -2.0),
            1.05,
            1.0,
        )
        warm_mixed_storm_penalty = np.where(
            (resort_df["precipitation_sum"] >= 12.0) & (temp_adj_c > 0.0),
            0.84,
            np.where((resort_df["precipitation_sum"] >= 8.0) & (temp_adj_c > 0.5), 0.92, 1.0),
        )
        pack_bonus = 0.58 + np.clip(depth_m, 0.0, 2.0) * 0.12 + np.clip(persistent_depth_m, 0.0, 2.0) * 0.07
        season_bonus = np.exp(0.48 * np.clip(float(depth_m.max()) - 0.8, 0.0, 0.9))
        enhanced_precip_temp_cm = (
            precip_temp_model_cm.to_numpy()
            * cold_bonus
            * rain_penalty
            * rain_ratio_penalty.to_numpy()
            * storm_duration_bonus
            * shoulder_season_penalty
            * deep_pack_bonus
            * cold_storm_signal_bonus
            * warm_mixed_storm_penalty
            * pack_bonus
            * season_bonus
        ) + depth_proxy * 0.05
    elif config.region_profile == "australian_alps":
        cold_bonus = np.where(temp_adj_c <= -4.5, 1.05, np.where(temp_adj_c <= -1.5, 1.02, np.where(temp_adj_c <= 0.75, 1.00, 0.78)))
        rain_penalty = np.where(snow_frac < 0.20, 0.36, np.where(snow_frac < 0.50, 0.68, np.where(snow_frac < 0.80, 0.90, 1.0)))
        pack_bonus = 0.97 + np.clip(depth_m, 0.0, 1.2) * 0.09 + np.clip(persistent_depth_m, 0.0, 1.2) * 0.05
        enhanced_precip_temp_cm = precip_temp_model_cm.to_numpy() * cold_bonus * rain_penalty * pack_bonus + depth_proxy * 0.025
    else:
        cold_bonus = np.where(temp_adj_c <= -5.0, 1.06, np.where(temp_adj_c <= -2.0, 1.00, np.where(temp_adj_c <= 0.0, 0.86, 0.40)))
        rain_penalty = np.where(snow_frac < 0.20, 0.10, np.where(snow_frac < 0.50, 0.38, np.where(snow_frac < 0.80, 0.72, 1.0)))
        pack_bonus = 0.74 + np.clip(depth_m, 0.0, 1.5) * 0.10 + np.clip(persistent_depth_m, 0.0, 1.5) * 0.06
        enhanced_precip_temp_cm = precip_temp_model_cm.to_numpy() * cold_bonus * rain_penalty * pack_bonus + depth_proxy * 0.03

    consensus_cm = depth_proxy * 0.40 + precip_temp_model_cm.to_numpy() * 0.60

    base_frame = pd.DataFrame(
        {
            "resort": config.name,
            "season_label": config.season_label,
            "resort_close_date": config.resort_close_date,
            "reported_total_cm": config.reported_total_cm,
            "date": resort_df["date"],
            "day_of_season": np.arange(1, len(resort_df) + 1),
            DIRECT_METHOD: direct_model_cm.to_numpy(),
            PT_METHOD: precip_temp_model_cm.to_numpy(),
            ENHANCED_PT_METHOD: enhanced_precip_temp_cm,
            CONSENSUS_METHOD: consensus_cm,
            "snowpack_gain_proxy_cm": np.nan_to_num(snowpack_gain_proxy_cm.to_numpy(), nan=0.0),
        }
    )

    long_frame = base_frame.melt(
        id_vars=["resort", "season_label", "resort_close_date", "reported_total_cm", "date", "day_of_season"],
        value_vars=[DIRECT_METHOD, PT_METHOD, ENHANCED_PT_METHOD, CONSENSUS_METHOD],
        var_name="method",
        value_name="daily_snowfall_cm",
    )
    long_frame["daily_snowfall_cm"] = long_frame["daily_snowfall_cm"].clip(lower=0.0)
    long_frame["cumulative_snowfall_cm"] = long_frame.groupby(["resort", "method"])["daily_snowfall_cm"].cumsum()
    return long_frame


def month_ticks_for_resort(df: pd.DataFrame) -> tuple[list[int], list[str]]:
    month_map = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
    tick_positions = []
    tick_labels = []
    for month in df["date"].dt.month.drop_duplicates():
        month_rows = df[df["date"].dt.month == month]
        if month_rows.empty:
            continue
        tick_positions.append(int(month_rows["day_of_season"].min()))
        tick_labels.append(month_map[int(month)])
    return tick_positions, tick_labels


def plot_methods(df: pd.DataFrame, output_path: str) -> None:
    panels = df[["resort", "season_label"]].drop_duplicates().reset_index(drop=True)
    n_panels = len(panels)
    ncols = 2 if n_panels > 1 else 1
    nrows = int(np.ceil(n_panels / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(17, 6.8 * nrows), constrained_layout=True)
    axes_flat = np.atleast_1d(axes).flatten()

    for ax, (_, panel) in zip(axes_flat, panels.iterrows()):
        resort_name = panel["resort"]
        season_label = panel["season_label"]
        panel_df = df[(df["resort"] == resort_name) & (df["season_label"] == season_label)].copy()
        tick_positions, tick_labels = month_ticks_for_resort(panel_df)

        close_date = pd.to_datetime(panel_df["resort_close_date"].iloc[0])
        season_start_date = panel_df["date"].min()
        season_end_day = int((close_date - season_start_date).days) + 1
        reported_total_cm = panel_df["reported_total_cm"].dropna().iloc[0] if panel_df["reported_total_cm"].notna().any() else None

        if reported_total_cm is not None:
            band_half_height = max(10.0, float(reported_total_cm) * 0.035)
            x_band = np.array([1, int(panel_df["day_of_season"].max())])
            ax.fill_between(
                x_band,
                float(reported_total_cm) - band_half_height,
                float(reported_total_cm) + band_half_height,
                color="#ffd84d",
                alpha=0.22,
                zorder=0,
            )
            ax.plot(
                x_band,
                [float(reported_total_cm), float(reported_total_cm)],
                color="#d4aa00",
                linewidth=2.0,
                alpha=0.85,
                linestyle="-",
                label="Reported season total",
            )

        for method in [DIRECT_METHOD, PT_METHOD, ENHANCED_PT_METHOD, CONSENSUS_METHOD]:
            method_df = panel_df[panel_df["method"] == method]
            ax.plot(
                method_df["day_of_season"],
                method_df["cumulative_snowfall_cm"],
                color=METHOD_COLORS[method],
                linewidth=2.4 if method == CONSENSUS_METHOD else 2.0,
                alpha=0.97,
                label=method,
            )
            last_row = method_df.iloc[-1]
            ax.text(
                float(last_row["day_of_season"]) + 1.2,
                float(last_row["cumulative_snowfall_cm"]),
                f"{last_row['cumulative_snowfall_cm']:.0f} cm",
                color=METHOD_COLORS[method],
                fontsize=8.5,
                fontweight="bold",
            )

        y_max = float(panel_df["cumulative_snowfall_cm"].max()) if not panel_df.empty else 0.0
        if 1 <= season_end_day <= int(panel_df["day_of_season"].max()):
            ax.axvline(season_end_day, color="#555555", linestyle="--", linewidth=1.2, alpha=0.8)
            ax.text(
                season_end_day - 1.5,
                y_max * 0.98 if y_max else 0.0,
                f"Resort closes\n{close_date.strftime('%b %d')}",
                color="#555555",
                fontsize=8,
                ha="right",
                va="top",
            )

        if reported_total_cm is not None:
            ax.text(
                max(2, int(panel_df["day_of_season"].max()) - 2),
                float(reported_total_cm),
                f" Reported total: {float(reported_total_cm):.0f} cm",
                color="#8c6d00",
                fontsize=8.5,
                fontweight="bold",
                ha="right",
                va="bottom",
            )

        ax.set_xlim(1, int(panel_df["day_of_season"].max()))

        ax.set_title(f"{resort_name} | Season {season_label}", fontsize=12.5, fontweight="bold")
        ax.set_xlabel("Month")
        ax.set_ylabel("Cumulative snowfall (cm)")
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(tick_labels)
        ax.grid(True, axis="y", linestyle="--", alpha=0.35)
        ax.legend(frameon=False, loc="upper left")

    for ax in axes_flat[len(panels):]:
        ax.set_visible(False)

    fig.suptitle(
        "Open-Meteo Seasonal Snowfall Comparison by Method\n"
        "Appi, Treble Cone, The Remarkables, and Perisher | direct + modeled snowfall, reported total highlighted",
        fontsize=16,
        fontweight="bold",
    )
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    frames = [build_method_frame(config) for config in RESORTS]
    result_df = pd.concat(frames, ignore_index=True)

    totals = (
        result_df.groupby(["resort", "season_label", "method"], as_index=False)
        .agg({"daily_snowfall_cm": "sum", "cumulative_snowfall_cm": "max"})
        .sort_values(["resort", "method"])
    )

    chart_path = os.path.join(CHARTS_DIR, "openmeteo_multimethod_cumulative_multiseason.png")
    plot_methods(result_df, chart_path)

    print(f"Saved chart: {chart_path}")
    print("\nSeason totals by method (cm):")
    print(
        totals[["resort", "season_label", "method", "daily_snowfall_cm"]].to_string(
            index=False, formatters={"daily_snowfall_cm": "{:.1f}".format}
        )
    )


if __name__ == "__main__":
    main()
