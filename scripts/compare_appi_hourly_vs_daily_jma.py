#!/usr/bin/env python3
"""One-off daily JMA precipitation-versus-snowfall diagnostics for Appi Kogen.

This script is intentionally self-contained so it can be deleted easily after use.
It fetches raw JMA daily data directly, applies the same snowfall-phase logic used
in the dbt base model, and summarizes how precipitation and temperature drive the
resort snowfall estimate versus what the lower station actually observed.
"""

from __future__ import annotations

import hashlib
import re
import time
from datetime import date
from io import StringIO
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests

START_SEASON_YEAR = 2020
END_SEASON_YEAR = 2026
WINTER_MONTHS = (11, 12, 1, 2, 3, 4)

# Current pipeline-matched station for Appi Kogen.
RESORT_NAME = "Appi Kogen Ski Resort"
RESORT_ELEVATION_M = 1180.0
STATION_NAME = "Iwate Matsuo"
STATION_PREC_NO = "33"
STATION_BLOCK_NO = "0214"
STATION_ELEVATION_M = 275.0
STATION_DISTANCE_KM = 10.7

SNOW_LAPSE_RATE_C_PER_KM = 6.5
SNOW_FULL_BELOW_C = -1.5
RAIN_ALL_ABOVE_C = 3.0

ROOT_DIR = Path(__file__).resolve().parents[1]
CHARTS_DIR = ROOT_DIR / "charts"
REQUEST_CACHE_DIR = ROOT_DIR / "request_cache"
CHARTS_DIR.mkdir(exist_ok=True)
REQUEST_CACHE_DIR.mkdir(exist_ok=True)


def parse_jma_number(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (float, np.floating)) and pd.isna(value):
        return None
    text = str(value).replace(",", "").strip()
    if text in {"///", "--", "×", "", "nan"}:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    return float(match.group()) if match else None


def infer_season_year(d: pd.Timestamp) -> int:
    return d.year + 1 if d.month in (11, 12) else d.year


def season_month_pairs(season_year: int) -> list[tuple[int, int]]:
    return [
        (season_year - 1, 11),
        (season_year - 1, 12),
        (season_year, 1),
        (season_year, 2),
        (season_year, 3),
        (season_year, 4),
    ]


def season_day_list(season_year: int) -> list[date]:
    start = pd.Timestamp(season_year - 1, 11, 1)
    end = pd.Timestamp(season_year, 4, 30)
    latest_available = pd.Timestamp.today().normalize() - pd.Timedelta(days=2)
    end = min(end, latest_available)
    return [ts.date() for ts in pd.date_range(start, end, freq="D")]


def cache_path_for_url(url: str) -> Path:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return REQUEST_CACHE_DIR / f"jma_{digest}.html"


def fetch_html(session: requests.Session, url: str) -> str:
    cache_path = cache_path_for_url(url)
    if cache_path.exists():
        return cache_path.read_text(encoding="utf-8")

    response = session.get(
        url,
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0 AppiDailySnowDiagnostics/1.0"},
    )
    response.raise_for_status()
    html = response.text
    cache_path.write_text(html, encoding="utf-8")
    return html


def find_column(columns: list[str], required_terms: list[str], excluded_terms: list[str] | None = None) -> str | None:
    excluded_terms = excluded_terms or []
    for column in columns:
        if all(term in column for term in required_terms) and not any(term in column for term in excluded_terms):
            return column
    return None


def fetch_daily_month(session: requests.Session, year: int, month: int) -> pd.DataFrame:
    html = None
    for endpoint in ("daily_a1.php", "daily_s1.php"):
        url = (
            f"https://www.data.jma.go.jp/stats/etrn/view/{endpoint}"
            f"?prec_no={STATION_PREC_NO}&block_no={STATION_BLOCK_NO}&year={year}&month={month}&day=&view="
        )
        candidate_html = fetch_html(session, url)
        if "ページを表示することが出来ませんでした" not in candidate_html:
            html = candidate_html
            break

    if html is None:
        raise RuntimeError(f"Daily JMA page failed for {year}-{month:02d}")

    tables = pd.read_html(StringIO(html), flavor="lxml")
    if not tables:
        raise RuntimeError(f"No daily JMA table found for {year}-{month:02d}")

    table = tables[0]
    table.columns = [
        " ".join(str(level) for level in col if str(level) != "nan").strip()
        for col in table.columns
    ]
    columns = list(table.columns)

    day_col = columns[0]
    precip_col = find_column(columns, ["降水量", "合計"])
    temp_mean_col = find_column(columns, ["気温", "平均"])
    snowfall_col = find_column(columns, ["降雪", "合計"]) or find_column(columns, ["雪", "降雪", "合計"])
    snow_depth_col = find_column(columns, ["最深積雪"])

    if precip_col is None or temp_mean_col is None:
        raise RuntimeError(f"Expected daily JMA precip/temp columns not found for {year}-{month:02d}")

    out = pd.DataFrame({"day": pd.to_numeric(table[day_col], errors="coerce")})
    out = out[out["day"].notna()].copy()
    out["date"] = pd.to_datetime({"year": year, "month": month, "day": out["day"].astype(int)})

    def parsed(column_name: str | None, default: float = np.nan) -> pd.Series:
        if column_name is None:
            return pd.Series(default, index=out.index, dtype="float64")
        vals = table.loc[out.index, column_name].map(parse_jma_number)
        return pd.to_numeric(vals, errors="coerce").astype("float64")

    out["precipitation_mm"] = parsed(precip_col, 0.0).fillna(0.0)
    out["temperature_mean_c"] = parsed(temp_mean_col, np.nan)
    out["station_snowfall_cm"] = parsed(snowfall_col, 0.0).fillna(0.0)
    out["station_snow_depth_cm"] = parsed(snow_depth_col, 0.0).fillna(0.0)
    out["season_year"] = out["date"].map(infer_season_year)
    return out[["date", "season_year", "precipitation_mm", "temperature_mean_c", "station_snowfall_cm", "station_snow_depth_cm"]]


def snow_fraction_and_ratio(adjusted_temp: pd.Series | np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    snow_fraction = np.where(
        adjusted_temp <= SNOW_FULL_BELOW_C,
        1.0,
        np.where(
            adjusted_temp >= RAIN_ALL_ABOVE_C,
            0.0,
            np.clip((RAIN_ALL_ABOVE_C - adjusted_temp) / (RAIN_ALL_ABOVE_C - SNOW_FULL_BELOW_C), 0.0, 1.0),
        ),
    )
    snow_liquid_ratio = np.where(
        adjusted_temp <= -8.0,
        22.0,
        np.where(
            adjusted_temp <= -5.0,
            19.0,
            np.where(
                adjusted_temp <= -2.0,
                14.0,
                np.where(adjusted_temp <= 0.0, 12.0, np.where(adjusted_temp <= 1.5, 10.0, 8.0)),
            ),
        ),
    )
    return snow_fraction, snow_liquid_ratio


def model_daily(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    elevation_delta_km = max(0.0, RESORT_ELEVATION_M - STATION_ELEVATION_M) / 1000.0
    out["adjusted_temperature_c"] = out["temperature_mean_c"] - SNOW_LAPSE_RATE_C_PER_KM * elevation_delta_km
    snow_fraction, snow_liquid_ratio = snow_fraction_and_ratio(out["adjusted_temperature_c"])
    out["snow_fraction"] = snow_fraction.astype(float)
    out["snow_liquid_ratio"] = snow_liquid_ratio.astype(float)
    out["modeled_snowfall_cm"] = out["precipitation_mm"] * out["snow_fraction"] * out["snow_liquid_ratio"] / 10.0
    return out


def fetch_all_daily(session: requests.Session) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for season_year in range(START_SEASON_YEAR, END_SEASON_YEAR + 1):
        for year, month in season_month_pairs(season_year):
            frames.append(fetch_daily_month(session, year, month))
            time.sleep(0.03)
    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["date"]).sort_values("date")
    return df[df["season_year"].between(START_SEASON_YEAR, END_SEASON_YEAR)].copy()


def build_summary(daily_modeled: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, float | int]] = []

    for season_year, group in daily_modeled.groupby("season_year"):
        season_year_value = pd.to_numeric(pd.Series([season_year]), errors="coerce").iloc[0]
        precip = group["precipitation_mm"].fillna(0.0)
        precip_total = float(precip.sum())
        active_precip = precip > 0

        precip_weighted_snow_fraction = (
            float(np.average(group.loc[active_precip, "snow_fraction"], weights=precip.loc[active_precip]))
            if active_precip.any()
            else np.nan
        )
        precip_weighted_slr = (
            float(np.average(group.loc[active_precip, "snow_liquid_ratio"], weights=precip.loc[active_precip]))
            if active_precip.any()
            else np.nan
        )

        rows.append(
            {
                "season_year": season_year_value,
                "model_snow_cm": float(group["modeled_snowfall_cm"].sum()),
                "raw_station_snow_cm": float(group["station_snowfall_cm"].sum()),
                "station_precip_mm": precip_total,
                "avg_station_temp_c": float(group["temperature_mean_c"].mean()),
                "avg_adjusted_temp_c": float(group["adjusted_temperature_c"].mean()),
                "precip_weighted_snow_fraction": precip_weighted_snow_fraction,
                "precip_weighted_slr": precip_weighted_slr,
                "station_snow_days": int((group["station_snowfall_cm"] > 0).sum()),
                "station_snow_but_not_full_snow_days": int(
                    ((group["station_snowfall_cm"] > 0) & (group["snow_fraction"] < 0.999)).sum()
                ),
                "station_snow_while_model_above_freezing_days": int(
                    ((group["station_snowfall_cm"] > 0) & (group["adjusted_temperature_c"] > 0.0)).sum()
                ),
            }
        )

    return pd.DataFrame(rows).sort_values("season_year")


def save_chart(summary: pd.DataFrame) -> Path:
    labels = [f"{y - 1}-{str(y)[-2:]}" for y in summary["season_year"]]
    x = np.arange(len(labels))

    fig, ax1 = plt.subplots(figsize=(12, 6), constrained_layout=True)
    line1 = ax1.plot(x, summary["model_snow_cm"], marker="o", linewidth=2.3, color="tab:blue", label="Modeled resort snowfall")
    line2 = ax1.plot(x, summary["raw_station_snow_cm"], marker="o", linewidth=1.8, linestyle="--", color="tab:green", label="Raw station snowfall")
    ax1.set_xlabel("Season")
    ax1.set_ylabel("Snowfall (cm)")
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels)
    ax1.grid(True, axis="y", linestyle="--", alpha=0.3)

    ax2 = ax1.twinx()
    bars = ax2.bar(x, summary["station_precip_mm"], width=0.52, alpha=0.18, color="tab:purple", label="Station precipitation")
    ax2.set_ylabel("Precipitation (mm)")

    handles = line1 + line2 + [bars]
    labels_for_legend = [str(h.get_label()) for h in handles]
    ax1.legend(handles, labels_for_legend, frameon=False, loc="upper right")

    ax1.set_title(
        "Appi Kogen: JMA Daily Precipitation vs Snowfall Diagnostics\n"
        f"Station {STATION_NAME} ({STATION_PREC_NO}-{STATION_BLOCK_NO}), {STATION_DISTANCE_KM:.1f} km away"
    )

    chart_path = CHARTS_DIR / "appi_daily_precip_vs_snow_diagnostics.png"
    fig.savefig(chart_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    return chart_path


def main() -> None:
    session = requests.Session()
    print(f"Comparing daily JMA precipitation and snowfall diagnostics for {RESORT_NAME}")
    print(f"Using station: {STATION_NAME} ({STATION_PREC_NO}-{STATION_BLOCK_NO}), {STATION_DISTANCE_KM:.1f} km away")
    print(f"Resort elevation {RESORT_ELEVATION_M:.0f} m | Station elevation {STATION_ELEVATION_M:.0f} m")
    print()

    daily_raw = fetch_all_daily(session)
    daily_modeled = model_daily(daily_raw)
    summary = build_summary(daily_modeled)

    csv_path = CHARTS_DIR / "appi_daily_precip_vs_snow_diagnostics.csv"
    summary.round(2).to_csv(csv_path, index=False)
    chart_path = save_chart(summary)

    avg_partial_days = summary["station_snow_but_not_full_snow_days"].mean()
    avg_above_freezing_days = summary["station_snow_while_model_above_freezing_days"].mean()
    season_year_series = pd.to_numeric(summary["season_year"], errors="coerce")
    mismatch_series = pd.to_numeric(summary["station_snow_but_not_full_snow_days"], errors="coerce")
    max_idx = int(mismatch_series.idxmax())
    max_row = summary.loc[max_idx]
    max_season_year = int(season_year_series.loc[max_idx])
    max_mismatch_days = int(mismatch_series.loc[max_idx])

    print(summary.to_string(index=False, formatters={
        "model_snow_cm": "{:.1f}".format,
        "raw_station_snow_cm": "{:.1f}".format,
        "station_precip_mm": "{:.1f}".format,
        "avg_station_temp_c": "{:.2f}".format,
        "avg_adjusted_temp_c": "{:.2f}".format,
        "precip_weighted_snow_fraction": "{:.3f}".format,
        "precip_weighted_slr": "{:.2f}".format,
    }))
    print()
    print(f"Average station-snow days not treated as full snow by the model: {avg_partial_days:.1f}")
    print(f"Average station-snow days with adjusted model temp above freezing: {avg_above_freezing_days:.1f}")
    print(
        "Largest possible lower-level snow mismatch season: "
        f"{max_season_year - 1}-{str(max_season_year)[-2:]} "
        f"({max_mismatch_days} day(s))"
    )
    print(f"Saved CSV: {csv_path}")
    print(f"Saved chart: {chart_path}")


if __name__ == "__main__":
    main()
