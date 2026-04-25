import math
import os
from datetime import date

import duckdb
import pandas as pd
from dotenv import load_dotenv
from matplotlib import pyplot as plt
from plotnine import (
    aes,
    element_blank,
    element_line,
    element_rect,
    element_text,
    facet_wrap,
    geom_line,
    ggplot,
    labs,
    scale_color_manual,
    scale_linetype_manual,
    scale_x_continuous,
    theme,
    theme_light,
)

from project_path import get_project_paths, set_dlt_env_vars

plt.switch_backend("Agg")


COUNTRY_LABELS = {
    "AU": "Australia",
    "JP": "Japan",
    "NZ": "New Zealand",
}

COUNTRY_MONTH_ORDER = {
    "AU": [6, 7, 8, 9, 10, 11],
    "JP": [11, 12, 1, 2, 3, 4],
    "NZ": [6, 7, 8, 9, 10, 11],
}

MODEL_DISPLAY_ORDER = [
    "ECMWF IFS",
    "UKMO Seamless",
    "ICON Seamless",
    "GEM Seamless",
    "CMA GRAPES Global",
    "JMA Seamless",
]

MODEL_PALETTE = [
    "#1f77b4",
    "#d62728",
    "#2ca02c",
    "#9467bd",
    "#ff7f0e",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#17becf",
    "#bcbd22",
]

MODEL_LINETYPES = [
    "solid",
    "dashed",
    "dotted",
    "dashdot",
    "solid",
    "dashed",
    "dotted",
    "dashdot",
    "solid",
    "dashed",
]


paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])
database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")

con = duckdb.connect(database_string)


def get_facet_layout(n_facets: int):
    if n_facets <= 4:
        ncol = 2
    else:
        ncol = 3

    nrow = max(1, math.ceil(n_facets / ncol))
    width = 14 if ncol == 2 else 15
    height = min(18, max(10, nrow * 3.9 + 2.4))
    return ncol, width, height


def get_target_season_year(country: str, today: date) -> int:
    if country in {"NZ", "AU"}:
        return today.year if 6 <= today.month <= 11 else today.year - 1
    return today.year + 1 if today.month in (11, 12) else today.year


def season_month_ticks(df: pd.DataFrame, country: str):
    month_name_map = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
    }
    month_order = COUNTRY_MONTH_ORDER.get(country, list(range(1, 13)))
    tick_days = []
    tick_labels = []

    for month in month_order:
        month_rows = df[df["month_col"] == month]
        if month_rows.empty:
            continue
        tick_days.append(int(month_rows["day_of_season"].min()))
        tick_labels.append(month_name_map[month])

    return tick_days, tick_labels


def get_model_order(present_models: list[str]) -> list[str]:
    return [model for model in MODEL_DISPLAY_ORDER if model in present_models]


def get_scale_maps(model_order: list[str]):
    color_map = {model: MODEL_PALETTE[i % len(MODEL_PALETTE)] for i, model in enumerate(model_order)}
    linetype_map = {model: MODEL_LINETYPES[i % len(MODEL_LINETYPES)] for i, model in enumerate(model_order)}
    return color_map, linetype_map


def fetch_cumulative_data(selected_countries: list[str]) -> pd.DataFrame:
    countries_sql = ", ".join(f"'{country}'" for country in selected_countries)
    models_sql = ", ".join(f"'{model}'" for model in MODEL_DISPLAY_ORDER)

    return con.execute(
        f"""
        select
            ski_field,
            country,
            year_col,
            month_col,
            datecol,
            day_of_season,
            facet_label,
            model_name,
            daily_snowfall_cm,
            cumulative_snowfall_cm
        from camonairflow.public_analysis.analysis_cumulative_snowfall
        where country in ({countries_sql})
          and model_name in ({models_sql})
        order by country, ski_field, year_col, model_name, datecol
        """
    ).df()


def main(selected_countries: list[str] | None = None):
    selected_countries = selected_countries or list(COUNTRY_LABELS.keys())
    df = fetch_cumulative_data(selected_countries)

    if df.empty:
        raise ValueError("No seasonal snowfall comparison data returned.")

    df["datecol"] = pd.to_datetime(df["datecol"])
    df["month_col"] = df["datecol"].dt.month

    output_dir = "/workspaces/CamOnAirFlow/charts"
    os.makedirs(output_dir, exist_ok=True)
    today = date.today()

    for country in selected_countries:
        country_df = df[df["country"] == country].copy()
        if country_df.empty:
            continue

        target_year = get_target_season_year(country, today)
        eligible_df = country_df[country_df["year_col"] <= target_year].copy()
        if eligible_df.empty:
            eligible_df = country_df.copy()

        latest_year = int(eligible_df["year_col"].max())
        country_plot_df = eligible_df[eligible_df["year_col"] == latest_year].copy()
        if country_plot_df.empty:
            continue

        month_ticks, month_labels = season_month_ticks(country_plot_df, country)
        ncol, fig_width, fig_height = get_facet_layout(country_plot_df["ski_field"].nunique())
        model_order = get_model_order(country_plot_df["model_name"].dropna().unique().tolist())
        color_map, linetype_map = get_scale_maps(model_order)
        country_plot_df["model_name"] = pd.Categorical(
            country_plot_df["model_name"],
            categories=model_order,
            ordered=True,
        )

        p = (
            ggplot(
                country_plot_df,
                aes(x="day_of_season", y="cumulative_snowfall_cm", color="model_name", linetype="model_name"),
            )
            + geom_line(size=1.25, alpha=0.95)
            + scale_color_manual(values=color_map, breaks=model_order)
            + scale_linetype_manual(values=linetype_map, breaks=model_order)
            + scale_x_continuous(breaks=month_ticks, labels=month_labels, expand=(0.01, 0))
            + facet_wrap("~ski_field", scales="free_y", ncol=ncol)
            + labs(
                title=f"{COUNTRY_LABELS.get(country, country)} Snowfall Model Comparison — Season {latest_year} ({country})",
                subtitle="Cumulative snowfall using model-specific Open-Meteo series",
                x="Month",
                y="Cumulative snowfall (cm)",
                color="Model",
                linetype="Model",
            )
            + theme_light(base_size=17)
            + theme(
                legend_position="top",
                legend_title=element_text(weight="bold", size=11),
                legend_text=element_text(size=10),
                axis_text_x=element_text(size=10, color="#444444"),
                axis_text_y=element_text(size=10, color="#444444"),
                axis_title_x=element_text(size=13, weight="bold"),
                axis_title_y=element_text(size=13, weight="bold"),
                plot_title=element_text(weight="bold", size=20, color="#111111"),
                plot_subtitle=element_text(size=11, color="#444444"),
                panel_spacing=0.10,
                strip_text_x=element_text(color="#111111", weight="bold", size=11),
                strip_background=element_rect(fill="#f0f0f0", color="#d0d0d0"),
                panel_grid_minor=element_blank(),
                panel_grid_major_x=element_line(color="#d9d9d9", size=0.4, linetype="dashed"),
                panel_grid_major_y=element_line(color="#ededed", size=0.35),
                plot_background=element_rect(fill="#ffffff", color="#ffffff"),
                panel_background=element_rect(fill="#fcfcfc", color="#e5e5e5"),
            )
        )

        out_path = os.path.join(output_dir, f"current_season_snow_model_compare_{country}.png")
        p.save(out_path, width=fig_width, height=fig_height, dpi=260, limitsize=False)
        plt.close("all")
        print(f"Saved chart: {out_path}")


if __name__ == "__main__":
    main()
