import math
import os

import duckdb
import pandas as pd
from dotenv import load_dotenv
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


def season_month_ticks(df: pd.DataFrame):
    month_name_map = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
    }
    month_order = [11, 12, 1, 2, 3, 4]
    tick_days = []
    tick_labels = []

    for month in month_order:
        month_rows = df[df["month_col"] == month]
        if month_rows.empty:
            continue
        tick_days.append(int(month_rows["day_of_season"].min()))
        tick_labels.append(month_name_map[month])

    return tick_days, tick_labels


df = con.execute(
    """
    with latest_season as (
        select greatest(
            coalesce((select max(year_col) from camonairflow.public_analysis.analysis_cumulative_snowfall where country = 'JP'), 0),
            coalesce((select max(year_col) from camonairflow.public_analysis.analysis_cumulative_snowfall_om_jp where country = 'JP'), 0)
        ) as latest_year
    )
    , jma as (
        select
            ski_field,
            country,
            year_col,
            datecol,
            day_of_season,
            cumulative_snowfall_cm as jma_new_cumulative_snowfall_cm
        from camonairflow.public_analysis.analysis_cumulative_snowfall
        where country = 'JP'
    )
    , om as (
        select
            ski_field,
            country,
            year_col,
            datecol,
            day_of_season,
            om_direct_cumulative_snowfall_cm,
            depth_precip_blend_cumulative_snowfall_cm as om_depth_precip_blend_cumulative_snowfall_cm
        from camonairflow.public_analysis.analysis_cumulative_snowfall_om_jp
        where country = 'JP'
    )
    select
        coalesce(j.ski_field, o.ski_field) as ski_field,
        coalesce(j.country, o.country) as country,
        coalesce(j.year_col, o.year_col) as year_col,
        coalesce(j.datecol, o.datecol) as datecol,
        coalesce(j.day_of_season, o.day_of_season) as day_of_season,
        coalesce(j.jma_new_cumulative_snowfall_cm, 0.0) as jma_new_cumulative_snowfall_cm,
        coalesce(o.om_direct_cumulative_snowfall_cm, 0.0) as om_direct_cumulative_snowfall_cm,
        coalesce(o.om_depth_precip_blend_cumulative_snowfall_cm, 0.0) as om_depth_precip_blend_cumulative_snowfall_cm
    from jma as j
    full outer join om as o
        on j.ski_field = o.ski_field
        and j.country = o.country
        and j.year_col = o.year_col
        and j.datecol = o.datecol
    where coalesce(j.year_col, o.year_col) = (select latest_year from latest_season)
    order by country, ski_field, datecol
    """
).df()

if df.empty:
    raise ValueError("No current-season snowfall comparison data returned.")

df["datecol"] = pd.to_datetime(df["datecol"])
df["month_col"] = df["datecol"].dt.month

model_columns = [
    "jma_new_cumulative_snowfall_cm",
    "om_direct_cumulative_snowfall_cm",
    "om_depth_precip_blend_cumulative_snowfall_cm",
]
model_name_map = {
    "jma_new_cumulative_snowfall_cm": "JMA Wet-bulb",
    "om_direct_cumulative_snowfall_cm": "OM Direct",
    "om_depth_precip_blend_cumulative_snowfall_cm": "OM Depth Blend",
}
model_order = [
    "JMA Wet-bulb",
    "OM Direct",
    "OM Depth Blend",
]

plot_df = df.melt(
    id_vars=["ski_field", "country", "year_col", "datecol", "day_of_season", "month_col"],
    value_vars=model_columns,
    var_name="model_version",
    value_name="cumulative_cm",
)
plot_df["model_version"] = plot_df["model_version"].map(model_name_map)
plot_df["model_version"] = pd.Categorical(plot_df["model_version"], categories=model_order, ordered=True)

summary = (
    df.sort_values(["ski_field", "datecol"])
    .groupby(["country", "ski_field"], as_index=False)
    .last()
)
facet_bounds = (
    df.groupby(["country", "ski_field"], as_index=False)
    .agg(
        min_day=("day_of_season", "min"),
        max_day=("day_of_season", "max"),
        max_jma_new=("jma_new_cumulative_snowfall_cm", "max"),
        max_om_direct=("om_direct_cumulative_snowfall_cm", "max"),
        max_om_depth=("om_depth_precip_blend_cumulative_snowfall_cm", "max"),
    )
)
summary = summary.merge(facet_bounds, on=["country", "ski_field"], how="left")
summary["label_x"] = summary["min_day"] + ((summary["max_day"] - summary["min_day"]).clip(lower=1) * 0.03)
summary["label_y"] = summary[[
    "max_jma_new",
    "max_om_direct",
    "max_om_depth",
]].max(axis=1) * 0.96

output_dir = "/workspaces/CamOnAirFlow/charts"
os.makedirs(output_dir, exist_ok=True)

for country in sorted(df["country"].dropna().unique()):
    country_df = df[df["country"] == country].copy()
    if country_df.empty:
        continue

    country_plot_df = plot_df[plot_df["country"] == country].copy()
    country_summary = summary[summary["country"] == country].copy()
    month_ticks, month_labels = season_month_ticks(country_df)
    ncol, fig_width, fig_height = get_facet_layout(country_df["ski_field"].nunique())

    p = (
        ggplot(
            country_plot_df,
            aes(x="day_of_season", y="cumulative_cm", color="model_version", linetype="model_version"),
        )
        + geom_line(size=1.55, alpha=0.95)
        + scale_color_manual(
            values={
                "JMA Wet-bulb": "#d62728",
                "OM Direct": "#1f77b4",
                "OM Depth Blend": "#9467bd",
            },
            breaks=model_order,
        )
        + scale_linetype_manual(
            values={
                "JMA Wet-bulb": "solid",
                "OM Direct": "dotted",
                "OM Depth Blend": "solid",
            },
            breaks=model_order,
        )
        + scale_x_continuous(breaks=month_ticks, labels=month_labels, expand=(0.01, 0))
        + facet_wrap("~ski_field", scales="free_y", ncol=ncol)
        + labs(
            title=f"Japan Snowfall Model Comparison — Current Season ({country})",
            subtitle="JMA wet-bulb vs Open-Meteo direct and calibrated depth blend",
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
    print(f"Saved chart: {out_path}")
