import math
import os

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
    geom_point,
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

MODEL_DISPLAY_ORDER = [
    "ERA5",
    "ECMWF IFS",
    "UKMO Seamless",
    "ICON Seamless",
    "GEM Seamless",
    "CMA GRAPES Global",
    "GFS Seamless",
    "Meteo-France Seamless",
    "JMA Seamless",
    "BOM ACCESS Global",
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
    height = min(22, max(10, nrow * 4.2 + 2.4))
    return ncol, width, height


def get_model_order(present_models: list[str]) -> list[str]:
    return [model for model in MODEL_DISPLAY_ORDER if model in present_models]


def get_scale_maps(model_order: list[str]):
    color_map = {model: MODEL_PALETTE[i % len(MODEL_PALETTE)] for i, model in enumerate(model_order)}
    linetype_map = {model: MODEL_LINETYPES[i % len(MODEL_LINETYPES)] for i, model in enumerate(model_order)}
    return color_map, linetype_map


def fetch_season_totals(selected_countries: list[str]) -> pd.DataFrame:
    countries_sql = ", ".join(f"'{c}'" for c in selected_countries)
    models_sql = ", ".join(f"'{m}'" for m in MODEL_DISPLAY_ORDER)

    return con.execute(
        f"""
        select
            ski_field,
            country,
            year_col,
            model_name,
            season_snowfall_cm,
            avg_temp_c
        from camonairflow.public_analysis.analysis_season_totals
        where country in ({countries_sql})
          and model_name in ({models_sql})
        order by country, ski_field, year_col, model_name
        """
    ).df()


def plot_country(
    country: str,
    country_df: pd.DataFrame,
    output_dir: str,
    metric: str = "season_snowfall_cm",
) -> None:
    metric_labels = {
        "season_snowfall_cm": ("Season Total Snowfall (cm)", "season_snowfall_cm"),
        "avg_temp_c": ("Season Avg Temperature (°C)", "avg_temp_c"),
    }
    y_label, col = metric_labels[metric]

    plot_df = country_df.copy()
    model_order = get_model_order(plot_df["model_name"].dropna().unique().tolist())
    color_map, linetype_map = get_scale_maps(model_order)
    plot_df["model_name"] = pd.Categorical(plot_df["model_name"], categories=model_order, ordered=True)

    all_years = sorted(plot_df["year_col"].unique())
    ncol, fig_width, fig_height = get_facet_layout(plot_df["ski_field"].nunique())

    metric_slug = metric.replace("_", "-")
    metric_title = {
        "season_snowfall_cm": "Total Snowfall",
        "avg_temp_c": "Avg Temperature",
    }[metric]

    p = (
        ggplot(
            plot_df,
            aes(x="year_col", y=col, color="model_name", linetype="model_name"),
        )
        + geom_line(size=1.1, alpha=0.90)
        + geom_point(size=1.6, alpha=0.85)
        + scale_color_manual(values=color_map, breaks=model_order)
        + scale_linetype_manual(values=linetype_map, breaks=model_order)
        + scale_x_continuous(
            breaks=all_years,
            labels=[str(y) for y in all_years],
            expand=(0.04, 0),
        )
        + facet_wrap("~ski_field", scales="free_y", ncol=ncol)
        + labs(
            title=f"{COUNTRY_LABELS.get(country, country)} — {metric_title} by Season ({country})",
            subtitle="Per-season model comparison using Open-Meteo series",
            x="Season",
            y=y_label,
            color="Model",
            linetype="Model",
        )
        + theme_light(base_size=17)
        + theme(
            legend_position="top",
            legend_title=element_text(weight="bold", size=11),
            legend_text=element_text(size=10),
            axis_text_x=element_text(size=9, color="#444444", angle=45, ha="right"),
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

    out_path = os.path.join(output_dir, f"season_totals_{metric_slug}_{country}.png")
    p.save(out_path, width=fig_width, height=fig_height, dpi=260, limitsize=False)
    plt.close("all")
    print(f"Saved chart: {out_path}")


def main(
    selected_countries: list[str] | None = None,
    metrics: list[str] | None = None,
) -> None:
    selected_countries = selected_countries or list(COUNTRY_LABELS.keys())
    metrics = metrics or ["season_snowfall_cm", "avg_temp_c"]

    df = fetch_season_totals(selected_countries)
    if df.empty:
        raise ValueError("No season totals data returned.")

    output_dir = "/workspaces/CamOnAirFlow/charts"
    os.makedirs(output_dir, exist_ok=True)

    for country in selected_countries:
        country_df = df[df["country"] == country].copy()
        if country_df.empty:
            continue
        for metric in metrics:
            plot_country(country, country_df, output_dir, metric=metric)


if __name__ == "__main__":
    main()
