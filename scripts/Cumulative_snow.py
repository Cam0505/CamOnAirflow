import duckdb
import os
import math
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
import pandas as pd
from plotnine import (
    ggplot, aes, geom_line, labs, facet_wrap, theme_light, theme,
    element_text, element_rect, element_line, scale_x_continuous,
    scale_color_manual, guides, guide_legend,
    geom_point, geom_text
)
import matplotlib.colors as mcolors
import numpy as np

# --- ENV, DuckDB connection ---
paths = get_project_paths()
set_dlt_env_vars(paths)
load_dotenv(dotenv_path=paths["ENV_FILE"])
database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")
con = duckdb.connect(database_string)

# --- Load Data ---
df = con.execute("""
    SELECT
        a.ski_field,
        a.country,
        a.year_col,
        a.datecol,
        a.day_of_season,
        a.facet_label,
        a.daily_snowfall_cm,
        a.cumulative_snowfall_cm
    FROM camonairflow.public_analysis.analysis_cumulative_snowfall a
    WHERE (a.model_name = 'ECMWF IFS' and country <> 'JP')
         OR (a.model_name = 'JMA Seamless' and country = 'JP')
      AND a.year_col >= 1990
""").df()

if df.empty:
    raise ValueError("No snowfall data returned from snowfall_cumulative_daily.")

df['datecol'] = pd.to_datetime(df['datecol'])
df['month_col'] = df['datecol'].dt.month


def get_facet_layout(n_facets):
    """Return (ncol, nrow, width, height) for readable country facet charts."""
    if n_facets <= 4:
        ncol = 2
    elif n_facets <= 9:
        ncol = 3
    else:
        ncol = 4

    nrow = max(1, math.ceil(n_facets / ncol))
    width = min(24, max(12, ncol * 4.8))
    height = min(24, max(8, nrow * 3.8 + 2.2))
    return ncol, nrow, width, height


def season_month_ticks_for_country(country_df):
    """Build month ticks/labels per country season (NH Nov-Apr, SH Jun-Nov)."""
    month_name_map = {
        1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
        7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
    }
    # Nov is shared by both hemispheres — detect via unambiguous core months.
    nh_core = {12, 1, 2, 3, 4}   # exclusively NH season
    sh_core = {6, 7, 8, 9, 10}   # exclusively SH season
    months_present = set(country_df['month_col'].dropna().astype(int).unique())

    if months_present & nh_core and not (months_present & sh_core):
        month_order = [11, 12, 1, 2, 3, 4]
    elif months_present & sh_core and not (months_present & nh_core):
        month_order = [6, 7, 8, 9, 10, 11]
    else:
        # Fallback when data contains both sets.
        month_order = [11, 12, 1, 2, 3, 4, 6, 7, 8, 9, 10]

    tick_days = []
    tick_labels = []
    for month in month_order:
        month_rows = country_df[country_df['month_col'] == month]
        if month_rows.empty:
            continue
        tick_days.append(int(month_rows['day_of_season'].min()))
        tick_labels.append(month_name_map[month])

    return tick_days, tick_labels

def ordinal_suffix(n: int) -> str:
    """Return the correct English ordinal suffix for n (1st, 2nd, 3rd, 4th…)."""
    n = abs(int(n))
    if 11 <= (n % 100) <= 13:   # 11th, 12th, 13th — special cases
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")


# --- Colors matching seasonal_var_daily.py ---
CURRENT_YEAR_COLOR = "#e41a1c"   # red
HIGHLIGHT_COLORS = [
    "#377eb8",  # blue  (2nd most recent past year)
    "#4daf4a",  # green (most recent past year)
]
OLDER_YEARS_COLOR = "#888888"

def grey_to_black_gradient(n):
    """Light grey to near-black for oldest years, giving clear depth gradient."""
    return [
        mcolors.to_hex((v, v, v))
        for v in np.linspace(0.8, 0.05, n)
    ] if n > 1 else ["#cccccc"]
output_dir = "/workspaces/CamOnAirFlow/charts"
os.makedirs(output_dir, exist_ok=True)

for country in sorted(df['country'].dropna().unique()):
    country_df = df[df['country'] == country].copy()
    if country_df.empty:
        continue

    country_df['facet_label'] = country_df['ski_field'].astype(str)
    latest_year = country_df['year_col'].max()
    country_df['is_latest'] = country_df['year_col'] == latest_year

    past_years = sorted([y for y in country_df['year_col'].unique() if y != latest_year])

    # Last 2 past years get distinct highlight colors; older years get grey gradient.
    highlight_years = past_years[-2:]   # most recent 2, ascending
    older_years = past_years[:-2]
    gradient = grey_to_black_gradient(len(older_years))
    year2color = dict(zip(older_years, gradient))
    for i, y in enumerate(highlight_years):
        year2color[y] = HIGHLIGHT_COLORS[i % len(HIGHLIGHT_COLORS)]

    # Build legend entries for labeled years (highlight + latest), newest first.
    labeled_years = highlight_years + [latest_year]
    legend_labels = [str(latest_year)] + [str(y) for y in reversed(highlight_years)]
    legend_colors_map = {str(latest_year): CURRENT_YEAR_COLOR}
    for i, y in enumerate(highlight_years):
        legend_colors_map[str(y)] = HIGHLIGHT_COLORS[i % len(HIGHLIGHT_COLORS)]

    labeled_df = country_df[country_df['year_col'].isin(labeled_years)].copy()
    labeled_df['legend_label'] = labeled_df['year_col'].astype(str)

    country_df = country_df.sort_values(['facet_label', 'is_latest', 'year_col', 'day_of_season'])
    df_ends = country_df[country_df['is_latest']].groupby(['facet_label', 'year_col']).last().reset_index()

    month_ticks, month_labels = season_month_ticks_for_country(country_df)
    ski_fields = sorted(country_df['ski_field'].dropna().unique())
    ncol, _, fig_width, fig_height = get_facet_layout(len(ski_fields))

    p_cum = ggplot()
    # Draw older grey-to-black years first so highlights paint on top.
    for y in older_years:
        p_cum += geom_line(
            data=country_df[country_df['year_col'] == y],
            mapping=aes(x="day_of_season", y="cumulative_snowfall_cm"),
            color=year2color[y], size=1.0, alpha=0.7
        )
    # Draw highlight years + latest via aes(color) so they appear in the legend.
    p_cum += geom_line(
        data=labeled_df,
        mapping=aes(x="day_of_season", y="cumulative_snowfall_cm",
                    group="year_col", color="legend_label"),
        size=1.6, alpha=0.97
    )
    # Latest year also drawn thick on top for visual emphasis (no legend duplicate).
    p_cum += geom_line(
        data=country_df[country_df['is_latest']],
        mapping=aes(x="day_of_season", y="cumulative_snowfall_cm"),
        color=CURRENT_YEAR_COLOR, size=2.7, alpha=0.99
    )
    p_cum += geom_point(
        data=df_ends,
        mapping=aes(x='day_of_season', y='cumulative_snowfall_cm'),
        size=6, color='red', fill='white'
    )
    p_cum += geom_text(
        data=df_ends,
        mapping=aes(x='day_of_season', y='cumulative_snowfall_cm', label='year_col'),
        color='red', size=9, fontweight='bold', nudge_x=4
    )

    percentile_labels = []
    for facet in country_df['facet_label'].unique():
        df_latest_facet = country_df[(country_df['year_col'] == latest_year) & (country_df['facet_label'] == facet)]
        if df_latest_facet.empty:
            continue

        max_day = df_latest_facet['day_of_season'].max()
        cum_value = df_latest_facet.loc[
            df_latest_facet['day_of_season'] == max_day,
            'cumulative_snowfall_cm'
        ].iloc[0]

        facet_past = []
        for y in past_years:
            row = country_df[
                (country_df['year_col'] == y)
                & (country_df['facet_label'] == facet)
                & (country_df['day_of_season'] == max_day)
            ]
            if not row.empty:
                facet_past.append(row['cumulative_snowfall_cm'].iloc[0])
            else:
                prev = country_df[
                    (country_df['year_col'] == y)
                    & (country_df['facet_label'] == facet)
                ]['cumulative_snowfall_cm']
                if not prev.empty:
                    facet_past.append(prev.iloc[-1])

        pct = 100 * sum(x < cum_value for x in facet_past) / len(facet_past) if facet_past else 0

        facet_days = country_df[country_df['facet_label'] == facet]['day_of_season']
        min_day = int(facet_days.min())
        max_day_overall = int(facet_days.max())
        raw_center = (min_day + max_day_overall) / 2
        margin = max(3, int(0.05 * (max_day_overall - min_day)))
        x_center = float(min(max(raw_center, min_day + margin), max_day_overall - margin))

        min_y = country_df[country_df['facet_label'] == facet]['cumulative_snowfall_cm'].min()
        max_y = country_df[country_df['facet_label'] == facet]['cumulative_snowfall_cm'].max()
        y_below = min_y - 0.12 * (max_y - min_y)

        percentile_labels.append({
            'facet_label': facet,
            'day_of_season': x_center,
            'cumulative_snowfall_cm': y_below,
            'percent_label': f"{pct:.0f}{ordinal_suffix(int(round(pct)))} Percentile"
        })

    df_percent = pd.DataFrame(percentile_labels)
    if not df_percent.empty:
        p_cum += geom_text(
            data=df_percent,
            mapping=aes(x='day_of_season', y='cumulative_snowfall_cm', label='percent_label'),
            color='red', size=7.5, fontweight='bold'
        )

    p_cum += scale_color_manual(
        name="Year",
        values=legend_colors_map,
        breaks=legend_labels
    )
    p_cum += guides(color=guide_legend(title="Year", override_aes={'alpha': 1, 'size': 2}))
    p_cum += labs(
        title=f"Cumulative Daily Snowfall ({country}): Last {len(past_years)} Years + (Red = {latest_year})",
        subtitle=(
            f"Grey→black = older years | blue/green = previous 2 years"
            f" | red = {latest_year}"
        ),
        x="Month (season)", y="Cumulative Snowfall (cm)"
    )
    p_cum += scale_x_continuous(
        breaks=month_ticks,
        labels=month_labels,
        expand=(0.01, 0)
    )
    p_cum += facet_wrap('~facet_label', scales='free_x', ncol=ncol)
    p_cum += theme_light(base_size=16)
    p_cum += theme(
        legend_position='right',
        axis_text_x=element_text(size=10),
        axis_title_x=element_text(size=14, weight='bold'),
        plot_title=element_text(weight='bold', size=18),
        plot_subtitle=element_text(size=12),
        panel_spacing=0.07,
        strip_text_x=element_text(color="black", weight="bold", size=12),
        strip_background=element_rect(fill="#e0e0e0", color="#888888"),
        panel_grid_major_x=element_line(color="#343434", size=0.5, linetype='dashed')
    )

    out_path = os.path.join(output_dir, f"cumulative_daily_snowfall_{country}.png")
    p_cum.save(out_path, width=fig_width, height=fig_height, dpi=220, limitsize=False)
    print(f"Saved chart: {out_path}")
