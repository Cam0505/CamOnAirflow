import duckdb
import os
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
import pandas as pd
from plotnine import (
    ggplot, aes, geom_line, labs, facet_wrap, theme_light, theme,
    element_text, element_rect, element_line, scale_x_continuous,
    geom_point, geom_text
)
from datetime import date
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
        ski_field,
        country,
        year_col,
        datecol,
        day_of_season,
        facet_label,
        daily_snowfall_cm,
        cumulative_snowfall_cm
    FROM camonairflow.public_analysis.snowfall_cumulative_daily
    WHERE year_col >= 1990
    and country in ('NZ', 'AU')
""").df()

# --- Assign Colors: grey (oldest) to black (newest, except latest) ---
latest_year = df['year_col'].max()
df['is_latest'] = df['year_col'] == latest_year
past_years = sorted([y for y in df['year_col'].unique() if y != latest_year])
n_past = len(past_years)
def grey_to_black_gradient(n):
    # Light grey (0.80, 0.80, 0.80) to black (0,0,0)
    return [
        mcolors.to_hex((v, v, v))
        for v in np.linspace(0.8, 0.05, n)
    ] if n > 1 else ["#cccccc"]
gradient = grey_to_black_gradient(n_past)
year2color = dict(zip(past_years, gradient))
df['year_color'] = df['year_col'].map(lambda y: 'red' if y == latest_year else year2color.get(y, "#000000"))

# For latest year label at right edge
df_ends = df[df['is_latest']].groupby(['facet_label', 'year_col']).last().reset_index()

# --- X Axis: Month ticks ---
def season_month_ticks(year=2022):
    months = [6, 7, 8, 9, 10, 11]
    tick_days = []
    tick_labels = []
    start = date(year, 6, 1)
    for m in months:
        first = date(year, m, 1)
        tick_day = (first - start).days + 1
        tick_days.append(tick_day)
        tick_labels.append(first.strftime('%b'))
    return tick_days, tick_labels
month_ticks, month_labels = season_month_ticks()

df = df.sort_values(['facet_label', 'is_latest', 'year_col'])

# --- Build the plot with individual geom_line for each past year ---
p_cum = ggplot()
for y in past_years:
    p_cum += geom_line(
        data=df[df['year_col'] == y],
        mapping=aes(x="day_of_season", y="cumulative_snowfall_cm"),
        color=year2color[y], size=1.3, alpha=0.93
    )
# Add latest year as thick red line, label its end
p_cum += geom_line(
    data=df[df['is_latest']],
    mapping=aes(x="day_of_season", y="cumulative_snowfall_cm"),
    color='red', size=2.7, alpha=0.99
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

# ---- PERCENTILE LABEL PER FACET (BELOW X AXIS) ----
percentile_labels = []
for facet in df['facet_label'].unique():
    df_latest_facet = df[(df['year_col'] == latest_year) & (df['facet_label'] == facet)]
    if df_latest_facet.empty:
        continue
    max_day = df_latest_facet['day_of_season'].max()
    cum_value = df_latest_facet.loc[df_latest_facet['day_of_season'] == max_day, 'cumulative_snowfall_cm'].iloc[0]

    # For each past year, get value at same day, or last available
    facet_past = []
    for y in past_years:
        row = df[(df['year_col'] == y) & (df['facet_label'] == facet) & (df['day_of_season'] == max_day)]
        if not row.empty:
            facet_past.append(row['cumulative_snowfall_cm'].iloc[0])
        else:
            # Use last available (season might have ended early)
            prev = df[(df['year_col'] == y) & (df['facet_label'] == facet)]['cumulative_snowfall_cm']
            if not prev.empty:
                facet_past.append(prev.iloc[-1])
    if facet_past:
        pct = 100 * sum(x < cum_value for x in facet_past) / len(facet_past)
    else:
        pct = 0

    # Place label below x axis:
    min_y = df[df['facet_label'] == facet]['cumulative_snowfall_cm'].min()
    y_below = min_y - 0.12 * (df[df['facet_label'] == facet]['cumulative_snowfall_cm'].max() - min_y)
    x_beyond = max_day + 4

    percentile_labels.append({
        'facet_label': facet,
        'day_of_season': x_beyond,
        'cumulative_snowfall_cm': y_below,
        'percent_label': f"{pct:.0f}% of years below"
    })

df_percent = pd.DataFrame(percentile_labels)
if not df_percent.empty:
    p_cum += geom_text(
        data=df_percent,
        mapping=aes(x='day_of_season', y='cumulative_snowfall_cm', label='percent_label'),
        color='red', size=7.5, fontweight='bold'
    )

p_cum += labs(
    title=f"Cumulative Daily Snowfall: Last {len(past_years)} Years + (Red = {latest_year})",
    subtitle="Grey = oldest, black = most recent, red = latest",
    x="Month (Season starts in June)", y="Cumulative Snowfall (cm)"
)
p_cum += scale_x_continuous(
    breaks=month_ticks,
    labels=month_labels,
    expand=(0.01, 0)
)
p_cum += facet_wrap('~facet_label', scales='free', ncol=3)
p_cum += theme_light(base_size=16)
p_cum += theme(
    legend_position='none',
    axis_text_x=element_text(size=10),
    axis_title_x=element_text(size=14, weight='bold'),
    plot_title=element_text(weight='bold', size=18),
    plot_subtitle=element_text(size=12),
    panel_spacing=0.07,
    strip_text_x=element_text(color="black", weight="bold", size=12),
    strip_background=element_rect(fill="#e0e0e0", color="#888888"),
    panel_grid_major_x=element_line(color="#343434", size=0.5, linetype='dashed')
)

# Save
out_path = "/workspaces/CamOnAirFlow/charts/cumulative_daily_snowfall.png"
p_cum.save(out_path, width=12, height=14, dpi=240, limitsize=False)
print("✅ Saved chart: cumulative_daily_snowfall.png")
