import duckdb
import os
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
from plotnine import (
    ggplot, aes, geom_line, labs, facet_wrap, theme_light, theme,
    element_text, element_rect, element_line, scale_x_continuous,
    scale_color_manual, guides, guide_legend
)

# --------------- CONFIGURABLE COLORS ----------------
CURRENT_YEAR_COLOR = "#e41a1c"  # RED
HIGHLIGHT_COLORS = [
    "#377eb8",  # blue
    "#4daf4a",  # green
    "#ffdf00",  # yellow
    "#000000",  # black
]
OLDER_YEARS_COLOR = "#888888"  # grey
# ----------------------------------------------------

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
    and country in ('AU', 'NZ')
""").df()

# --- Calculate daily mean for each day and field ---
df['facet_label'] = df['facet_label'].astype(str)
clim = (
    df.groupby(['facet_label', 'day_of_season'])['cumulative_snowfall_cm']
    .mean()
    .reset_index()
    .rename(columns={'cumulative_snowfall_cm': 'mean_cumulative'})
)
df = df.merge(clim, on=['facet_label', 'day_of_season'], how='left')
df['snowfall_anomaly'] = df['cumulative_snowfall_cm'] - df['mean_cumulative']

# --- Colour assignments ---
years_sorted = sorted(df['year_col'].unique())
current_year = years_sorted[-1]
last4_years = years_sorted[-5:-1]  # previous 4 years, most recent last
older_years = years_sorted[:-5]

year2color = {current_year: CURRENT_YEAR_COLOR}
for i, y in enumerate(last4_years):
    color = HIGHLIGHT_COLORS[i] if i < len(HIGHLIGHT_COLORS) else "#333333"
    year2color[y] = color
for y in older_years:
    year2color[y] = OLDER_YEARS_COLOR

def legend_label(row):
    if row['year_col'] == current_year:
        return str(current_year)
    elif row['year_col'] in last4_years:
        return str(row['year_col'])
    else:
        return f"1990–{years_sorted[-6]}"

df['color'] = df['year_col'].map(year2color)
df['legend_label'] = df.apply(legend_label, axis=1)

legend_labels = [str(current_year)] + [str(y) for y in last4_years] + [f"1990–{years_sorted[-6]}"]
legend_colors = [year2color[current_year]] + [year2color[y] for y in last4_years] + [OLDER_YEARS_COLOR]

# --- Plot ---
p = (
    ggplot(df, aes(x="day_of_season", y="snowfall_anomaly", group="year_col", color="legend_label"))
    + geom_line(size=1.3, alpha=0.98)
    + facet_wrap('~facet_label', scales='free', ncol=3)
    + scale_x_continuous(
        breaks=[1, 31, 62, 93, 124, 155],  # Jun–Nov approx.
        labels=['Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov'],
        expand=(0.01, 0)
    )
    + scale_color_manual(
        name="Year",
        values=dict(zip(legend_labels, legend_colors))
    )
    + guides(color=guide_legend(title="Year", override_aes={'alpha': 1, 'size': 2}))
    + labs(
        title="Daily Cumulative Snowfall Anomaly vs. Historical Mean",
        subtitle=(
            f"Red = {current_year}, last 4 = blue/green/yellow/black, "
            f"grey = 1990–{years_sorted[-6]}\n"
            "Anomaly = Cumulative snowfall minus daily mean (1990–present)"
        ),
        x="Month (Season starts in June)", y="Cumulative Snowfall Anomaly (cm)"
    )
    + theme_light(base_size=16)
    + theme(
        legend_position='bottom',
        legend_title=element_text(weight='bold', size=12),
        legend_text=element_text(size=11),
        axis_text_x=element_text(size=10),
        axis_title_x=element_text(size=14, weight='bold'),
        plot_title=element_text(weight='bold', size=18),
        plot_subtitle=element_text(size=12),
        panel_spacing=0.07,
        strip_text_x=element_text(color="black", weight="bold", size=12),
        strip_background=element_rect(fill="#e0e0e0", color="#888888"),
        panel_grid_major_x=element_line(color="#343434", size=0.5, linetype='dashed')
    )
)

# --- Save chart ---
out_path = "/workspaces/CamOnAirFlow/charts/snowfall_anomaly_daily_legend.png"
p.save(out_path, width=12, height=12, dpi=180, limitsize=False)
print("✅ Saved chart with legend: snowfall_anomaly_daily_legend.png")
