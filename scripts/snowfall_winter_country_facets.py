import duckdb
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
import os
import math
from plotnine import ggplot, aes, geom_line, geom_point, element_line, scale_x_continuous, geom_hline, labs, scale_fill_manual, scale_color_manual, facet_wrap, theme_light, theme, element_text, geom_bar, element_rect
import pandas as pd

# Load environment variables and set DLT config
paths = get_project_paths()
set_dlt_env_vars(paths)

PROJECT_ROOT = paths["PROJECT_ROOT"]
ENV_FILE = paths["ENV_FILE"]

load_dotenv(dotenv_path=ENV_FILE)

database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")
# Connect to your DuckDB/MotherDuck database (adjust path as needed)
con = duckdb.connect(database_string) 

# Query your analysis view
df = con.execute("""
    SELECT
        snowfall.ski_field,
        snowfall.year_col,
        snowfall.month_col,
        snowfall.avg_daily_snowfall,
        snowfall.total_monthly_snowfall,
        snowfall.country
    FROM camonairflow.public_analysis.snowfall_winter_agg as snowfall
""").df()

if df.empty:
    raise ValueError("No snowfall data returned from snowfall_winter_agg.")

def calculate_year_breaks(year_series):
    """Calculate appropriate year breaks based on data range"""
    min_year = int(year_series.min())
    max_year = int(year_series.max())
    year_range = max_year - min_year + 1
    if year_range <= 6:
        step = 1
    elif year_range <= 10: 
        step = 2
    elif year_range <= 30:
        step = 3
    elif year_range <= 48:
        step = 4
    else: 
        step = 6

    breaks = list(range(min_year, max_year + 1, step))
    return breaks


def get_facet_layout(n_facets):
    """Return (ncol, nrow, width, height) for readable country facet charts."""
    if n_facets <= 4:
        ncol = 2
    elif n_facets <= 9:
        ncol = 3
    else:
        ncol = 4

    nrow = max(1, math.ceil(n_facets / ncol))

    # Size each facet panel generously to avoid compressed bars/labels.
    width = min(24, max(12, ncol * 4.8))
    height = min(24, max(8, nrow * 3.8 + 2.2))
    return ncol, nrow, width, height

month_labels = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
}
# Winter+spring across both hemispheres: NH (Dec-May), SH (Jun-Nov).
season_month_order = [12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
df['month_name'] = df['month_col'].map(month_labels)
df['month_name'] = pd.Categorical(
    df['month_name'],
    categories=[month_labels[m] for m in season_month_order],
    ordered=True
)

month_color_map = {
    'Dec': '#4B0082',
    'Jan': '#6A00A8',
    'Feb': '#2E2A8A',
    'Mar': '#006DCC',
    'Apr': '#0096C7',
    'May': '#00B4D8',
    'Jun': '#48CAE4',
    'Jul': '#2A9D8F',
    'Aug': '#52B788',
    'Sep': '#90BE6D',
    'Oct': '#F9C74F',
    'Nov': '#F8961E'
}

output_dir = "/workspaces/CamOnAirFlow/charts"
os.makedirs(output_dir, exist_ok=True)

color_list = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
    "#bcbd22", "#17becf", "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5", "#c49c94"
]

for country in sorted(df['country'].dropna().unique()):
    country_df = df[df['country'] == country].copy()
    if country_df.empty:
        continue

    country_df['facet_label'] = country_df['ski_field'].astype(str)

    yearly = (
        country_df.groupby(['ski_field', 'year_col'], as_index=False)
        .agg(
            avg_daily_snowfall=('avg_daily_snowfall', 'mean'),
            total_winter_snowfall=('total_monthly_snowfall', 'sum')
        )
    )
    yearly['facet_label'] = yearly['ski_field'].astype(str)

    yearly['prev_total'] = yearly.groupby('ski_field')['total_winter_snowfall'].shift(1)
    yearly['change_vs_prev'] = yearly['total_winter_snowfall'] - yearly['prev_total']
    yearly['long_term_avg'] = yearly.groupby('ski_field')['total_winter_snowfall'].transform('mean')
    yearly['above_avg'] = yearly['total_winter_snowfall'] > yearly['long_term_avg']

    overall_avg = yearly['total_winter_snowfall'].mean()
    yearly_breaks = calculate_year_breaks(yearly['year_col'])

    ski_fields = sorted(yearly['ski_field'].dropna().unique())
    ncol, _, fig_width, fig_height = get_facet_layout(len(ski_fields))
    color_map = {field: color_list[i % len(color_list)] for i, field in enumerate(ski_fields)}

    p_vs_avg = (
        ggplot(yearly, aes('year_col', 'total_winter_snowfall', color='ski_field'))
        + geom_line(size=1.8)
        + geom_hline(aes(yintercept='long_term_avg'), linetype='dashed', color='#1f77b4', size=1.2)
        + geom_hline(yintercept=overall_avg, linetype='dotted', color='#ff7f0e', size=1.2)
        + geom_point(aes(fill='above_avg'), size=2.5, color='black', alpha=0.8, show_legend=False)
        + labs(
            title=f'Winter Snowfall vs Long-Term & Overall Average ({country})',
            subtitle='Dashed line = ski field avg; dotted orange = country overall avg; blue=above avg, orange=below avg',
            x='Year', y='Total Winter Snowfall (cm)'
        )
        + scale_x_continuous(breaks=yearly_breaks)
        + scale_fill_manual(values={True: '#1f77b4', False: '#ff7f0e'})
        + scale_color_manual(values=color_map)
        + facet_wrap('~facet_label', scales='fixed', ncol=ncol)
        + theme_light(base_size=14)
        + theme(
            legend_position='right',
            axis_text_x=element_text(rotation=45, hjust=1, size=10),
            axis_title_x=element_text(size=14, weight='bold'),
            plot_title=element_text(weight='bold', size=18),
            plot_subtitle=element_text(size=11),
            panel_spacing=0.08,
            strip_text_x=element_text(color="black", weight="bold", size=10),
            strip_background=element_rect(fill="#e0e0e0", color="#888888"),
            panel_grid_major_x=element_line(color="#343434", size=0.5, linetype='dashed')
        )
    )

    vs_avg_out = os.path.join(output_dir, f"winter_snowfall_vs_avg_{country}.png")
    p_vs_avg.save(vs_avg_out, width=fig_width, height=fig_height, dpi=130, limitsize=False)

    country_monthly_df = country_df[country_df['year_col'] != 2026].copy()
    if country_monthly_df.empty:
        country_monthly_df = country_df.copy()
    monthly_breaks = calculate_year_breaks(country_monthly_df['year_col'])

    p_monthly = (
        ggplot(country_monthly_df, aes('year_col', 'total_monthly_snowfall', fill='month_name'))
        + geom_bar(stat='identity', position='stack')
        + facet_wrap('~facet_label', scales='fixed', ncol=ncol)
        + labs(
            title=f'Total Winter Snowfall per Month ({country})',
            subtitle='Each bar shows total snowfall (cm) by month',
            x='Year', y='Total Snowfall (cm)'
        )
        + theme_light(base_size=14)
        + scale_x_continuous(breaks=monthly_breaks)
        + scale_fill_manual(values=month_color_map)
        + theme(
            legend_position='right',
            axis_text_x=element_text(rotation=45, hjust=1, size=9),
            axis_title_x=element_text(size=12, weight='bold'),
            plot_title=element_text(weight='bold', size=16),
            plot_subtitle=element_text(size=10),
            panel_spacing=0.08,
            strip_text_x=element_text(color="black", weight="bold", size=10),
            strip_background=element_rect(fill="#e0e0e0", color="#888888"),
            panel_grid_major_x=element_line(color="#343434", size=0.5, linetype='dashed')
        )
    )

    monthly_out = os.path.join(output_dir, f"total_snowfall_per_month_{country}.png")
    p_monthly.save(monthly_out, width=fig_width, height=fig_height, dpi=130, limitsize=False)
    print(f"Saved charts for {country}: {vs_avg_out}, {monthly_out}")