import duckdb
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
import os
from plotnine import ggplot, aes, geom_line, geom_point, geom_text, geom_hline, labs, scale_fill_manual, scale_color_manual, facet_wrap, theme_light, theme, element_text, geom_bar, element_rect
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

# Calculate total winter snowfall per ski_field/year for proportion
df['season_total'] = df.groupby(['ski_field', 'year_col'])['total_monthly_snowfall'].transform('sum')
df['month_prop'] = df['total_monthly_snowfall'] / df['season_total']
df['month_name'] = df['month_col'].map({6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov'})
df['month_name'] = pd.Categorical(df['month_name'], categories=['Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov'], ordered=True)
df['facet_label'] = df['country'] + ' - ' + df['ski_field']

# Yearly aggregation from the monthly analysis
yearly = (
    df.groupby(['ski_field', 'year_col'], as_index=False)
    .agg(
        avg_daily_snowfall=('avg_daily_snowfall', 'mean'),
        total_winter_snowfall=('total_monthly_snowfall', 'sum')
    )
)

# Add country info to yearly
yearly = yearly.merge(df[['ski_field', 'country']].drop_duplicates(), on='ski_field', how='left')

# Calculate year-on-year change and long-term average
yearly['prev_total'] = yearly.groupby('ski_field')['total_winter_snowfall'].shift(1)
yearly['change_vs_prev'] = yearly['total_winter_snowfall'] - yearly['prev_total']
yearly['long_term_avg'] = yearly.groupby('ski_field')['total_winter_snowfall'].transform('mean')
yearly['change_positive'] = yearly['change_vs_prev'] > 0
yearly['above_avg'] = yearly['total_winter_snowfall'] > yearly['long_term_avg']

# Calculate the overall average (across all ski fields and years)
overall_avg = yearly['total_winter_snowfall'].mean()

# Define a color list for manual scaling
color_list = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
    "#bcbd22", "#17becf", "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5", "#c49c94"
]

# Add this before plotting:
yearly['facet_label'] = yearly['country'] + ' - ' + yearly['ski_field']

# Plot 1: Yearly trend with change vs previous year
p1 = (
    ggplot(yearly, aes('year_col', 'total_winter_snowfall', color='ski_field'))
    + geom_line(size=1.8)
    + geom_point(aes(fill='change_positive'), size=4, color='black', alpha=0.8, show_legend=False)
    + geom_text(aes(label='change_vs_prev'), nudge_y=12, size=8, format_string='{:.0f}', color='black', show_legend=False)
    + labs(title='Total Winter Snowfall by Year',
           subtitle='Point color: green=increase, red=decrease vs previous year',
           x='Year', y='Total Winter Snowfall (cm)')
    + scale_fill_manual(values={True: '#2ca02c', False: '#d62728'})
    + scale_color_manual(values=color_list)
    + facet_wrap('~facet_label', scales='free_x', ncol=4)
    + theme_light(base_size=16)
    + theme(
        legend_position='right',
        axis_text_x=element_text(rotation=45, hjust=1, size=12),
        axis_title_x=element_text(size=16, weight='bold'),
        plot_title=element_text(weight='bold', size=20),
        plot_subtitle=element_text(size=14),
        panel_spacing=0.05,  # Reduce space between panels
        strip_text_x=element_text(color="black", weight="bold", size=12),
        strip_background=element_rect(fill="#e0e0e0", color="#888888")
    )
)

# Plot 2: Highlight vs long-term average and overall average
p2 = (
    ggplot(yearly, aes('year_col', 'total_winter_snowfall', color='ski_field'))
    + geom_line(size=1.8)
    + geom_hline(aes(yintercept='long_term_avg'), linetype='dashed', color='#1f77b4', size=1.2)
    + geom_hline(yintercept=overall_avg, linetype='dotted', color='#ff7f0e', size=1.2)
    + geom_point(aes(fill='above_avg'), size=4, color='black', alpha=0.8, show_legend=False)
    + labs(
        title='Winter Snowfall vs Long-Term & Overall Average',
        subtitle='Dashed line = ski field avg; dotted orange = overall avg; blue=above avg, orange=below avg',
        x='Year', y='Total Winter Snowfall (cm)'
    )
    + scale_fill_manual(values={True: '#1f77b4', False: '#ff7f0e'})
    + scale_color_manual(values=color_list)
    + facet_wrap('~facet_label', scales='free_x', ncol=4)
    + theme_light(base_size=16)
    + theme(
        legend_position='right',
        axis_text_x=element_text(rotation=45, hjust=1, size=12),
        axis_title_x=element_text(size=16, weight='bold'),
        plot_title=element_text(weight='bold', size=20),
        plot_subtitle=element_text(size=14),
        panel_spacing=0.05,  # Reduce space between panels
        strip_text_x=element_text(color="black", weight="bold", size=12),
        strip_background=element_rect(fill="#e0e0e0", color="#888888")
    )
)

# Remove 2025 from the dataframe before plotting p3
df_plot = df[df['year_col'] != 2025]

# Plot 3: Monthly proportion of total winter snowfall (without 2025)
p3 = (
    ggplot(df_plot, aes('year_col', 'month_prop', fill='month_name'))
    + geom_bar(stat='identity', position='stack')
    + facet_wrap('~facet_label', ncol=4)
    + labs(
        title='Monthly Proportion of Total Winter Snowfall',
        subtitle='Each bar shows the % of season snowfall by month',
        x='Year', y='Proportion of Season Snowfall'
    )
    + theme_light(base_size=16)
    + theme(
        legend_position='right',
        axis_text_x=element_text(rotation=45, hjust=1, size=10),
        axis_title_x=element_text(size=14, weight='bold'),
        plot_title=element_text(weight='bold', size=18),
        plot_subtitle=element_text(size=12),
        panel_spacing=0.05,
        strip_text_x=element_text(color="black", weight="bold", size=12),
        strip_background=element_rect(fill="#e0e0e0", color="#888888")
    )
)

# Save with a wide and shorter aspect ratio, under 6100px in both dimensions
p2.save("/workspaces/CamOnAirFlow/winter_snowfall_vs_avg.png", width=32, height=28, dpi=150, limitsize=False)
p3.save("/workspaces/CamOnAirFlow/monthly_proportion_snowfall.png", width=32, height=28, dpi=150, limitsize=False)