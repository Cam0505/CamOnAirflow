import duckdb
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
import os
from plotnine import ggplot, aes, geom_line, geom_point, geom_hline, labs, scale_fill_manual, scale_color_manual, facet_wrap, theme_light, theme, element_text, geom_bar, element_rect
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
con = duckdb.connect(database_string) 

# Query your rainfall analysis view
df = con.execute("""
    SELECT
        Ice_climbing,
        country,
        year_col,
        month_col,
        avg_daily_precipitation,
        total_monthly_precipitation,
        avg_daily_rainfall,
        total_monthly_rainfall
    FROM camonairflow.public_analysis.rainfall_pre_winter_agg
                 where Ice_climbing in ( 'Remarkables')
""").df()
#  'Black Peak'

# Helper for both rainfall and precipitation
def make_plots(df, value_col, avg_col, label_prefix, file_prefix):
    df = df.copy()
    df['season_total'] = df.groupby(['Ice_climbing', 'year_col'])[value_col].transform('sum')
    df['month_prop'] = df[value_col] / df['season_total']
    df['month_name'] = df['month_col'].map({3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul'})
    df['month_name'] = pd.Categorical(df['month_name'], categories=['Mar', 'Apr', 'May', 'Jun', 'Jul'], ordered=True)
    df['facet_label'] = df['country'] + ' - ' + df['Ice_climbing']

    yearly = (
        df.groupby(['Ice_climbing', 'year_col'], as_index=False)
        .agg(
            avg_daily=(avg_col, 'mean'),
            total_season=(value_col, 'sum')
        )
    )
    yearly = yearly.merge(df[['Ice_climbing', 'country']].drop_duplicates(), on='Ice_climbing', how='left')
    yearly['prev_total'] = yearly.groupby('Ice_climbing')['total_season'].shift(1)
    yearly['change_vs_prev'] = yearly['total_season'] - yearly['prev_total']
    yearly['long_term_avg'] = yearly.groupby('Ice_climbing')['total_season'].transform('mean')
    yearly['change_positive'] = yearly['change_vs_prev'] > 0
    yearly['above_avg'] = yearly['total_season'] > yearly['long_term_avg']
    overall_avg = yearly['total_season'].mean()
    yearly['facet_label'] = yearly['country'] + ' - ' + yearly['Ice_climbing']

    # Trend vs avg
    p_trend = (
        ggplot(yearly, aes('year_col', 'total_season', color='Ice_climbing'))
        + geom_line(size=1.8)
        + geom_hline(aes(yintercept='long_term_avg'), linetype='dashed', color='#1f77b4', size=1.2)
        + geom_hline(yintercept=overall_avg, linetype='dotted', color='#ff7f0e', size=1.2)
        + geom_point(aes(fill='above_avg'), size=4, color='black', alpha=0.8, show_legend=False)
        + labs(
            title=f'{label_prefix} vs Long-Term & Overall Average',
            subtitle='Dashed line = location avg; dotted orange = overall avg; blue=above avg, orange=below avg',
            x='Year', y=f'Total Pre-Winter {label_prefix} (mm)'
        )
        + scale_fill_manual(values={True: '#1f77b4', False: '#ff7f0e'})
        + scale_color_manual(values=color_list)
        + facet_wrap('~facet_label', scales='free_x', nrow=1)
        + theme_light(base_size=16)
        + theme(
            legend_position='right',
            axis_text_x=element_text(rotation=45, hjust=1, size=12),
            axis_title_x=element_text(size=16, weight='bold'),
            plot_title=element_text(weight='bold', size=20),
            plot_subtitle=element_text(size=14),
            panel_spacing=0.05,
            strip_text_x=element_text(color="black", weight="bold", size=12),
            strip_background=element_rect(fill="#e0e0e0", color="#888888")
        )
    )

    # Monthly proportion
    df_plot = df[df['year_col'] != 2025]
    p_prop = (
        ggplot(df_plot, aes('year_col', 'month_prop', fill='month_name'))
        + geom_bar(stat='identity', position='stack')
        + facet_wrap('~facet_label', nrow=1)
        + labs(
            title=f'Monthly Proportion of Pre-Winter {label_prefix}',
            subtitle=f'Each bar shows the % of pre-winter {label_prefix.lower()} by month',
            x='Year', y=f'Proportion of Pre-Winter {label_prefix}'
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

    p_trend.save(f"/workspaces/CamOnAirFlow/{file_prefix}_vs_avg.png", width=28, height=17, dpi=150, limitsize=False)
    p_prop.save(f"/workspaces/CamOnAirFlow/monthly_proportion_{file_prefix}.png", width=28, height=17, dpi=150, limitsize=False)

# Color palette
color_list = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
    "#bcbd22", "#17becf", "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5", "#c49c94"
]

# Rainfall visuals
make_plots(df, 'total_monthly_rainfall', 'avg_daily_rainfall', 'Rainfall', 'pre_winter_rainfall')

# Precipitation visuals
make_plots(df, 'total_monthly_precipitation', 'avg_daily_precipitation', 'Precipitation', 'pre_winter_precipitation')