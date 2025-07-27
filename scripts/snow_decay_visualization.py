#!/usr/bin/env python3
"""
Snow Decay Analysis Visualization

Creates faceted time series plots showing:
1. Annual decay rate trends by location
2. Monthly decay rate patterns by location
3. Snow depth trends over time
4. Temperature correlation analysis

Uses plotnine for ggplot2-style plotting with facets.
"""

import pandas as pd
from plotnine import (
    ggplot, aes, geom_hline, geom_line, geom_point, facet_grid, facet_wrap,
    theme_minimal, theme, element_text, element_line, labs,
    scale_x_continuous, scale_color_manual, scale_color_brewer, geom_vline
)
import duckdb
from pathlib import Path
import warnings
import os
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars

warnings.filterwarnings('ignore')

# Load environment variables and set DLT config
paths = get_project_paths()
set_dlt_env_vars(paths)

PROJECT_ROOT = paths["PROJECT_ROOT"]
ENV_FILE = paths["ENV_FILE"]

load_dotenv(dotenv_path=ENV_FILE)


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



# Database connection
def get_db_connection():
    """Connect to MotherDuck database"""
    database_string = os.getenv("MD")
    if not database_string:
        raise ValueError("Missing MD in environment.")
    return duckdb.connect(database_string)

def load_annual_data():
    """Load annual snow decay data"""
    conn = get_db_connection()

    query = """
    SELECT 
        location,
        year,
        decay_days,
        avg_annual_temp_c,
        avg_decay_rate_pct_per_day,
        decay_rate_index,
        year_over_year_decay_change_pct,
        total_annual_snowfall_cm,
        baseline_annual_temp_c,
        baseline_annual_snowfall_cm,
        data_quality_rating
    FROM camonairflow.public_base.base_simple_snow_decay
    where year <= 2024
    ORDER BY location, year
    """

    df = conn.execute(query).fetchdf()
    conn.close()
    return df



def create_multi_index_mean_comparison_plot(df):
    """Create multi-row facet chart: % melt per day, mean temp diff, snowfall sum diff vs mean over all years"""
    plot_df = df.copy()
    # Calculate means over all years for each location
    mean_decay = plot_df.groupby('location')['decay_rate_index'].mean()
    mean_temp = plot_df.groupby('location')['avg_annual_temp_c'].mean()
    mean_snowfall = plot_df.groupby('location')['total_annual_snowfall_cm'].mean()

    plot_df['decay_index_mean'] = plot_df.apply(lambda row: row['decay_rate_index'] - mean_decay[row['location']], axis=1)
    plot_df['temp_index_mean'] = plot_df.apply(lambda row: row['avg_annual_temp_c'] - mean_temp[row['location']], axis=1)
    plot_df['snowfall_index_mean'] = plot_df.apply(lambda row: row['total_annual_snowfall_cm'] - mean_snowfall[row['location']], axis=1)

    melt_df = pd.melt(
        plot_df,
        id_vars=['location', 'year'],
        value_vars=['decay_index_mean', 'temp_index_mean', 'snowfall_index_mean'],
        var_name='measure',
        value_name='difference'
    )
    measure_labels = {
        'decay_index_mean': 'Decay Rate Index (%/day,\nDiff from Mean)',
        'temp_index_mean': 'Mean Temp (°C,\nDiff from Mean)',
        'snowfall_index_mean': 'Snowfall Total (cm,\nDiff from Mean)'
    }
    melt_df['measure_label'] = melt_df['measure'].map(measure_labels)

    year_breaks = calculate_year_breaks(melt_df['year'])
    color_palette = ['#D32F2F', '#1976D2', '#388E3C', '#FBC02D', '#7B1FA2', '#0288D1']

    p = (ggplot(melt_df, aes(x='year', y='difference', color='location', group='location')) +
         geom_hline(yintercept=0, linetype='dashed', color='red', alpha=0.7) +
         geom_line(size=2.2, alpha=0.85) +
         geom_point(size=3, alpha=0.95) +
         facet_grid('measure_label~location', scales='free_y', labeller='label_value') +
         theme_minimal() +
         theme(
             figure_size=(15, 18),
             axis_text_x=element_text(rotation=45, size=12),
             axis_text_y=element_text(size=12),
             strip_text_y=element_text(size=11, weight='bold', angle=0),
             strip_text_x=element_text(size=11, weight='bold'),
             legend_position='right',
             legend_title=element_text(size=14, weight='bold'),
             legend_text=element_text(size=12),
             axis_title_y=element_text(size=14, weight='bold'),
             axis_title_x=element_text(size=14, weight='bold'),
             panel_grid_major=element_line(color='#e0e0e0', size=0.8),
             panel_grid_minor=element_line(color='#f5f5f5', size=0.5)
    ) +
        labs(
             title='Difference from Mean Year by Location and Measure',
             subtitle='Each colored line = location. Facet rows: Decay Rate Index (%/day), Mean Temp (°C), Snowfall Total (cm). All values are difference from mean over all years.',
             x='Year',
             y='Difference from Mean',
             color='Location',
             caption='Red dashed line = mean year (0). Each facet row shows a different metric. Legend at right shows location color.'
    ) +
        scale_x_continuous(breaks=year_breaks) +
        scale_color_manual(values=color_palette)
    )
    # Add horizontal mean line (already at y=0)
    return p



def create_index_comparison_plot(df):
    """Create index comparison plot (baseline = 100)"""

    # Filter for locations with good data quality
    quality_locations = df[df['data_quality_rating'].isin(['High', 'Medium'])]['location'].unique()
    plot_df = df[df['location'].isin(quality_locations)].copy()

    if len(plot_df) == 0:
        print("No data available for index comparison plot")
        return None

    # Get full year range
    year_breaks = calculate_year_breaks(plot_df['year'])

    # Add a vertical line and annotation for each baseline year
    p = (ggplot(plot_df, aes(x='year', y='decay_rate_index', color='location')) +
         geom_hline(yintercept=0, linetype='dashed', color='red', alpha=0.7) +
         geom_line(size=1.2, alpha=0.8) +
         geom_point(size=2, alpha=0.9) +
         facet_wrap('~location', scales='free_y', ncol=3) +
         theme_minimal() +
         theme(
             figure_size=(15, 10),
             axis_text_x=element_text(rotation=45),
             strip_text=element_text(size=10, weight='bold'),
             legend_position='none'
    ) +
        labs(
             title='Snow Decay Rate Index by Location (Difference from Baseline Year)',
             subtitle='Values >0 indicate faster melting than baseline year, <0 indicate slower',
             x='Year',
             y='Decay Rate Index (Difference from Baseline Year, %/day)',
             caption='Red dashed line = baseline year (0) | Above = faster melt, Below = slower melt'
    ) +
        scale_x_continuous(breaks=year_breaks) +
        scale_color_brewer(type='qual', palette='Set1')
    )

    return p

def create_multi_index_comparison_plot(df):
    """Create multi-row facet chart: % melt per day, mean temp diff, snowfall sum diff"""
    # Calculate differences from baseline using new DBT columns
    plot_df = df.copy()
    plot_df['decay_index'] = plot_df['decay_rate_index']
    plot_df['temp_index'] = plot_df['avg_annual_temp_c'] - plot_df['baseline_annual_temp_c']
    plot_df['snowfall_index'] = plot_df['total_annual_snowfall_cm'] - plot_df['baseline_annual_snowfall_cm']

    melt_df = pd.melt(
        plot_df,
        id_vars=['location', 'year'],
        value_vars=['decay_index', 'temp_index', 'snowfall_index'],
        var_name='measure',
        value_name='difference'
    )
    measure_labels = {
        'decay_index': 'Decay Rate Index (%/day, \nDiff from Baseline)',
        'temp_index': 'Mean Temp (°C, \nDiff from Baseline)',
        'snowfall_index': 'Snowfall Total (cm, \nDiff from Baseline)'
    }
    melt_df['measure_label'] = melt_df['measure'].map(measure_labels)

    year_breaks = calculate_year_breaks(melt_df['year'])

    # Use more distinct colors
    color_palette = ['#D32F2F', '#1976D2', '#388E3C', '#FBC02D', '#7B1FA2', '#0288D1']

    # Add baseline year annotation
    baseline_year = plot_df['year'].min()

    p = (ggplot(melt_df, aes(x='year', y='difference', color='location', group='location')) +
         geom_hline(yintercept=0, linetype='dashed', color='red', alpha=0.7) +
         geom_line(size=2.2, alpha=0.85) +
         geom_point(size=3, alpha=0.95) +
         facet_grid('measure_label~location', scales='free_y', labeller='label_value') +
         theme_minimal() +
         theme(
             figure_size=(15, 18),
             axis_text_x=element_text(rotation=45, size=12),
             axis_text_y=element_text(size=12),
             strip_text_y=element_text(size=11, weight='bold', angle=0), 
             strip_text_x=element_text(size=11, weight='bold'),
             legend_position='right',
             legend_title=element_text(size=14, weight='bold'),
             legend_text=element_text(size=12),
             axis_title_y=element_text(size=14, weight='bold'),
             axis_title_x=element_text(size=14, weight='bold'),
             panel_grid_major=element_line(color='#e0e0e0', size=0.8),
             panel_grid_minor=element_line(color='#f5f5f5', size=0.5)
    ) +
        labs(
             title='Difference from Baseline Year by Location and Measure',
             subtitle=f'Each colored line = location. Facet rows: Decay Rate Index (%/day), Mean Temp (°C), Snowfall Total (cm). All values are difference from {baseline_year}.',
             x='Year',
             y='Difference from Baseline',
             color='Location',
             caption='Red dashed line = baseline year (0). Each facet row shows a different metric. Legend at right shows location color.'
    ) +
        scale_x_continuous(breaks=year_breaks) +
        scale_color_manual(values=color_palette)
    )
    # Add vertical baseline year lines and annotation per location
    p += geom_vline(xintercept=baseline_year, linetype='dotted', color='black', alpha=0.7)

    return p

def save_plots():
    print("Loading data...")
    annual_df = load_annual_data()
    # monthly_df = load_monthly_data()

    # print(f"Loaded {len(annual_df)} annual records and {len(monthly_df)} monthly records")
    print(f"Locations: {annual_df['location'].unique()}")

    # Create output directory
    output_dir = Path('charts')
    output_dir.mkdir(exist_ok=True)

    # Generate plots
    plots = [
        ('snow_decay_rate_index.png', create_index_comparison_plot(annual_df)),
        ('snow_decay_multi_index_comparison.png', create_multi_index_comparison_plot(annual_df)),
        ('snow_decay_multi_index_mean_comparison.png', create_multi_index_mean_comparison_plot(annual_df))
    ]

    for filename, plot in plots:
        if plot is not None:
            try:
                print(f"Saving {filename}...")
                plot.save(output_dir / filename, dpi=300, width=13, height=16)
                print(f"✅ Saved {filename}")
            except Exception as e:
                print(f"❌ Error saving {filename}: {e}")
        else:
            print(f"⚠️  Skipped {filename} - no data available")

if __name__ == "__main__":
    print("🏂 Snow Decay Analysis Visualization")
    print("=====================================")

    try:
        save_plots()
        print("\n🎉 Visualization complete! Check the 'charts' directory for output files.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
