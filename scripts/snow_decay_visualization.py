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
    and country = 'NZ'
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

    # Prepare to return a dict of plots per location
    plots = {}
    color_palette = ['#D32F2F', '#1976D2', '#388E3C', '#FBC02D', '#7B1FA2', '#0288D1']

    for location in melt_df['location'].unique():
        loc_df = melt_df[melt_df['location'] == location].copy()
        # Dynamic label per measure/location
        loc_mean_decay = mean_decay[location]
        loc_mean_temp = mean_temp[location]
        loc_mean_snowfall = mean_snowfall[location]
        measure_labels = {
            'decay_index_mean': f'Decay Rate Index (%/day,\nDiff from Mean\n({loc_mean_decay:.2f}%/day))',
            'temp_index_mean': f'Mean Temp (°C,\nDiff from Mean\n({loc_mean_temp:.2f}°C))',
            'snowfall_index_mean': f'Snowfall Total (cm,\nDiff from Mean\n({loc_mean_snowfall:.2f} cm))'
        }
        loc_df['measure_label'] = loc_df['measure'].map(measure_labels)
        year_breaks = calculate_year_breaks(loc_df['year'])
        p = (ggplot(loc_df, aes(x='year', y='difference', group=1)) +
             geom_hline(yintercept=0, linetype='dashed', color='red', alpha=0.7) +
             geom_line(size=2.2, alpha=0.85, color='#1976D2') +
             geom_point(size=3, alpha=0.95, color='#1976D2') +
             facet_grid('measure_label~.', scales='free_y', labeller='label_value') +
             theme_minimal() +
             theme(
                 figure_size=(10, 12),
                 axis_text_x=element_text(rotation=45, size=12),
                 axis_text_y=element_text(size=12),
                 strip_text_y=element_text(size=11, weight='bold', angle=0),
                 strip_text_x=element_text(size=11, weight='bold'),
                 legend_position='none',
                 axis_title_y=element_text(size=14, weight='bold'),
                 axis_title_x=element_text(size=14, weight='bold'),
                 panel_grid_major=element_line(color='#e0e0e0', size=0.8),
                 panel_grid_minor=element_line(color='#f5f5f5', size=0.5)
        ) +
            labs(
                 title=f'Difference from Mean Year: {location}',
                 subtitle='Facet rows: Decay Rate Index (%/day), Mean Temp (°C), Snowfall Total (cm). All values are difference from mean over all years.',
                 x='Year',
                 y='Difference from Mean',
                 caption='Red dashed line = mean year (0). Each facet row shows a different metric.'
        ) +
            scale_x_continuous(breaks=year_breaks)
        )
        plots[location] = p
    return plots



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
    p = (ggplot(plot_df, aes(x='year', y='avg_decay_rate_pct_per_day', color='location')) +
        #  geom_hline(yintercept=0, linetype='dashed', color='red', alpha=0.7) +
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
             title='Snow Decay Rate Index by Location',
             subtitle='Values >0 indicate faster melting than baseline year, <0 indicate slower',
             x='Year',
             y='Decay Rate Index (Difference from Baseline Year, %/day)',
             caption='Red dashed line = baseline year (0) | Above = faster melt, Below = slower melt'
    ) +
        scale_x_continuous(breaks=year_breaks) +
        scale_color_brewer(type='qual', palette='Set1')
    )

    return p

def save_plots():
    print("Loading data...")
    annual_df = load_annual_data()

    print(f"Locations: {annual_df['location'].unique()}")

    # Create output directory
    output_dir = Path('charts')
    output_dir.mkdir(exist_ok=True)

    # Generate plots
    # One graph per location for multi-index mean comparison
    mean_plots = create_multi_index_mean_comparison_plot(annual_df)
    for location, plot in mean_plots.items():
        filename = f'snow_decay_multi_index_mean_comparison_{location.replace(" ", "_")}.png'
        try:
            print(f"Saving {filename}...")
            plot.save(output_dir / filename, dpi=320, width=10, height=12)
            print(f"✅ Saved {filename}")
        except Exception as e:
            print(f"❌ Error saving {filename}: {e}")

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
