import duckdb
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
import os
from plotnine import ggplot, aes, geom_line, labs, scale_color_manual, element_line, theme_light, theme, element_text, facet_wrap, scale_x_datetime
from plotnine.themes.elements import element_rect
import pandas as pd
from PIL import Image

# Load environment variables and set DLT config
paths = get_project_paths()
set_dlt_env_vars(paths)

PROJECT_ROOT = paths["PROJECT_ROOT"]
ENV_FILE = paths["ENV_FILE"]

load_dotenv(dotenv_path=ENV_FILE)

database_string = os.getenv("MD")
if not database_string:
    raise ValueError("Missing MD in environment.")
# Connect to your DuckDB/MotherDuck database
con = duckdb.connect(database_string) 


df = con.execute("""
    SELECT *
    FROM camonairflow.public_staging.ice_enrichment_model 
""").df()

df['date'] = pd.to_datetime(df['date'])

locations = df['location'].unique()

# Define color maps for the plots
color_map1 = {
    'is_ice_forming': '#1f77b4',
    'is_ice_degrading': '#d62728'
}

color_map2 = {
    'ice_has_formed': '#ff7f0e',
    'ice_quality': '#2ca02c'
}

# Add this function at the top of your script, after your imports
def calculate_date_breaks(date_series):
    """Calculate appropriate date breaks based on data range"""
    date_range = (date_series.max() - date_series.min()).days
    
    # Scale breaks based on range - aim for 8-10 breaks across the range
    if date_range <= 120:  # Less than 4 months
        return "15 days", "%Y-%m-%d"
    elif date_range <= 365:  # Less than a year
        return "30 days", "%Y-%m-%d"
    elif date_range <= 730:  # Less than 2 years
        return "60 days", "%Y-%m-%d"
    else:  # More than 2 years
        return "90 days", "%Y-%m"

for loc in locations:
    dfl = df[df['location'] == loc].sort_values('date')
    
    # Dynamically calculate date breaks
    date_breaks, date_format = calculate_date_breaks(dfl['date'])
    
    # PLOT 1: Formation and Degradation
    forming_degrading = dfl.melt(
        id_vars=['date'], 
        value_vars=['is_ice_forming', 'is_ice_degrading'],
        var_name='enrichment', value_name='value'
    )
    
    p1 = (
        ggplot(forming_degrading, aes('date', 'value', color='enrichment'))
        + geom_line(size=1.2)
        + scale_color_manual(values=color_map1)
        + scale_x_datetime(date_breaks=date_breaks, date_labels=date_format)
        + labs(
            title=f"Ice Formation & Degradation for {loc}",
            y="Score (0-1)", x="Date"  # Add x-label to show on all plots
        )
        + theme_light(base_size=14)
        + theme(
            plot_title=element_text(color="white", backgroundcolor="#222222", size=18, weight='bold', ha='center'),
            axis_text_x=element_text(rotation=45, hjust=1),
            legend_position="right"
        )
    )
    
    temp_file1 = f"/workspaces/CamOnAirFlow/temp_form_degrade_{loc.replace(' ', '_')}.png"
    p1.save(temp_file1, width=16, height=5, dpi=150)
    
    # PLOT 2: Quality and Has Formed
    quality_formed = dfl.melt(
        id_vars=['date'], 
        value_vars=['ice_quality', 'ice_has_formed'],
        var_name='enrichment', value_name='value'
    )
    
    p2 = (
        ggplot(quality_formed, aes('date', 'value', color='enrichment'))
        + geom_line(size=1.2)
        + scale_color_manual(values=color_map2)
        + scale_x_datetime(date_breaks=date_breaks, date_labels=date_format)
        + labs(
            title=f"Ice Quality & Formation Status for {loc}",
            y="Score (0-1)", x="Date"
        )
        + theme_light(base_size=14)
        + theme(
            plot_title=element_text(color="white", backgroundcolor="#222222", size=18, weight='bold', ha='center'),
            axis_text_x=element_text(rotation=45, hjust=1),
            legend_position="right"
        )
    )
    
    temp_file2 = f"/workspaces/CamOnAirFlow/temp_quality_formed_{loc.replace(' ', '_')}.png"
    p2.save(temp_file2, width=16, height=5, dpi=150)
    
    # Combine the two images
    img1 = Image.open(temp_file1)
    img2 = Image.open(temp_file2)
    
    combined_height = img1.height + img2.height
    combined_img = Image.new('RGB', (img1.width, combined_height))
    combined_img.paste(img1, (0, 0))
    combined_img.paste(img2, (0, img1.height))
    
    # Save the combined image
    combined_img.save(f"/workspaces/CamOnAirFlow/ice_enrichment_split_{loc.replace(' ', '_')}.png")
    
    # Clean up temporary files
    os.remove(temp_file1)
    os.remove(temp_file2)

# --- Combined multi-panel plot for other variables ---
facet_vars = [
    ('mean_temperature', "Mean Temperature (°C)", "#1f77b4"),
    ('sunshine_hours', "Sunshine Hours (hrs)", "#ffbb78"),
    ('hours_below_freeze', "Hours Below Freeze", "#1f77b4"),
    ('total_snow', "Total Snow (mm)", "#2ca02c"),
    ('mean_shortwave', "Mean Shortwave Radiation", "#bcbd22"),
    ('freeze_thaw_cycles', "Freeze Thaw Cycles", "#e377c2"),
    ('total_precip', "Total Precipitation (mm)", "#9467bd"),
    ('mean_rh', "Mean Relative Humidity (%)", "#17becf"),
]

for loc in locations:
    dfl = df[df['location'] == loc].sort_values('date')
    
    # Dynamically calculate date breaks
    date_breaks, date_format = calculate_date_breaks(dfl['date'])
    
    # Melt for faceting
    melt_vars = [v[0] for v in facet_vars]
    dfl_melt = dfl.melt(id_vars=['date'], value_vars=melt_vars, var_name='variable', value_name='value')
    
    # Map pretty names and colors
    name_map = {v[0]: v[1] for v in facet_vars}
    color_map = {v[0]: v[2] for v in facet_vars}
    dfl_melt['pretty'] = dfl_melt['variable'].map(name_map)
    dfl_melt['color'] = dfl_melt['variable'].map(color_map)

    # Calculate optimal layout
    num_vars = len(facet_vars)
    ncol = 2  # Default to 2 columns
    

    p_facet = (
        ggplot(dfl_melt, aes('date', 'value'))
        + geom_line(aes(color='pretty'), size=1.8)
        + facet_wrap('~pretty', scales='free_y', ncol=2)
        + scale_color_manual(values={v[1]: v[2] for v in facet_vars}, name="Variable")
        + scale_x_datetime(date_breaks=date_breaks, date_labels=date_format)
        + labs(
            title=f"Weather & Ice Stats for {loc}",
            x="Date", 
            y=None
        )
        + theme_light(base_size=20)
        + theme(
            figure_size=(18, 20),
            plot_title=element_text(color="white", backgroundcolor="#222222", size=28, weight='bold', ha='center'),
            axis_text_x=element_text(rotation=45, hjust=1, size=16),
            axis_title_x=element_text(size=20, weight='bold'),
            axis_text_y=element_text(size=16),
            legend_position='bottom',
            legend_box='horizontal',
            legend_title=element_text(size=20, weight='bold'),
            legend_box_margin=1,
            legend_text=element_text(size=20),
            legend_key_size=18,
            # legend_box_spacing=1,
            legend_margin=1,
            strip_text_x=element_text(size=20, weight='bold', color='black'),
            strip_background=element_rect(fill="#d0d0d0", color="#555555", size=1.5),
            panel_spacing=0.05,
            axis_ticks_x=element_line(color="gray", size=1.0),
            axis_line_x=element_line(color="gray", size=1.0),
            plot_margin=0.05  # Increased margin all around
        )
    )
    
    # Also update the save dimensions
    p_facet.save(
        f"/workspaces/CamOnAirFlow/weather_ice_stats_{loc.replace(' ', '_')}.png", 
        width=24, height=28, dpi=160, limitsize=False  # Increased height
    )
