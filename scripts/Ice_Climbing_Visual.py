import duckdb
from dotenv import load_dotenv
from project_path import get_project_paths, set_dlt_env_vars
import os
from plotnine import ggplot, aes, geom_line, labs, scale_color_manual, theme_light, theme, element_text, guides, guide_legend, facet_wrap, scale_x_datetime
from plotnine.themes.elements import element_rect
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


df = con.execute("""
    SELECT *
    FROM camonairflow.ice_climbing.ice_climbing_hourly
""").df()

df['date'] = pd.to_datetime(df['date'])

locations = df['location'].unique()
color_map = {
    'is_ice_forming': '#1f77b4',
    'ice_has_formed': '#ff7f0e',
    'ice_quality': '#2ca02c',
    'is_ice_degrading': '#d62728'
}

for loc in locations:
    dfl = df[df['location'] == loc].sort_values('date')
    # Melt enrichment fields for easier plotting
    dfl_melt = dfl.melt(id_vars=['date'], value_vars=['is_ice_forming', 'ice_has_formed', 'ice_quality', 'is_ice_degrading'],
                        var_name='enrichment', value_name='value')
    p = (
        ggplot(dfl_melt, aes('date', 'value', color='enrichment'))
        + geom_line(size=1.2)
        + scale_color_manual(values=color_map)
        + scale_x_datetime(date_breaks="10 days", date_labels="%Y-%m-%d")
        + labs(
            title=f"Ice Formation Fields for {loc}",
            y="Score (0-1)", x="Date"
        )
        + theme_light(base_size=16)
        + theme(
            plot_title=element_text(color="white", backgroundcolor="#222222", size=20, weight='bold', ha='center'),
            axis_text_x=element_text(rotation=45, hjust=1)
        )
    )
    p.save(f"/workspaces/CamOnAirFlow/ice_enrichment_{loc.replace(' ', '_')}.png", width=16, height=7, dpi=150)

# --- Combined multi-panel plot for other variables ---
facet_vars = [
    ('temperature_2m', "Temperature (°C)", "#1f77b4"),
    ('sunshine_hours', "Sunshine Hours (hrs)", "#ffbb78"),
    ('hours_below_freeze', "Hours Below Freeze", "#1f77b4"),
    ('total_snow', "Total Snow (mm)", "#2ca02c"),
    ('mean_shortwave', "Mean Shortwave Radiation", "#bcbd22"),  # swapped in
    ('freeze_thaw_cycles', "Freeze Thaw Cycles", "#e377c2"),    # swapped in
    ('total_precip', "Total Precipitation (mm)", "#9467bd"),
    ('mean_rh', "Mean Relative Humidity (%)", "#17becf"),  # NEW VARIABLE
]

for loc in locations:
    dfl = df[df['location'] == loc].sort_values('date')
    # Melt for faceting
    melt_vars = [v[0] for v in facet_vars]
    dfl_melt = dfl.melt(id_vars=['date'], value_vars=melt_vars, var_name='variable', value_name='value')
    # Map pretty names and colors
    name_map = {v[0]: v[1] for v in facet_vars}
    color_map = {v[0]: v[2] for v in facet_vars}
    dfl_melt['pretty'] = dfl_melt['variable'].map(name_map)
    dfl_melt['color'] = dfl_melt['variable'].map(color_map)

    p_facet = (
        ggplot(dfl_melt, aes('date', 'value'))
        + geom_line(aes(color='pretty'), size=1.2)
        + facet_wrap('~pretty', scales='free_y', ncol=2)
        + scale_color_manual(values={v[1]: v[2] for v in facet_vars}, name="Variable")
        + scale_x_datetime(date_breaks="10 days", date_labels="%Y-%m-%d")
        + labs(
            title=f"Weather & Ice Stats for {loc}",
            x="Date",
            y=None
        )
        + theme_light(base_size=16)
        + theme(
            plot_title=element_text(color="white", backgroundcolor="#222222", size=20, weight='bold', ha='center'),
            axis_text_x=element_text(color="white", rotation=45, hjust=1),
            axis_text_y=element_text(color="white"),
            legend_position='right',
            legend_title=element_text(size=14, weight='bold'),
            strip_text_x=element_text(color="black", size=14, weight='bold'),
            strip_background=element_rect(fill="#e0e0e0", color="#888888"),
            plot_background=element_rect(fill="#222222")
        )
        + guides(color=guide_legend(title="Variable"))
    )
    p_facet.save(f"/workspaces/CamOnAirFlow/weather_ice_stats_{loc.replace(' ', '_')}.png", width=16, height=18, dpi=150, limitsize=False)
