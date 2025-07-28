#!/usr/bin/env python3
"""
Interactive California Wildfire Map Visualization
Shows wildfires over time with ignition and extinguishment dates
"""

import pandas as pd
import geopandas as gpd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import numpy as np
import warnings
warnings.filterwarnings('ignore')

print("Loading California wildfire data...")

# Load the shapefile data
shapefile_path = "data/CALFIRE_PERIMETERS/Post1980SHP/California_Fire_Perimeters_(all).shp"
gdf = gpd.read_file(shapefile_path)

print(f"Loaded {len(gdf)} fire records")

# Convert to Web Mercator for better visualization
gdf = gdf.to_crs("EPSG:4326")

# Parse dates
gdf['ALARM_DATE'] = pd.to_datetime(gdf['ALARM_DATE'])
gdf['CONT_DATE'] = pd.to_datetime(gdf['CONT_DATE'])

# Filter out records with invalid dates or missing data
gdf = gdf[gdf['ALARM_DATE'].notna() & gdf['CONT_DATE'].notna() & gdf['GIS_ACRES'].notna()]
gdf = gdf[gdf['ALARM_DATE'] >= '1980-01-01']  # Filter to post-1980 as intended
gdf = gdf[gdf['CONT_DATE'] >= gdf['ALARM_DATE']]  # Ensure containment is after alarm

print(f"Filtered to {len(gdf)} valid records")

# Calculate fire duration
gdf['DURATION_DAYS'] = (gdf['CONT_DATE'] - gdf['ALARM_DATE']).dt.days

# Get centroids for plotting
gdf['centroid'] = gdf.geometry.centroid
gdf['lon'] = gdf.centroid.x
gdf['lat'] = gdf.centroid.y

# Create time-based animation data
# We'll create monthly snapshots showing active fires
start_date = gdf['ALARM_DATE'].min()
end_date = gdf['CONT_DATE'].max()

print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

# Create monthly date range
date_range = pd.date_range(start=start_date, end=end_date, freq='MS')

# Prepare data for animation
animation_data = []

print("Preparing animation data...")

for current_date in date_range:
    # Find fires that are active on this date
    active_fires = gdf[
        (gdf['ALARM_DATE'] <= current_date) & 
        (gdf['CONT_DATE'] >= current_date)
    ].copy()
    
    if len(active_fires) > 0:
        active_fires['frame_date'] = current_date
        active_fires['days_since_ignition'] = (current_date - active_fires['ALARM_DATE']).dt.days
        active_fires['days_until_containment'] = (active_fires['CONT_DATE'] - current_date).dt.days
        
        # Add status information
        active_fires['status'] = active_fires.apply(
            lambda x: 'New' if x['days_since_ignition'] < 30 else 
                     ('Contained Soon' if x['days_until_containment'] < 30 else 'Active'),
            axis=1
        )
        
        animation_data.append(active_fires)

# Combine all frames
if animation_data:
    df_animated = pd.concat(animation_data, ignore_index=True)
else:
    print("No animation data created")
    df_animated = pd.DataFrame()

print(f"Created {len(df_animated)} animation records across {len(date_range)} time frames")

# Create the interactive map
print("Creating interactive map...")

# Define size scale for acres (use log scale for better visualization)
df_animated['size'] = np.log10(df_animated['GIS_ACRES'] + 1) * 5

# Define color scale based on acres burned
df_animated['color_value'] = np.log10(df_animated['GIS_ACRES'] + 1)

# Create hover text
df_animated['hover_text'] = df_animated.apply(
    lambda x: f"<b>{x['FIRE_NAME'] if pd.notna(x['FIRE_NAME']) else 'Unnamed Fire'}</b><br>" +
              f"Agency: {x['AGENCY']}<br>" +
              f"Acres Burned: {x['GIS_ACRES']:,.0f}<br>" +
              f"Ignited: {x['ALARM_DATE'].strftime('%Y-%m-%d')}<br>" +
              f"Contained: {x['CONT_DATE'].strftime('%Y-%m-%d')}<br>" +
              f"Duration: {x['DURATION_DAYS']} days<br>" +
              f"Status: {x['status']}",
    axis=1
)

# Create the animated scatter mapbox
fig = px.scatter_mapbox(
    df_animated,
    lat='lat',
    lon='lon',
    size='size',
    color='color_value',
    hover_name='hover_text',
    animation_frame='frame_date',
    color_continuous_scale='YlOrRd',
    size_max=30,
    zoom=5,
    center={"lat": 37.5, "lon": -119.5},  # Center on California
    mapbox_style='carto-positron',
    title='California Wildfires Over Time (1980-Present)<br><sub>Size indicates acres burned, color shows intensity</sub>',
    labels={'color_value': 'Fire Intensity<br>(log acres)', 'frame_date': 'Date'},
    height=800
)

# Update layout for better appearance
fig.update_layout(
    title_font_size=24,
    title_x=0.5,
    font=dict(size=12),
    margin=dict(l=0, r=0, t=80, b=0),
    updatemenus=[
        dict(
            type="buttons",
            showactive=False,
            y=0.95,
            x=0.05,
            xanchor="left",
            yanchor="top",
            buttons=[
                dict(
                    label="Play",
                    method="animate",
                    args=[None, {
                        "frame": {"duration": 500, "redraw": True},
                        "fromcurrent": True,
                        "transition": {"duration": 300, "easing": "quadratic-in-out"}
                    }]
                ),
                dict(
                    label="Pause",
                    method="animate",
                    args=[[None], {
                        "frame": {"duration": 0, "redraw": False},
                        "mode": "immediate",
                        "transition": {"duration": 0}
                    }]
                )
            ]
        )
    ],
    sliders=[{
        "active": 0,
        "yanchor": "top",
        "xanchor": "left",
        "currentvalue": {
            "font": {"size": 16},
            "prefix": "Date: ",
            "visible": True,
            "xanchor": "right"
        },
        "transition": {"duration": 300, "easing": "cubic-in-out"},
        "pad": {"b": 10, "t": 50},
        "len": 0.9,
        "x": 0.1,
        "y": 0,
        "steps": [
            {
                "args": [[f.strftime('%Y-%m-%d')], {
                    "frame": {"duration": 300, "redraw": True},
                    "mode": "immediate",
                    "transition": {"duration": 300}
                }],
                "label": f.strftime('%b %Y'),
                "method": "animate"
            }
            for f in sorted(df_animated['frame_date'].unique())
        ]
    }]
)

# Update traces
fig.update_traces(
    marker=dict(
        opacity=0.7
    ),
    selector=dict(mode='markers')
)

# Save the interactive map
print("Saving interactive map...")
fig.write_html("california_wildfires_interactive_map.html", auto_open=True)

print("\nMap created successfully!")
print("The interactive map has been saved as 'california_wildfires_interactive_map.html'")
print("It should open automatically in your browser.")

# Also create a static overview map showing all fires colored by decade
print("\nCreating static overview map...")

# Prepare data for static map
gdf['decade'] = (gdf['ALARM_DATE'].dt.year // 10) * 10
gdf['decade_str'] = gdf['decade'].astype(str) + 's'

# Create static map
fig_static = px.scatter_mapbox(
    gdf,
    lat='lat',
    lon='lon',
    size=np.log10(gdf['GIS_ACRES'] + 1) * 5,
    color='decade_str',
    hover_data={
        'FIRE_NAME': True,
        'GIS_ACRES': ':,.0f',
        'ALARM_DATE': '|%Y-%m-%d',
        'DURATION_DAYS': True,
        'decade_str': False,
        'lat': False,
        'lon': False
    },
    color_discrete_sequence=px.colors.qualitative.Plotly,
    size_max=20,
    zoom=5,
    center={"lat": 37.5, "lon": -119.5},
    mapbox_style='carto-positron',
    title='California Wildfires by Decade (1980-Present)<br><sub>Size indicates acres burned</sub>',
    labels={'decade_str': 'Decade', 'GIS_ACRES': 'Acres Burned'},
    height=800
)

fig_static.update_layout(
    title_font_size=24,
    title_x=0.5,
    font=dict(size=12),
    margin=dict(l=0, r=0, t=80, b=0)
)

fig_static.update_traces(
    marker=dict(
        opacity=0.6
    )
)

# Save static map
fig_static.write_html("california_wildfires_overview_map.html")

print("Static overview map saved as 'california_wildfires_overview_map.html'")

# Print summary statistics
print("\n" + "="*50)
print("SUMMARY STATISTICS")
print("="*50)
print(f"Total fires mapped: {len(gdf):,}")
print(f"Total acres burned: {gdf['GIS_ACRES'].sum():,.0f}")
print(f"Average fire size: {gdf['GIS_ACRES'].mean():,.0f} acres")
print(f"Largest fire: {gdf.loc[gdf['GIS_ACRES'].idxmax(), 'FIRE_NAME']} ({gdf['GIS_ACRES'].max():,.0f} acres)")
print(f"Average fire duration: {gdf['DURATION_DAYS'].mean():.1f} days")
print(f"Longest fire duration: {gdf['DURATION_DAYS'].max()} days") 