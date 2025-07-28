#!/usr/bin/env python3
"""
California Wildfire Perimeter Visualization (Polygons)
=====================================================
• Displays CALFIRE HISTORICAL PERIMETERS (Post-1980) as true fire polygons (not circles)
• Daily animation from ignition (ALARM_DATE) to containment (CONT_DATE)
• Static size & colour scale (log10 of GIS_ACRES)
• Vertical map layout centred on California
"""

import pandas as pd
import geopandas as gpd
import plotly.express as px
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

DATA_PATH = Path("data/CALFIRE_PERIMETERS/Post1980SHP/California_Fire_Perimeters_(all).shp")

print("Loading CALFIRE perimeter shapefile …")

gdf = gpd.read_file(DATA_PATH)
print(f"Total records in shapefile: {len(gdf):,}")

# -----------------------------------------------------------------------------
# Basic cleaning & prep
# -----------------------------------------------------------------------------

# Ensure CRS is WGS84 for Mapbox
if gdf.crs is None or gdf.crs.to_epsg() != 4326:
    gdf = gdf.to_crs("EPSG:4326")

# Parse dates and fix missing
for col in ["ALARM_DATE", "CONT_DATE"]:
    gdf[col] = pd.to_datetime(gdf[col], errors="coerce")

# Replace missing containment dates with ignition date (instantaneous fire)
gdf["CONT_DATE"] = gdf["CONT_DATE"].fillna(gdf["ALARM_DATE"])

# Ensure acres numeric, replace NaNs with zero (still plotted but smallest size)
gdf["GIS_ACRES"] = pd.to_numeric(gdf["GIS_ACRES"], errors="coerce").fillna(0)

# Drop records without valid geometry or alarm date
initial_len = len(gdf)
gdf = gdf[gdf.geometry.notnull() & gdf["ALARM_DATE"].notnull()]
print(f"Removed {initial_len - len(gdf):,} records without geometry/ALARM_DATE. Remaining: {len(gdf):,}")

# Feature id for geojson mapping
gdf["feature_id"] = gdf.index.astype(str)

# Build one GeoJSON containing ALL perimeter geometries
print("Converting GeoDataFrame to GeoJSON … (this may take a moment)")
geojson = gdf.__geo_interface__  # Fast conversion

# -----------------------------------------------------------------------------
# Build animation DataFrame (daily)
# -----------------------------------------------------------------------------

start_date, end_date = gdf["ALARM_DATE"].min(), gdf["CONT_DATE"].max()
print(f"Date range: {start_date.date()} → {end_date.date()}")

date_range = pd.date_range(start_date, end_date, freq="1D")
print(f"Generating daily frames: {len(date_range):,} days …")

# Pre-compute static colour/size once (log scale)
gdf["log_acres"] = np.log10(gdf["GIS_ACRES"] + 1)
size_scale = (gdf["log_acres"] - gdf["log_acres"].min()) / (gdf["log_acres"].max() - gdf["log_acres"].min())
size_px = 5 + size_scale * 20  # Between 5-25 px (static across frames)
gdf["size_px"] = size_px

animation_records = []

for current_date in date_range:
    active = gdf[(gdf["ALARM_DATE"] <= current_date) & (gdf["CONT_DATE"] >= current_date)]
    if not active.empty:
        frame_df = active[["feature_id", "log_acres", "size_px", "FIRE_NAME", "AGENCY", "GIS_ACRES", "ALARM_DATE", "CONT_DATE"]].copy()
        frame_df["frame_date"] = current_date
        animation_records.append(frame_df)

animation_df = pd.concat(animation_records, ignore_index=True)
print(f"Animation dataframe rows: {len(animation_df):,}")

# Hover template
animation_df["hover"] = (
    "<b>" + animation_df["FIRE_NAME"].fillna("Unnamed Fire") + "</b><br>" +
    "Agency: " + animation_df["AGENCY"].astype(str) + "<br>" +
    "Acres Burned: " + animation_df["GIS_ACRES"].round(0).astype(int).astype(str) + "<br>" +
    "Ignited: " + animation_df["ALARM_DATE"].dt.strftime("%Y-%m-%d") + "<br>" +
    "Contained: " + animation_df["CONT_DATE"].dt.strftime("%Y-%m-%d")
)

# -----------------------------------------------------------------------------
# Create animated choropleth map
# -----------------------------------------------------------------------------

print("Building animated map …")

fig = px.choropleth_mapbox(
    animation_df,
    geojson=geojson,
    locations="feature_id",
    color="log_acres",
    featureidkey="properties.feature_id",
    hover_name="hover",
    animation_frame="frame_date",
    color_continuous_scale="YlOrRd",
    range_color=[gdf["log_acres"].min(), gdf["log_acres"].max()],  # 🔒 static colour scale
    mapbox_style="carto-positron",
    zoom=5,
    center={"lat": 37.5, "lon": -119.5},
    height=1000,  # vertical layout (taller than wide)
    width=650,
    labels={"log_acres": "log₁₀(Acres)"},
    title="California Wildfires Over Time (1980-Present)"
)

# Add citation annotation
fig.add_annotation(
    text="Source: CALFIRE HISTORICAL PERIMETERS",
    x=0.5, y=0.97, xanchor="center", yanchor="top", showarrow=False, font=dict(size=12)
)

# Improve layout (vertical presentation)
fig.update_layout(margin=dict(l=0, r=0, t=80, b=10))

# Ensure slider increments daily (Plotly picks frame order automatically)
fig.update_layout(
    updatemenus=[{
        "type": "buttons",
        "showactive": False,
        "x": 0.05,
        "y": 0.02,
        "buttons": [
            {"label": "Play", "method": "animate", "args": [None, {"frame": {"duration": 20, "redraw": True}, "transition": {"duration": 0}}]},
            {"label": "Pause", "method": "animate", "args": [[None], {"frame": {"duration": 0, "redraw": False}, "mode": "immediate"}]}
        ]
    }]
)

# -----------------------------------------------------------------------------
# Save outputs
# -----------------------------------------------------------------------------
print("Saving HTML output …")
fig.write_html("california_wildfires_perimeter_animation.html", auto_open=True)
print("Done! Open 'california_wildfires_perimeter_animation.html' to view the visualization.") 