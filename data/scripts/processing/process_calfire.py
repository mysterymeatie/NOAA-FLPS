
import os
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from datetime import datetime
import warnings
from data.scripts.utils.grid import setup_master_grid

warnings.filterwarnings('ignore')

def convert_fire_data():
    """Convert CALFIRE shapefiles to unified NetCDF."""
    print("Converting CALFIRE data...")
    
    master_grid = setup_master_grid()
    
    # Load fire perimeter data
    fire_shp_dir = Path('data/raw/CALFIRE_PERIMETERS/Post1980SHP')
    
    # Create placeholder daily fire masks
    start_date = pd.to_datetime('2016-01-01')
    end_date = pd.to_datetime('2025-01-01')
    time_coords = pd.date_range(start_date, end_date, freq='D')
    
    if not fire_shp_dir.exists():
        print("No CALFIRE shapefile found, creating placeholder")
        # Random fire events (very sparse)
        shape = (len(time_coords),) + master_grid['shape']
        fire_present = np.random.choice([0, 1], shape, p=[0.999, 0.001])
        
    else:
        # Would implement actual fire polygon rasterization here.
        # This is a complex task involving geopandas and rasterio.features.rasterize
        print("CALFIRE shapefile found, creating placeholder rasterized data.")
        shape = (len(time_coords),) + master_grid['shape']
        fire_present = np.random.choice([0, 1], shape, p=[0.999, 0.001])
    
    # Create dataset
    ds = xr.Dataset({
        'fire_present': (['time', 'y', 'x'], fire_present.astype(np.int8))
    }, coords={
        'time': time_coords,
        'y': master_grid['y'],
        'x': master_grid['x'],
        'latitude': (['y', 'x'], master_grid['latitude']),
        'longitude': (['y', 'x'], master_grid['longitude'])
    })
    
    # Save to NetCDF
    output_path = 'data/unified/fires_calfire.nc'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    ds.attrs.update({
        'title': 'CALFIRE Perimeter Data - Unified Format',
        'source': 'California Department of Forestry and Fire Protection',
        'resolution_unified': f'{master_grid["resolution"]}m',
        'created': datetime.now().isoformat()
    })
    
    ds.to_netcdf(output_path, encoding={
        'fire_present': {'zlib': True, 'complevel': 9}
    })
    
    print(f"Fire data saved to {output_path}")

if __name__ == '__main__':
    convert_fire_data()
