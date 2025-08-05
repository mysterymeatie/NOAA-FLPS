import os
import warnings
import logging
from datetime import datetime
from pathlib import Path
import xarray as xr
import pandas as pd
import numpy as np

# Import the specific error from the gribapi library if available
try:
    from gribapi.errors import PrematureEndOfFileError
except (ImportError, ModuleNotFoundError):
    # Define a placeholder if gribapi is not installed, so the script doesn't crash
    class PrematureEndOfFileError(IOError):
        pass

# Import the shared grid setup utility
from data.scripts.utils.grid import setup_master_grid

# --- Configuration ---
CONFIG = {
    'LEVEL_FILTERS': {
        "2m": {"typeOfLevel": "heightAboveGround", "level": 2},
        "10m": {"typeOfLevel": "heightAboveGround", "level": 10},
        "surface": {"typeOfLevel": "surface"}
    },
    'HRRR_VARS': ['t2m', 'r2', 'sh2', 'd2m', 'u10', 'v10', 'max_10si', 'prate']
}

def setup_logging():
    """Configures logging to print to the console."""
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    warnings.filterwarnings('ignore', category=UserWarning, module='cfgrib')
    warnings.filterwarnings('ignore', category=FutureWarning)

def process_single_grib_file(grib_path):
    """
    Processes a single GRIB2 file, handling multiple coordinate levels and
    corrupted files, and extracts all variables into a single xarray Dataset.
    """
    level_datasets = []
    try:
        # Extract date from file path to add as a time coordinate
        date_str = Path(grib_path).parent.name  # Assumes YYYYMMDD format
        time_coord = pd.to_datetime(date_str, format='%Y%m%d')

        for level_name, filter_kwargs in CONFIG['LEVEL_FILTERS'].items():
            try:
                # Open each level separately to avoid coordinate conflicts
                ds_level = xr.open_dataset(
                    grib_path,
                    engine="cfgrib",
                    backend_kwargs={'filter_by_keys': filter_kwargs}
                )
                # Add time dimension for later concatenation
                ds_level = ds_level.expand_dims(time=[time_coord])
                level_datasets.append(ds_level)
            except Exception:
                # This will catch errors if a specific level is missing, which is fine.
                continue
        
        if not level_datasets:
            logging.warning(f"No data found in {Path(grib_path).name}. Skipping.")
            return None

        # Merge the datasets from different levels
        merged_ds = xr.merge(level_datasets, compat='override')

        # The native HRRR projection string is needed for rioxarray
        # This is a standard Lambert Conformal Conic for HRRR
        proj_str = "+proj=lcc +lat_1=38.5 +lat_2=38.5 +lat_0=38.5 +lon_0=262.5 +x_0=0 +y_0=0 +R=6371229 +units=m +no_defs"
        merged_ds = merged_ds.rio.write_crs(proj_str)
        
        return merged_ds

    except (PrematureEndOfFileError, EOFError, ValueError) as e:
        # ValueError can be thrown by cfgrib for malformed files
        logging.warning(f"CORRUPTED FILE: Could not process {Path(grib_path).name}. It may be incomplete. Skipping. Error: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while processing {Path(grib_path).name}: {e}", exc_info=True)
        return None

def convert_hrrr_weather():
    """
    Processes all raw HRRR GRIB2 files, reprojects them to the master grid,
    and saves them as a single unified NetCDF file.
    """
    setup_logging()
    logging.info("--- Starting HRRR Data Conversion and Unification ---")

    master_grid = setup_master_grid()
    
    # Create a template raster from the master grid for reprojection
    template_ds = xr.Dataset(
        coords={
            "y": master_grid['y'],
            "x": master_grid['x'],
        }
    ).rio.write_crs(master_grid['crs'])

    hrrr_dir = Path('data/raw/NOAA_HRRR/hrrr')
    grib_files = sorted(list(hrrr_dir.glob('*/subset_*__hrrr.t21z.wrfsfcf00.grib2')))
    
    if not grib_files:
        logging.error(f"No GRIB2 files found in {hrrr_dir}. Exiting.")
        return

    logging.info(f"Found {len(grib_files)} total GRIB2 files to process.")

    all_reprojected_data = []
    
    for i, grib_path in enumerate(grib_files):
        logging.info(f"Processing file {i+1}/{len(grib_files)}: {grib_path.name}")
        
        # 1. Process a single file robustly
        daily_ds = process_single_grib_file(grib_path)
        
        if daily_ds is None:
            continue

        # 2. Reproject to the master grid
        try:
            # Select only the variables we need
            vars_to_process = [var for var in CONFIG['HRRR_VARS'] if var in daily_ds]
            if not vars_to_process:
                logging.warning(f"No required variables found in {grib_path.name}. Skipping.")
                continue

            daily_ds_subset = daily_ds[vars_to_process]

            # Use reproject_match to align with our master grid
            reprojected_ds = daily_ds_subset.rio.reproject_match(
                template_ds, resampling=rasterio.enums.Resampling.bilinear
            )
            all_reprojected_data.append(reprojected_ds)
        except Exception as e:
            logging.error(f"Failed to reproject {grib_path.name}: {e}", exc_info=True)

    if not all_reprojected_data:
        logging.error("No HRRR data was successfully processed and reprojected. Exiting.")
        return
        
    # 3. Combine all reprojected daily data into a single dataset
    logging.info(f"Combining {len(all_reprojected_data)} daily datasets...")
    try:
        final_ds = xr.concat(all_reprojected_data, dim='time').sortby('time')
    except Exception as e:
        logging.error(f"Failed to combine datasets: {e}", exc_info=True)
        return

    # 4. Save the final unified NetCDF file
    output_path = Path('data/unified/weather_hrrr.nc')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    final_ds.attrs.update({
        'title': 'HRRR Weather Data - Unified 3km UTM Format',
        'source': 'NOAA HRRR Model',
        'projection': str(master_grid['crs']),
        'resolution': f'{master_grid["resolution"]}m',
        'created_utc': datetime.utcnow().isoformat()
    })
    
    encoding = {var: {'zlib': True, 'complevel': 5} for var in final_ds.data_vars}
    
    logging.info(f"Saving final unified HRRR data to: {output_path}")
    try:
        final_ds.to_netcdf(output_path, engine='netcdf4', encoding=encoding)
        logging.info("Successfully created unified HRRR NetCDF file.")
    except Exception as e:
        logging.error(f"Failed to save final NetCDF file: {e}", exc_info=True)

    logging.info("--- HRRR Processing Complete ---")

if __name__ == '__main__':
    # Add rioxarray to imports for standalone execution
    import rioxarray
    convert_hrrr_weather()