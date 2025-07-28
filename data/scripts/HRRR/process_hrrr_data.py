# -*- coding: utf-8 -*-
"""
This script processes downloaded HRRR GRIB2 files into a single, analysis-ready
NetCDF file for a specified region.

Key Features:
- Handles `cfgrib` coordinate conflicts by opening variables from different
  atmospheric levels (e.g., 2m, 10m, surface) separately from each GRIB file.
- Merges the data from different levels into a unified xarray Dataset.
- Clips the data to a specified bounding box.
- Combines daily processed files into a single master NetCDF file.
- Uses the `netcdf4` engine for robust NetCDF creation with compression.
"""

import os
import argparse
import logging
import warnings
import glob
from datetime import datetime
import xarray as xr
import numpy as np

# --- Configuration ---
CONFIG = {
    'BBOX_SOCAL': {
        "lat_min": 31.0, "lat_max": 38.0,
        "lon_min": 236.0, "lon_max": 245.0  # Using 0-360 longitude
    },
    'LEVEL_FILTERS': {
        "2m": {"typeOfLevel": "heightAboveGround", "level": 2},
        "10m": {"typeOfLevel": "heightAboveGround", "level": 10},
        "surface": {"typeOfLevel": "surface"}
    }
}

def setup_logging():
    """Configures logging to print to the console."""
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    warnings.filterwarnings('ignore', category=UserWarning, module='cfgrib')
    warnings.filterwarnings('ignore', category=FutureWarning)

def process_single_grib_file(grib_path, bbox):
    """
    Processes a single GRIB2 file, handling multiple coordinate levels.

    Args:
        grib_path (str): Path to the GRIB2 file.
        bbox (dict): Bounding box dictionary with lat/lon min/max.

    Returns:
        xarray.Dataset or None: A processed and clipped dataset, or None if processing fails.
    """
    level_datasets = []
    
    for level_name, filter_kwargs in CONFIG['LEVEL_FILTERS'].items():
        try:
            # Open the GRIB file, filtering for a specific level
            ds_level = xr.open_dataset(
                grib_path,
                engine="cfgrib",
                backend_kwargs={'filter_by_keys': filter_kwargs}
            )

            if not ds_level.data_vars:
                continue

            # Clip the dataset to the bounding box
            # Ensure we handle longitude coordinates correctly (0-360)
            mask_lat = (ds_level.latitude >= bbox['lat_min']) & (ds_level.latitude <= bbox['lat_max'])
            mask_lon = (ds_level.longitude >= bbox['lon_min']) & (ds_level.longitude <= bbox['lon_max'])
            
            clipped_ds = ds_level.where(mask_lat & mask_lon, drop=True)

            if clipped_ds.nbytes > 0:
                level_datasets.append(clipped_ds)
            
        except (ValueError, KeyError) as e:
            # This can happen if a level doesn't exist in the file
            logging.debug(f"Level '{level_name}' not found or failed to process in {os.path.basename(grib_path)}: {e}")
            continue
        except Exception as e:
            logging.error(f"An unexpected error occurred processing level '{level_name}' in {os.path.basename(grib_path)}: {e}", exc_info=True)
            continue

    if not level_datasets:
        logging.warning(f"No data could be extracted from {os.path.basename(grib_path)}.")
        return None

    # Merge the datasets from all levels. `compat='override'` handles differing coordinates.
    try:
        combined_ds = xr.merge(level_datasets, compat='override')
        logging.info(f"Successfully merged {len(level_datasets)} levels from {os.path.basename(grib_path)}.")
        return combined_ds
    except Exception as e:
        logging.error(f"Failed to merge levels for {os.path.basename(grib_path)}: {e}")
        return None

def main():
    """
    Main function to parse arguments and orchestrate the processing workflow.
    """
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Process HRRR GRIB2 files into a unified NetCDF.",
        epilog="Handles multi-level coordinate conflicts and clips data to a bounding box."
    )

    # --- Argument Parsing ---
    default_input_dir = os.path.join('data', 'raw', 'hrrr')
    default_output_file = os.path.join('data', 'processed', 'hrrr_socal_hourly.nc')

    parser.add_argument(
        '--input-dir',
        default=default_input_dir,
        help=f"Directory containing the downloaded GRIB2 files. Default: {default_input_dir}"
    )
    parser.add_argument(
        '--output-file',
        default=default_output_file,
        help=f"Path to save the final NetCDF file. Default: {default_output_file}"
    )

    args = parser.parse_args()

    # --- Main Logic ---
    logging.info("="*50)
    logging.info("      HRRR GRIB2 to NetCDF Processor")
    logging.info("="*50)

    # 1. Find GRIB2 files
    grib_files = sorted(glob.glob(os.path.join(args.input_dir, "**", "*.grib2"), recursive=True))
    if not grib_files:
        logging.error(f"No GRIB2 files found in {os.path.abspath(args.input_dir)}. Exiting.")
        return
    logging.info(f"Found {len(grib_files)} GRIB2 files to process.")

    # 2. Process each file individually
    processed_datasets = []
    for i, grib_path in enumerate(grib_files):
        logging.info(f"--- Processing file {i+1}/{len(grib_files)}: {os.path.basename(grib_path)} ---")
        dataset = process_single_grib_file(grib_path, CONFIG['BBOX_SOCAL'])
        if dataset:
            processed_datasets.append(dataset)

    if not processed_datasets:
        logging.error("No datasets were successfully processed. Cannot create NetCDF file.")
        return

    # 3. Combine all processed datasets into a single master dataset
    logging.info("="*50)
    logging.info(f"Combining {len(processed_datasets)} processed datasets into a single NetCDF file.")
    try:
        # Use concat to join along the 'time' dimension.
        final_ds = xr.concat(processed_datasets, dim="time")
        # Sort by time to ensure chronological order
        final_ds = final_ds.sortby('time')
    except Exception as e:
        logging.error(f"Failed to combine datasets: {e}", exc_info=True)
        logging.error("Aborting NetCDF creation.")
        return
        
    logging.info("Successfully combined all datasets.")
    logging.info(f"Final dataset dimensions: {final_ds.dims}")
    logging.info(f"Final dataset variables: {list(final_ds.data_vars)}")

    # 4. Save the final dataset to NetCDF
    output_dir = os.path.dirname(args.output_file)
    os.makedirs(output_dir, exist_ok=True)
    
    # Define encoding for compression. This reduces file size.
    encoding = {var: {'zlib': True, 'complevel': 5} for var in final_ds.data_vars}

    logging.info(f"Saving final NetCDF file to: {os.path.abspath(args.output_file)}")
    try:
        # **FIX**: Specify engine='netcdf4' to handle the encoding correctly.
        final_ds.to_netcdf(args.output_file, engine='netcdf4', encoding=encoding)
        logging.info("Successfully created the NetCDF file.")
    except Exception as e:
        logging.error(f"Failed to save NetCDF file: {e}", exc_info=True)

    logging.info("="*50)
    logging.info("Processing complete.")
    logging.info("="*50)

if __name__ == '__main__':
    main()
