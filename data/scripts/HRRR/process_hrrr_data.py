# -*- coding: utf-8 -*-
"""
This script processes downloaded HRRR GRIB2 files into yearly, analysis-ready
NetCDF files for a specified region.

Key Features:
- Chunks output into separate NetCDF files by year for manageability and performance.
- Handles `cfgrib` coordinate conflicts by opening variables from different
  atmospheric levels (e.g., 2m, 10m, surface) separately from each GRIB file.
- Gracefully skips corrupted or incomplete GRIB2 files.
- Merges the data from different levels into a unified xarray Dataset.
- Clips the data to a specified bounding box.
- Uses the `netcdf4` engine for robust NetCDF creation with compression.
"""

import os
import argparse
import logging
import warnings
import glob
from datetime import datetime
import xarray as xr
import pandas as pd
# Import the specific error from the gribapi library
from gribapi.errors import PrematureEndOfFileError

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
    Processes a single GRIB2 file, handling multiple coordinate levels and
    corrupted files.
    """
    level_datasets = []
    try:
        for level_name, filter_kwargs in CONFIG['LEVEL_FILTERS'].items():
            try:
                ds_level = xr.open_dataset(
                    grib_path,
                    engine="cfgrib",
                    backend_kwargs={'filter_by_keys': filter_kwargs}
                )
                if ds_level.data_vars:
                    mask_lat = (ds_level.latitude >= bbox['lat_min']) & (ds_level.latitude <= bbox['lat_max'])
                    mask_lon = (ds_level.longitude >= bbox['lon_min']) & (ds_level.longitude <= bbox['lon_max'])
                    clipped_ds = ds_level.where(mask_lat & mask_lon, drop=True)
                    if clipped_ds.nbytes > 0:
                        level_datasets.append(clipped_ds)
            except Exception:
                # This will catch errors if a specific level is missing, which is fine.
                continue
        
        if not level_datasets:
            return None

        return xr.merge(level_datasets, compat='override')

    except (PrematureEndOfFileError, EOFError):
        logging.warning(f"CORRUPTED FILE: Could not process {os.path.basename(grib_path)}. It may be incomplete. Skipping.")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while processing {os.path.basename(grib_path)}: {e}")
        return None

def main():
    """
    Main function to parse arguments and orchestrate the processing workflow.
    """
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Process HRRR GRIB2 files into yearly NetCDF files.",
        epilog="Groups raw GRIB2 files by year and creates a separate NetCDF for each."
    )

    default_input_dir = os.path.join('data', 'raw', 'NOAA_HRRR')
    default_output_dir = os.path.join('data', 'processed', 'HRRR')

    parser.add_argument('--input-dir', default=default_input_dir, help=f"Input directory for GRIB2 files. Default: {default_input_dir}")
    parser.add_argument('--output-dir', default=default_output_dir, help=f"Output directory for NetCDF files. Default: {default_output_dir}")

    args = parser.parse_args()

    logging.info("="*50)
    logging.info("      HRRR GRIB2 to Yearly NetCDF Processor")
    logging.info("="*50)

    grib_files = sorted(glob.glob(os.path.join(args.input_dir, "**", "*.grib2"), recursive=True))
    if not grib_files:
        logging.error(f"No GRIB2 files found in {os.path.abspath(args.input_dir)}. Exiting.")
        return
    logging.info(f"Found {len(grib_files)} total GRIB2 files to process.")

    files_by_year = {}
    for f in grib_files:
        try:
            year = os.path.basename(os.path.dirname(f))[:4]
            if year not in files_by_year:
                files_by_year[year] = []
            files_by_year[year].append(f)
        except IndexError:
            logging.warning(f"Could not determine year for file {f}. Skipping.")

    logging.info(f"Found data spanning {len(files_by_year)} years: {sorted(files_by_year.keys())}")

    os.makedirs(args.output_dir, exist_ok=True)
    total_success = 0

    for year, file_list in files_by_year.items():
        logging.info("="*50)
        logging.info(f"Processing year: {year} ({len(file_list)} files)")

        processed_datasets = []
        for i, grib_path in enumerate(file_list):
            logging.debug(f"--- Processing file {i+1}/{len(file_list)}: {os.path.basename(grib_path)} ---")
            dataset = process_single_grib_file(grib_path, CONFIG['BBOX_SOCAL'])
            if dataset:
                processed_datasets.append(dataset)

        if not processed_datasets:
            logging.warning(f"No valid datasets were successfully processed for {year}. Skipping year.")
            continue

        try:
            final_ds = xr.concat(processed_datasets, dim="time", coords="minimal", compat="override").sortby('time')
            logging.info(f"Successfully combined {len(processed_datasets)} valid datasets for {year}.")
        except Exception as e:
            logging.error(f"Failed to combine datasets for {year}: {e}", exc_info=True)
            continue
        
        output_filename = f"hrrr_socal_hourly_{year}.nc"
        output_path = os.path.join(args.output_dir, output_filename)
        encoding = {var: {'zlib': True, 'complevel': 5} for var in final_ds.data_vars}

        logging.info(f"Saving yearly NetCDF file to: {os.path.abspath(output_path)}")
        try:
            final_ds.to_netcdf(output_path, engine='netcdf4', encoding=encoding)
            logging.info(f"Successfully created NetCDF file for {year}.")
            total_success += 1
        except Exception as e:
            logging.error(f"Failed to save NetCDF file for {year}: {e}", exc_info=True)

    logging.info("="*50)
    logging.info("Processing complete.")
    logging.info(f"Successfully created {total_success}/{len(files_by_year)} yearly NetCDF files.")
    logging.info("="*50)

if __name__ == '__main__':
    main()
