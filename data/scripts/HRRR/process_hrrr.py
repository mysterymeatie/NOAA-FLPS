#!/usr/bin/env python3
"""
HRRR GRIB2 to NetCDF Converter

Converts HRRR GRIB2 files to NetCDF4 format while preserving all 8 feature layers,
cropping to Southern California region, and maintaining 3km resolution.

Variables processed:
- TMP: Temperature (2m above ground)
- DPT: Dew Point Temperature (2m above ground) 
- RH: Relative Humidity (2m above ground)
- SPFH: Specific Humidity (2m above ground)
- UGRD: U-Wind Component (10m above ground)
- VGRD: V-Wind Component (10m above ground)
- WIND: Wind Speed (10m above ground)
- PRATE: Precipitation Rate (surface)
"""

import os
import sys
import logging
import glob
import argparse
import warnings
from datetime import datetime
import xarray as xr
import cfgrib
import numpy as np

# Silence xarray FutureWarning about timedelta decoding
warnings.filterwarnings("ignore", 
                       message="In a future version, xarray will not decode timedelta values.*",
                       category=FutureWarning)

# (Optional) utils path if needed later
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

# Configuration - Map cfgrib variable names to standard names
HRRR_VARIABLES = {
    # cfgrib variable name: (standard name, description, dataset_index)
    't2m': ('TMP', 'Temperature 2m above ground', 1),
    'd2m': ('DPT', 'Dew Point Temperature 2m above ground', 1),
    'r2': ('RH', 'Relative Humidity 2m above ground', 1),
    'sh2': ('SPFH', 'Specific Humidity 2m above ground', 1),
    'u10': ('UGRD', 'U-Wind Component 10m above ground', 0),
    'v10': ('VGRD', 'V-Wind Component 10m above ground', 0),
    'max_10si': ('WIND', 'Wind Speed 10m above ground', 0),
    'prate': ('PRATE', 'Precipitation Rate surface', 2)
}

def setup_logging():
    """Configures logging to print to the console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def _open_cfgrib_as_dataset(grib_file_path: str) -> xr.Dataset:
    """Open a GRIB2 file with cfgrib and return a unified, LOADED xarray Dataset of 8 variables with coords.

    Note: Data are eagerly loaded to detach from cfgrib/eccodes backends. This allows safe backend closure
    and avoids accumulating open file handles/memory contexts during long multi-file runs.
    """
    filename = os.path.basename(grib_file_path)
    logging.info(f"Processing: {filename}")

    datasets = cfgrib.open_datasets(grib_file_path)
    if not datasets:
        raise RuntimeError(f"No datasets could be loaded from {filename}")

    logging.info(f"  Found {len(datasets)} datasets in GRIB file")

    combined_vars = {}
    reference_coords = None

    for i, ds in enumerate(datasets):
        logging.info(f"  Dataset {i}: {list(ds.data_vars.keys())}")
        if reference_coords is None:
            reference_coords = ds
        for cfgrib_var in ds.data_vars:
            if cfgrib_var in HRRR_VARIABLES:
                standard_name, description, _expected_dataset = HRRR_VARIABLES[cfgrib_var]
                var = ds[cfgrib_var]
                if 'heightAboveGround' in var.coords:
                    var = var.drop_vars('heightAboveGround')
                combined_vars[standard_name] = var
                combined_vars[standard_name].attrs['description'] = description
                combined_vars[standard_name].attrs['cfgrib_name'] = cfgrib_var
                logging.info(f"    {cfgrib_var} -> {standard_name}")

    if not combined_vars:
        # Ensure we close datasets before raising
        for src in datasets:
            try:
                src.close()
            except Exception:
                pass
        raise RuntimeError(f"No recognized variables found in {filename}")

    ds_out = xr.Dataset(combined_vars)
    ds_out = ds_out.assign_coords({
        'latitude': reference_coords.latitude,
        'longitude': reference_coords.longitude,
        'time': reference_coords.time
    })
    # Drop cfgrib's auxiliary coords that may hinder concat
    for drop_name in ['step', 'surface']:
        if drop_name in ds_out.coords:
            ds_out = ds_out.drop_vars(drop_name)
    # Eagerly load to detach from cfgrib backend
    ds_out.load()
    # Close source datasets to free eccodes contexts
    for src in datasets:
        try:
            src.close()
        except Exception:
            pass
    return ds_out


def _compute_crop_slices(lat2: np.ndarray, lon2: np.ndarray, bbox: dict) -> tuple:
    """Compute rectangular y/x slices that cover the bbox on 2D lat/lon arrays."""
    lon_min_360 = bbox['lon_min'] + 360 if bbox['lon_min'] < 0 else bbox['lon_min']
    lon_max_360 = bbox['lon_max'] + 360 if bbox['lon_max'] < 0 else bbox['lon_max']
    mask = (
        (lat2 >= bbox['lat_min']) & (lat2 <= bbox['lat_max']) &
        (lon2 >= lon_min_360) & (lon2 <= lon_max_360)
    )
    ys, xs = np.where(mask)
    if ys.size == 0 or xs.size == 0:
        raise RuntimeError("Crop bbox produced empty selection. Check coordinates and bbox.")
    y0, y1 = int(ys.min()), int(ys.max()) + 1
    x0, x1 = int(xs.min()), int(xs.max()) + 1
    return slice(y0, y1), slice(x0, x1)


def process_all_to_single_file(input_dir: str, output_file: str, start_year: int, end_year: int) -> None:
    """Process all GRIB2 files across years, crop to bbox, and save one combined NetCDF (all timesteps)
    using streamed append to avoid memory/handle exhaustion.
    """
    setup_logging()
    logging.info("=" * 60)
    logging.info(" HRRR GRIB2 → Single NetCDF (cropped SoCal)")
    logging.info("=" * 60)

    # Gather all GRIB2 files between years
    all_gribs = []
    for entry in sorted(os.listdir(input_dir)):
        if not entry.isdigit():
            continue
        year = int(entry[:4])
        if year < start_year or year > end_year:
            continue
        day_dir = os.path.join(input_dir, entry)
        if not os.path.isdir(day_dir):
            continue
        files = sorted(glob.glob(os.path.join(day_dir, "*.grib2")))
        all_gribs.extend(files)

    if not all_gribs:
        logging.error(f"No GRIB2 files found in {input_dir} for years {start_year}-{end_year}")
        return

    logging.info(f"Total GRIB2 files to process: {len(all_gribs)}")

    bbox = {
        'lon_min': -118.8, 'lon_max': -116.8,
        'lat_min': 32.7, 'lat_max': 34.5
    }

    crop_yx = None
    # NetCDF streaming setup
    nc_initialized = False
    time_index = 0

    for idx, grib_path in enumerate(all_gribs, 1):
        logging.info(f"[{idx}/{len(all_gribs)}] {os.path.basename(grib_path)}")
        day_ds = _open_cfgrib_as_dataset(grib_path)

        # Determine crop slices once from the first file for consistency (ensures identical 78x75 grid)
        if crop_yx is None:
            lat2 = day_ds['latitude'].values
            lon2 = day_ds['longitude'].values
            # Handle potential 3D lat/lon from cfgrib by taking first time slice
            if day_ds['latitude'].ndim == 3:
                lat2 = lat2[0, :, :]
                lon2 = lon2[0, :, :]
            y_slice, x_slice = _compute_crop_slices(lat2, lon2, bbox)
            crop_yx = (y_slice, x_slice)
            logging.info(f"  Crop slices determined: y={y_slice}, x={x_slice}")

            # Initialize NetCDF on first crop using 2D lat/lon grid, unlimited time
            from netCDF4 import Dataset as NC
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            nc = NC(output_file, mode='w')
            ny = y_slice.stop - y_slice.start
            nx = x_slice.stop - x_slice.start
            nc.createDimension('time', None)
            nc.createDimension('y', ny)
            nc.createDimension('x', nx)

            # Coordinates
            vtime = nc.createVariable('time', 'f8', ('time',))
            vtime.units = 'seconds since 1970-01-01 00:00:00'
            vtime.long_name = 'time'

            # Store 2D lat/lon once
            lat2_use = lat2[y_slice, x_slice]
            lon2_use = lon2[y_slice, x_slice]
            vlat = nc.createVariable('latitude', 'f4', ('y', 'x'), zlib=True, complevel=4)
            vlon = nc.createVariable('longitude', 'f4', ('y', 'x'), zlib=True, complevel=4)
            vlat[:, :] = lat2_use.astype('float32')
            vlon[:, :] = lon2_use.astype('float32')
            vlat.units = 'degrees_north'; vlon.units = 'degrees_east'

            # Data variables
            for v in ['TMP','DPT','RH','SPFH','UGRD','VGRD','WIND','PRATE']:
                nc.createVariable(v, 'f4', ('time', 'y', 'x'), zlib=True, complevel=4)

            # Global attributes
            nc.title = 'HRRR Daily (21Z) - Southern California Cropped'
            nc.description = 'All available HRRR timesteps cropped to SoCal bbox; variables standardized to 8 core layers.'
            nc.source = 'NOAA/NCEP/EMC (HRRR via GRIB2 → cfgrib)'
            nc.bbox = f"{bbox['lon_min']}, {bbox['lat_min']}, {bbox['lon_max']}, {bbox['lat_max']}"
            nc.resolution = '3km (cropped to ~78x75 grid)'
            nc.conventions = 'CF-1.6'
            nc.sync(); nc.close()
            nc_initialized = True

        y_slice, x_slice = crop_yx
        cropped = day_ds.isel(y=y_slice, x=x_slice)
        # Eager load to release xarray graph
        cropped.load()

        # Append to NetCDF
        from netCDF4 import Dataset as NC
        nc = NC(output_file, mode='a')
        # Write time as seconds since epoch
        # Extract scalar time value
        tval = np.array(cropped['time']).astype('datetime64[s]')
        epoch = np.datetime64('1970-01-01T00:00:00', 's')
        tsec = int((tval - epoch).astype('int64'))
        nc.variables['time'][time_index] = tsec
        # Write variables
        for v in ['TMP','DPT','RH','SPFH','UGRD','VGRD','WIND','PRATE']:
            if v in cropped:
                nc.variables[v][time_index, :, :] = cropped[v].values.astype('float32')
        nc.sync(); nc.close()

        # Cleanup
        del day_ds
        del cropped
        time_index += 1

    logging.info("Done.")

def process_hrrr_directory(input_dir: str, output_file: str, start_year: int, end_year: int) -> None:
    """Wrapper to build one combined cropped HRRR NetCDF from GRIB2 files across years."""
    process_all_to_single_file(input_dir=input_dir, output_file=output_file, start_year=start_year, end_year=end_year)

def main():
    """Main function with argument parsing."""
    parser = argparse.ArgumentParser(description="Convert HRRR GRIB2 files to ONE combined NetCDF (SoCal crop)")
    
    parser.add_argument(
        '--input-dir', 
        default='data/raw/NOAA_HRRR/hrrr',
        help='Input directory containing GRIB2 files (default: data/raw/NOAA_HRRR/hrrr)'
    )
    
    parser.add_argument(
        '--output-file',
        default='data/processed/HRRR/hrrr_daily.nc',
        help='Output NetCDF file (default: data/processed/HRRR/hrrr_daily.nc)'
    )
    parser.add_argument('--start-year', type=int, default=2016, help='Start year (default: 2016)')
    parser.add_argument('--end-year', type=int, default=2025, help='End year inclusive (default: 2025)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_dir):
        print(f"Error: Input directory does not exist: {args.input_dir}")
        return 1
    
    process_hrrr_directory(
        input_dir=args.input_dir,
        output_file=args.output_file,
        start_year=args.start_year,
        end_year=args.end_year,
    )
    return 0

if __name__ == "__main__":
    exit(main())