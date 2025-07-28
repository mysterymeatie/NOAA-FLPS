# -*- coding: utf-8 -*-
"""
This script processes MODIS MOD13Q1 vegetation index data in HDF format,
converting each file into two analysis-ready, clipped GeoTIFF files (NDVI & EVI).

Key Features:
- Reads a study area polygon from a GeoJSON file.
- Processes multiple HDF files in parallel for maximum efficiency.
- Assigns the correct Coordinate Reference System (CRS) to the raw data.
- Clips the satellite imagery precisely to the study area boundary.
- Applies quality filters and scaling to the data.
- Outputs clean, spatially-referenced GeoTIFF files.
"""

import os
import glob
import re
import warnings
import logging
import argparse
from datetime import datetime, timedelta
import sys

# --- GDAL Environment Fix ---
# Attempt to fix GDAL data path issue for pyogrio before importing geopandas
if 'CONDA_PREFIX' in os.environ:
    gdal_data_path = os.path.join(os.environ['CONDA_PREFIX'], 'Library', 'share', 'gdal')
    if os.path.exists(gdal_data_path):
        os.environ['GDAL_DATA'] = gdal_data_path
    else:
        # Also check sys.prefix for base environments
        gdal_data_path_sys = os.path.join(sys.prefix, 'Library', 'share', 'gdal')
        if os.path.exists(gdal_data_path_sys):
            os.environ['GDAL_DATA'] = gdal_data_path_sys

import geopandas as gpd
import rioxarray
import rasterio
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- Configuration ---
CONFIG = {
    'SCALE_FACTOR': 0.0001,
    'MODIS_PROJ': "+proj=sinu +R=6371007.181 +nadgrids=@null +wktext +units=m +no_defs",
    'DATE_REGEX': re.compile(r'\.A(\d{4})(\d{3})\.'),
    'VALID_QA_VALUES': [0],  # 0 = Good data, use with confidence
}

# --- Utility Functions ---

def setup_logging():
    """Configures logging to print to the console."""
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    warnings.filterwarnings("ignore", category=FutureWarning)

def parse_modis_filename(filename):
    """Parses MODIS filename to extract year and day of year."""
    basename = os.path.basename(filename)
    date_match = CONFIG['DATE_REGEX'].search(basename)
    if date_match:
        year = int(date_match.group(1))
        doy = int(date_match.group(2))
        return year, doy
    return None, None

def doy_to_date(year, doy):
    """Converts year and day-of-year to a calendar date string."""
    return (datetime(year, 1, 1) + timedelta(days=doy - 1)).strftime('%Y-%m-%d')

# --- Core Processing Function ---

def process_single_file(hdf_path, study_area_geom, output_dir_ndvi, output_dir_evi):
    """
    Processes a single HDF file into clipped NDVI and EVI GeoTIFFs.
    """
    try:
        # 1. Generate output filenames
        year, doy = parse_modis_filename(hdf_path)
        if not year:
            logging.warning(f"Could not parse date from {os.path.basename(hdf_path)}, skipping.")
            return None
        date_str = doy_to_date(year, doy)
        
        base_filename = f"{os.path.basename(hdf_path).split('.hdf')[0]}"
        output_path_ndvi = os.path.join(output_dir_ndvi, f"{base_filename}_NDVI.tif")
        output_path_evi = os.path.join(output_dir_evi, f"{base_filename}_EVI.tif")

        # If both files already exist, skip processing
        if os.path.exists(output_path_ndvi) and os.path.exists(output_path_evi):
            return f"Skipped (already exists): {base_filename}"

        # Convert to an absolute path and sanitize for GDAL
        hdf_path_abs = os.path.abspath(hdf_path)
        hdf_path_sanitized = hdf_path_abs.replace('\\', '/')

        # 2. Open HDF subdatasets using rioxarray
        # Open HDF subdatasets using rasterio to get the subdataset list first.
        # This is more robust than hardcoding the internal dataset paths.
        with rasterio.open(hdf_path_sanitized) as src:
            subdatasets = src.subdatasets

        # Find the correct subdataset URI for each variable
        def find_subdataset(subdatasets, name):
            for sds in subdatasets:
                if name in sds:
                    return sds
            raise ValueError(f"Subdataset containing '{name}' not found.")

        ndvi_uri = find_subdataset(subdatasets, "NDVI")
        evi_uri = find_subdataset(subdatasets, "EVI")
        qa_uri = find_subdataset(subdatasets, "pixel reliability")

        # Now open the specific subdatasets with rioxarray
        ndvi_da = rioxarray.open_rasterio(ndvi_uri, masked=True)
        evi_da = rioxarray.open_rasterio(evi_uri, masked=True)
        qa_da = rioxarray.open_rasterio(qa_uri, masked=True)

        # 3. Assign CRS and Clip
        # Assign the native MODIS Sinusoidal projection
        ndvi_da = ndvi_da.rio.write_crs(CONFIG['MODIS_PROJ'])
        evi_da = evi_da.rio.write_crs(CONFIG['MODIS_PROJ'])
        qa_da = qa_da.rio.write_crs(CONFIG['MODIS_PROJ'])
        
        # Clip the rasters to the study area geometry
        ndvi_clipped = ndvi_da.rio.clip([study_area_geom], all_touched=True)
        evi_clipped = evi_da.rio.clip([study_area_geom], all_touched=True)
        qa_clipped = qa_da.rio.clip([study_area_geom], all_touched=True)

        # 4. Apply Quality Filter and Scaling
        # Create a mask where the QA values are valid
        quality_mask = qa_clipped.isin(CONFIG['VALID_QA_VALUES'])
        
        # Apply the mask and the scale factor. Where the mask is False, pixels become NaN.
        ndvi_final = ndvi_clipped.where(quality_mask) * CONFIG['SCALE_FACTOR']
        evi_final = evi_clipped.where(quality_mask) * CONFIG['SCALE_FACTOR']

        # 5. Save to GeoTIFF
        ndvi_final.rio.to_raster(output_path_ndvi, compress='LZW', dtype='float32')
        evi_final.rio.to_raster(output_path_evi, compress='LZW', dtype='float32')

        return f"Successfully processed: {base_filename}"

    except Exception as e:
        logging.error(f"Error processing {os.path.basename(hdf_path)}: {e}", exc_info=False)
        return None

# --- Main Batch Processor ---

def main():
    """Parses command-line arguments and runs the processor."""
    setup_logging()
    
    parser = argparse.ArgumentParser(description="MODIS HDF to GeoTIFF Processor.")

    # --- Set up default paths ---
    # This logic allows the script to be run from the project root or its own directory
    if os.path.exists(os.path.join('data', 'raw', 'MODIS_NDVI_250m')):
        # Running from project root
        DEFAULT_DATA_PATH = os.path.join('data', 'raw', 'MODIS_NDVI_250m')
        DEFAULT_GEOJSON_PATH = os.path.join('data', 'santiago.geojson')
        DEFAULT_OUTPUT_DIR_NDVI = os.path.join('data', 'processed', 'geotiff_ndvi')
        DEFAULT_OUTPUT_DIR_EVI = os.path.join('data', 'processed', 'geotiff_evi')
    else:
        # Running from scripts directory
        DEFAULT_DATA_PATH = os.path.join('..', 'raw', 'MODIS_NDVI_250m')
        DEFAULT_GEOJSON_PATH = os.path.join('..', 'santiago.geojson')
        DEFAULT_OUTPUT_DIR_NDVI = os.path.join('..', 'processed', 'geotiff_ndvi')
        DEFAULT_OUTPUT_DIR_EVI = os.path.join('..', 'processed', 'geotiff_evi')

    parser.add_argument('--data_path', type=str, default=DEFAULT_DATA_PATH)
    parser.add_argument('--geojson_path', type=str, default=DEFAULT_GEOJSON_PATH)
    parser.add_argument('--output_dir_ndvi', type=str, default=DEFAULT_OUTPUT_DIR_NDVI)
    parser.add_argument('--output_dir_evi', type=str, default=DEFAULT_OUTPUT_DIR_EVI)
    parser.add_argument('--max_workers', type=int, default=None)
    args = parser.parse_args()

    logging.info("="*60)
    logging.info("      MODIS HDF to GeoTIFF Processor")
    logging.info("="*60)

    # --- Pre-run validation ---
    for path in [args.data_path, args.geojson_path]:
        if not os.path.exists(path):
            logging.error(f"Input path not found: {path}")
            return
    for path in [args.output_dir_ndvi, args.output_dir_evi]:
        os.makedirs(path, exist_ok=True)

    # --- Load and prepare study area geometry ---
    logging.info(f"Loading study area from: {os.path.basename(args.geojson_path)}")
    study_area_gdf = gpd.read_file(args.geojson_path)
    # Transform the study area CRS to match the MODIS data CRS for clipping
    study_area_geom_modis_crs = study_area_gdf.to_crs(CONFIG['MODIS_PROJ']).geometry.iloc[0]
    
    hdf_files = sorted(glob.glob(os.path.join(args.data_path, "*.hdf")))
    if not hdf_files:
        logging.error("No HDF files found. Check the data path.")
        return
    logging.info(f"Found {len(hdf_files)} HDF files to process.")

    # --- Parallel Processing ---
    max_workers = args.max_workers or os.cpu_count()
    logging.info(f"Starting parallel processing with up to {max_workers} workers...")
    
    processed_count = 0
    skipped_count = 0
    error_count = 0

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_single_file, hdf, study_area_geom_modis_crs, args.output_dir_ndvi, args.output_dir_evi): hdf
            for hdf in hdf_files
        }
        
        total_files = len(hdf_files)
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                logging.info(f"({i + 1}/{total_files}) - {result}")
                if "Successfully processed" in result:
                    processed_count += 1
                elif "Skipped" in result:
                    skipped_count += 1
            else:
                error_count += 1

    logging.info("="*50)
    logging.info("Processing Summary:")
    logging.info(f"  Successfully processed: {processed_count} files")
    logging.info(f"  Skipped (already exist): {skipped_count} files")
    logging.info(f"  Errors: {error_count} files")
    logging.info(f"Total files processed this run: {processed_count}")
    logging.info("="*50)

if __name__ == '__main__':
    main()