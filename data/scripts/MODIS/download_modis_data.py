# -*- coding: utf-8 -*-
"""
  This script is configured to download a specific NASA satellite data product:

   * Product: MOD13Q1 - This is the "MODIS/Terra Vegetation Indices 16-Day L3 Global 250m" product. It
     provides two key measurements of vegetation health:
       * NDVI (Normalized Difference Vegetation Index): A measure of vegetation greenness.
       * EVI (Enhanced Vegetation Index): An optimized version of NDVI that is more sensitive in areas with
         high biomass.
   * Resolution: The data has a spatial resolution of 250 meters per pixel.
   * Temporal Resolution: A new image is produced every 16 days.
   * Tile: It specifically downloads data for tile `h08v05`, which covers Southern California, including the
     Santiago Canyon study area.
   * Date Range: By default, it downloads all available data for this tile from the beginning of the mission
     (February 18, 2000) to the current date.
     
Other products can be downloaded by tweaking the parameters in the configuration down below.

**Prerequisites:**

1. A NASA Earthdata Login account. You can register here:
   https://urs.earthdata.nasa.gov/users/new

2. A .netrc file in your home directory with your credentials.
   This script will guide you if one is not found.
"""

import os
import argparse
import sys
import logging
from datetime import datetime
import earthaccess

# --- Configuration ---
CONFIG = {
    'PRODUCT_SHORT_NAME': 'MOD13Q1',
    'TILE_BOUNDING_BOX': (-111.3, 29.9, -100.1, 40.1), # Bounding box for tile h08v05
    'DEFAULT_START_DATE': '2000-02-18', # First day of MODIS data
}

def setup_logging():
    """Configures logging to print to the console."""
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

def main():
    """
    Main function to parse arguments and orchestrate the download process.
    """
    setup_logging()

    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(
        description="Download MODIS data from NASA LAADS DAAC using earthaccess.",
        epilog="Authentication is handled by a .netrc file."
    )
    
    # Use relative paths for better portability
    if os.path.exists(os.path.join('data', 'raw')):
        # Running from project root
        default_output_dir = os.path.join('data', 'raw', 'MODIS_NDVI_250m')
    else:
        # Running from scripts directory
        default_output_dir = os.path.join('..', 'raw', 'MODIS_NDVI_250m')

    parser.add_argument(
        '--output-dir', 
        default=default_output_dir,
        help=f"Directory to save the downloaded files. Default: {default_output_dir}"
    )
    parser.add_argument(
        '--start-date', 
        default=CONFIG['DEFAULT_START_DATE'],
        help=f"Start date in YYYY-MM-DD format. Default: {CONFIG['DEFAULT_START_DATE']}"
    )
    parser.add_argument(
        '--end-date', 
        default=datetime.now().strftime('%Y-%m-%d'),
        help="End date in YYYY-MM-DD format. Default: today."
    )
    parser.add_argument(
        '--product', 
        default=CONFIG['PRODUCT_SHORT_NAME'],
        help=f"MODIS product short name (e.g., MOD13Q1). Default: {CONFIG['PRODUCT_SHORT_NAME']}"
    )

    args = parser.parse_args()

    # --- Main Logic ---
    logging.info("="*50)
    logging.info("MODIS Data Downloader using earthaccess")
    logging.info("="*50)

    # 1. Authenticate with NASA Earthdata
    try:
        logging.info("Attempting to log in to NASA Earthdata using .netrc file...")
        auth = earthaccess.login(strategy="netrc")
        if not auth.authenticated:
            logging.error("Authentication failed. Please ensure you have a valid .netrc file in your home directory.")
            logging.error("See documentation for details: https://earthaccess.readthedocs.io/en/latest/tutorials/getting-started/")
            sys.exit(1)
        logging.info("Successfully authenticated with NASA Earthdata.")
    except Exception as e:
        logging.error(f"An error occurred during authentication: {e}")
        sys.exit(1)

    # 2. Search for the data
    try:
        logging.info(f"Searching for {args.product} data within the tile's bounding box from {args.start_date} to {args.end_date}...")
        results = earthaccess.search_data(
            short_name=args.product,
            bounding_box=CONFIG['TILE_BOUNDING_BOX'],
            temporal=(args.start_date, args.end_date),
            count=-1 # Get all results
        )
        if not results:
            logging.warning("No data found for the specified criteria.")
            sys.exit(0)
        logging.info(f"Found {len(results)} total files in the region.")

        # Filter results to keep only the desired tile
        tile_str = ".h08v05."
        filtered_results = [res for res in results if tile_str in res.data_links(access='direct_url')[0]]
        
        if not filtered_results:
            logging.warning(f"No files found specifically for tile h08v05 after filtering.")
            sys.exit(0)
            
        logging.info(f"Found {len(filtered_results)} files for tile h08v05 to download.")

    except Exception as e:
        logging.error(f"An error occurred during data search: {e}")
        sys.exit(1)

    # 3. Download the data
    os.makedirs(args.output_dir, exist_ok=True)
    logging.info(f"Downloading files to: {os.path.abspath(args.output_dir)}")
    
    try:
        earthaccess.download(filtered_results, local_path=args.output_dir)
    except Exception as e:
        logging.error(f"An error occurred during download: {e}")
        sys.exit(1)

    logging.info("="*50)
    logging.info("Download process complete.")
    logging.info("="*50)

if __name__ == '__main__':
    main()