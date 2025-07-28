# -*- coding: utf-8 -*-
"""
This script downloads High-Resolution Rapid Refresh (HRRR) data from NOAA's
archives using a single, efficient FastHerbie object.

You can find the documentation to the Herbie library here: https://herbie-data.readthedocs.io/en/latest/

Key Features:
- Downloads variables from all atmospheric levels (2m, 10m, surface) at once.
- Uses a single combined regex pattern with non-capturing groups for efficiency
  and to prevent warnings.
- Downloads data in monthly batches to prevent hanging on problematic old files.
- Overwrites existing GRIB2 files to ensure data is fresh.
- Provides a live status update every 60 seconds during download.
- Gracefully handles errors when index files are missing for old data.
- Allows specifying a custom date range, time, interval, and data source,
  or using a convenient default setting.
  
When run with --use-defaults, the size of the downloaded data is ~32GB.
"""

import os
import argparse
import logging
import warnings
import time
import threading
import glob
from datetime import datetime
import pandas as pd
from herbie import FastHerbie

# --- Configuration ---
CONFIG = {
    'MODEL': 'hrrr',
    'PRODUCT': 'sfc',
    'FXX': 0,  # Forecast hour 0 (analysis)
    'ALL_LEVELS_PATTERN': r":(?:TMP|DPT|RH|SPFH):2 m above ground|:(?:UGRD|VGRD|WIND):10 m above ground|:PRATE:surface"
}

def setup_logging():
    """Configures logging to print to the console."""
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    warnings.filterwarnings("ignore", message="This pattern is interpreted as a regular expression, and has match groups.", category=UserWarning)
    warnings.filterwarnings('ignore', category=FutureWarning)

def download_worker(fh, pattern, result_container):
    """Worker function to run the download in a separate thread."""
    try:
        downloaded_files = fh.download(pattern)
        result_container['files'] = downloaded_files
    except Exception as e:
        result_container['error'] = e

def main():
    """
    Main function to parse arguments and orchestrate the download process.
    """
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Download HRRR data from NOAA's archives using Herbie.",
        epilog="Example for custom range: --start-date 2022-01-01 --end-date 2022-01-02 --time-utc 18:00 --date-interval 6H"
    )

    default_output_dir = os.path.join('data', 'raw', 'NOAA_HRRR')
    
    custom_args = parser.add_argument_group('Custom Run Arguments')
    custom_args.add_argument('--output-dir', default=default_output_dir, help=f"Directory to save files. Default: {default_output_dir}")
    custom_args.add_argument('--start-date', help="Start date in YYYY-MM-DD format.")
    custom_args.add_argument('--end-date', help="End date in YYYY-MM-DD format.")
    custom_args.add_argument('--time-utc', default='21:00', help="Time of day in UTC (HH:MM). Default: 21:00 (1 PM PST).")
    custom_args.add_argument('--date-interval', default='1D', help="Pandas frequency string (e.g., '1D', '12H'). Default: '1D'.")
    custom_args.add_argument('--source', default='aws', choices=['aws', 'google', 'pando', 'pando2', 'nomads'], help="Data source. Default: 'aws'.")
    parser.add_argument('--use-defaults', action='store_true', help="Use default settings: daily data at 1 PM PST from 2014-07-01 to today.")

    args = parser.parse_args()

    logging.info("="*50)
    logging.info("      HRRR GRIB2 Data Downloader")
    logging.info("="*50)

    if args.use_defaults:
        start_str, end_str, time_str, interval_str = "2014-07-01", datetime.now().strftime('%Y-%m-%d'), "21:00", "1D"
        logging.info("Using default configuration.")
    else:
        if not args.start_date or not args.end_date:
            parser.error("--start-date and --end-date are required unless --use-defaults is specified.")
        start_str, end_str, time_str, interval_str = args.start_date, args.end_date, args.time_utc, args.date_interval
        logging.info("Using custom configuration.")

    try:
        start_dt = datetime.strptime(f"{start_str} {time_str}", '%Y-%m-%d %H:%M')
        end_dt = datetime.strptime(f"{end_str} {time_str}", '%Y-%m-%d %H:%M')
        all_dates = pd.to_datetime(pd.date_range(start_dt, end_dt, freq=interval_str))
        logging.info(f"Generated {len(all_dates)} total timestamps for the specified range.")
    except ValueError as e:
        logging.error(f"Invalid date or time format: {e}")
        return

    os.makedirs(args.output_dir, exist_ok=True)
    logging.info(f"Output directory: {os.path.abspath(args.output_dir)}")
    logging.info(f"Data source: {args.source.upper()}")

    monthly_periods = all_dates.to_period('M').unique()
    grouped_dates = [all_dates[all_dates.to_period('M') == period] for period in monthly_periods]
    total_downloaded_count = 0

    for i, month_batch in enumerate(grouped_dates):
        batch_name = month_batch[0].strftime('%Y-%m')
        logging.info("="*50)
        logging.info(f"Processing Batch {i+1}/{len(grouped_dates)}: {batch_name}")

        for date in month_batch:
            date_folder = os.path.join(args.output_dir, date.strftime('%Y%m%d'))
            if os.path.isdir(date_folder):
                existing_files = glob.glob(os.path.join(date_folder, "*.grib2"))
                if existing_files:
                    logging.info(f"Found existing GRIB2 file in {date_folder}. Deleting to replace.")
                    for f in existing_files:
                        os.remove(f)
        
        try:
            fh = FastHerbie(
                month_batch,
                model=CONFIG['MODEL'],
                product=CONFIG['PRODUCT'],
                fxx=[CONFIG['FXX']],
                save_dir=args.output_dir,
                source=args.source
            )
            logging.info(f"Initialized FastHerbie for {len(fh.objects)} potential files in batch {batch_name}.")
            
            result_container = {}
            download_thread = threading.Thread(target=download_worker, args=(fh, CONFIG['ALL_LEVELS_PATTERN'], result_container))
            download_thread.start()

            while download_thread.is_alive():
                logging.info(f"--> Status: Download for batch {batch_name} in progress...")
                download_thread.join(60)

            if 'error' in result_container:
                raise result_container['error']
            
            downloaded_files = result_container.get('files')
            
            if downloaded_files:
                count = len(downloaded_files)
                total_downloaded_count += count
                logging.info(f"Successfully downloaded {count} files for batch {batch_name}.")
            else:
                logging.warning(f"No new files were downloaded for batch {batch_name}.")

        except Exception as e:
            # Specifically check for the "Cant open index file" error
            if "Cant open index file" in str(e):
                logging.warning(f"Could not find index files for some dates in batch {batch_name}. This is common for old data.")
                logging.warning("Skipping problematic dates and continuing.")
            else:
                logging.error(f"An unexpected error occurred during batch {batch_name}: {e}", exc_info=False)
                logging.warning(f"Skipping batch {batch_name} due to error. Will continue with next batch.")
            continue

    logging.info("="*50)
    logging.info("Download process complete.")
    logging.info(f"Total new files downloaded across all batches: {total_downloaded_count}")
    logging.info("="*50)

if __name__ == '__main__':
    main()
