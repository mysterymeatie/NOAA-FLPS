#!/usr/bin/env python3
"""
Feature Engineering Pipeline for Wildfire Risk Prediction
Phase 1 Implementation - Stage 1 of Random Forest Development

This script creates advanced features from the master dataset for wildfire prediction.
Includes temporal, meteorological, spatial, and fire weather index features.

Usage:
    python scripts/feature_engineering.py --input data/processed/merged/master_dataset.nc --output data/ml_features/

Author: NOAA-FLPS Project
Date: 2025-08-11
"""

import argparse
import os
import sys
import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')
from sklearn.neighbors import KDTree

def create_temporal_features(df):
    """Create temporal features for wildfire prediction."""
    print("Creating temporal features...")
    
    # Ensure time is datetime
    df['time'] = pd.to_datetime(df['time'])
    
    # Basic temporal features
    df['day_of_year'] = df['time'].dt.dayofyear
    df['month'] = df['time'].dt.month
    df['day_of_month'] = df['time'].dt.day
    df['week_of_year'] = df['time'].dt.isocalendar().week
    
    # Seasonality features
    df['season'] = df['month'].map({12: 'winter', 1: 'winter', 2: 'winter',
                                    3: 'spring', 4: 'spring', 5: 'spring',
                                    6: 'summer', 7: 'summer', 8: 'summer',
                                    9: 'fall', 10: 'fall', 11: 'fall'})
    
    # Fire season indicator (May-October in California)
    df['fire_season'] = ((df['month'] >= 5) & (df['month'] <= 10)).astype(int)
    
    # Cyclical encoding for temporal features
    df['day_of_year_sin'] = np.sin(2 * np.pi * df['day_of_year'] / 365.25)
    df['day_of_year_cos'] = np.cos(2 * np.pi * df['day_of_year'] / 365.25)
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    
    print(f"  Added {8} temporal features")
    return df

def create_fire_weather_index(df):
    """Create Fire Weather Index (FWI) components."""
    print("Creating Fire Weather Index components...")
    
    # Temperature in Celsius (already available as temp_celsius)
    temp_c = df['temp_celsius']
    dewpoint_c = df['dewpoint_celsius']
    humidity = (df['SPFH'] * 1000)  # Convert specific humidity to g/kg approx
    wind_speed = df['WIND']
    
    # Fine Fuel Moisture Code (FFMC) - simplified version
    # Based on temperature, humidity, and wind
    humidity_effect = np.maximum(0, 100 - humidity)
    wind_effect = np.sqrt(wind_speed)
    temp_effect = np.maximum(0, temp_c)
    
    df['ffmc'] = humidity_effect + (temp_effect * 0.5) + (wind_effect * 2)
    df['ffmc'] = np.clip(df['ffmc'], 0, 100)
    
    # Drought Code (DC) - cumulative dryness index
    # Simplified: based on temperature and VPD
    temp_factor = np.maximum(0, temp_c - 5) / 40  # Normalized temp effect
    vpd_factor = df['vpd'] / 5000  # Normalized VPD effect
    df['drought_code'] = temp_factor + vpd_factor
    df['drought_code'] = np.clip(df['drought_code'], 0, 10)
    
    # Build Up Index (BUI) - organic matter dryness
    df['buildup_index'] = (df['ffmc'] / 100) * df['drought_code']
    
    # Fire Weather Index (FWI) - overall fire danger rating
    wind_factor = wind_speed / 20  # Normalized wind
    df['fire_weather_index'] = df['buildup_index'] * wind_factor * (temp_c / 40)
    df['fire_weather_index'] = np.clip(df['fire_weather_index'], 0, 50)
    
    print(f"  Added 4 fire weather index features")
    return df

def create_rolling_statistics(df, window_sizes=[3, 7, 14]):
    """Create rolling statistics for weather variables."""
    print(f"Creating rolling statistics for windows: {window_sizes}...")
    
    # Sort by location and time for proper rolling calculations
    df = df.sort_values(['y', 'x', 'time']).reset_index(drop=True)
    
    # Variables to create rolling stats for
    rolling_vars = ['temp_celsius', 'dewpoint_celsius', 'vpd', 'WIND', 'fire_weather_index']
    
    features_added = 0
    for var in rolling_vars:
        if var in df.columns:
            for window in window_sizes:
                # Rolling mean - simplified approach
                rolling_mean = (
                    df.groupby(['y', 'x'])[var]
                    .transform(lambda x: x.rolling(window=window, min_periods=1).mean())
                )
                df[f'{var}_roll_{window}d_mean'] = rolling_mean
                
                # Rolling std for temperature and wind
                if var in ['temp_celsius', 'WIND']:
                    rolling_std = (
                        df.groupby(['y', 'x'])[var]
                        .transform(lambda x: x.rolling(window=window, min_periods=1).std())
                    )
                    df[f'{var}_roll_{window}d_std'] = rolling_std
                    features_added += 2
                else:
                    features_added += 1
    
    print(f"  Added {features_added} rolling statistic features")
    return df

def create_lag_features(df, lag_days=[1, 2, 7]):
    """Create lagged features for key variables."""
    print(f"Creating lag features for days: {lag_days}...")
    
    # Ensure proper sorting and reset index
    df = df.sort_values(['y', 'x', 'time']).reset_index(drop=True)
    
    # Variables to create lags for
    lag_vars = ['temp_celsius', 'vpd', 'fire_weather_index', 'NDVI', 'fire_present']
    
    features_added = 0
    for var in lag_vars:
        if var in df.columns:
            for lag in lag_days:
                lag_values = (
                    df.groupby(['y', 'x'])[var]
                    .transform(lambda x: x.shift(lag))
                )
                df[f'{var}_lag_{lag}d'] = lag_values
                features_added += 1
    
    print(f"  Added {features_added} lag features")
    return df

def create_spatial_features(df):
    """Create spatial features including elevation gradients."""
    print("Creating spatial features...")
    
    features_added = 0

    # Elevation-derived slope/aspect per time slice (vectorized)
    out_frames = []
    for time_val, time_df in df.groupby('time', sort=False):
        elev_values = time_df.pivot(index='y', columns='x', values='elevation')
        grad_y, grad_x = np.gradient(elev_values.values)
        slope = np.sqrt(grad_y ** 2 + grad_x ** 2)
        aspect = np.degrees(np.arctan2(grad_y, grad_x))
        aspect = (aspect + 360.0) % 360.0
        slope_df = pd.DataFrame(slope, index=elev_values.index, columns=elev_values.columns)
        aspect_df = pd.DataFrame(aspect, index=elev_values.index, columns=elev_values.columns)
        long = (
            slope_df.stack().rename('elevation_slope').to_frame()
            .join(aspect_df.stack().rename('elevation_aspect'))
            .reset_index().rename(columns={'level_0': 'y', 'level_1': 'x'})
        )
        long['time'] = time_val
        out_frames.append(long)
    if out_frames:
        spatial_df = pd.concat(out_frames, ignore_index=True)
        df = df.merge(spatial_df, on=['time', 'y', 'x'], how='left')
        features_added += 2
    else:
        df['elevation_slope'] = np.nan
        df['elevation_aspect'] = np.nan
        features_added += 2

    # Distance to historical fires using KDTree (vectorized)
    fire_locations = df.loc[df['fire_present'] == 1, ['y', 'x']].drop_duplicates()
    if not fire_locations.empty:
        tree = KDTree(fire_locations[['y', 'x']].values)
        dists, _ = tree.query(df[['y', 'x']].values, k=1)
        df['distance_to_historical_fire'] = dists.astype(np.float32)
        features_added += 1
    else:
        df['distance_to_historical_fire'] = np.float32(np.inf)
        features_added += 1

    print(f"  Added {features_added} spatial features (vectorized)")
    return df

def create_vegetation_dynamics(df):
    """Create vegetation dynamics features."""
    print("Creating vegetation dynamics features...")
    
    df = df.sort_values(['y', 'x', 'time']).reset_index(drop=True)
    
    # NDVI/EVI trends and anomalies
    for var in ['NDVI', 'EVI']:
        if var in df.columns:
            # Rolling mean for anomaly calculation
            rolling_30d = (
                df.groupby(['y', 'x'])[var]
                .transform(lambda x: x.rolling(window=30, min_periods=10).mean())
            )
            df[f'{var}_30d_mean'] = rolling_30d
            
            # Anomaly (current - 30-day mean)
            df[f'{var}_anomaly'] = df[var] - df[f'{var}_30d_mean']
            
            # Rate of change (derivative)
            change_7d = (
                df.groupby(['y', 'x'])[var]
                .transform(lambda x: x.diff(periods=7))  # 7-day change
            )
            df[f'{var}_change_7d'] = change_7d
    
    print(f"  Added 6 vegetation dynamics features")
    return df

def _compute_temp_climatology(clim_years, raw_dir: str):
    """Compute per-cell, per-DOY temperature climatology from training years.

    Returns a tuple of (clim_doy_df, clim_cell_df, clim_global) for fallback handling.
    - clim_doy_df: columns [y, x, day_of_year, mu, sigma]
    - clim_cell_df: columns [y, x, mu_cell, sigma_cell]
    - clim_global: dict {mu_global, sigma_global}
    """
    print(f"Building temperature climatology from years: {clim_years} ...")
    frames = []
    for year in clim_years:
        parquet_file = os.path.join(raw_dir, f'master_table_{year}.parquet')
        if not os.path.exists(parquet_file):
            print(f"  Warning: missing {parquet_file}; skipping")
            continue
        df_y = pd.read_parquet(parquet_file, columns=['time', 'y', 'x', 'temp_celsius'])
        df_y['time'] = pd.to_datetime(df_y['time'])
        df_y['day_of_year'] = df_y['time'].dt.dayofyear.astype(np.int16)
        frames.append(df_y[['y', 'x', 'day_of_year', 'temp_celsius']])
    if not frames:
        raise RuntimeError("No training years found to build climatology")
    df_all = pd.concat(frames, axis=0, ignore_index=True)
    # Per cell+DOY
    clim_doy = (
        df_all.groupby(['y', 'x', 'day_of_year'])['temp_celsius']
        .agg(['mean', 'std'])
        .reset_index()
        .rename(columns={'mean': 'mu', 'std': 'sigma'})
    )
    # Per cell overall fallback
    clim_cell = (
        df_all.groupby(['y', 'x'])['temp_celsius']
        .agg(['mean', 'std'])
        .reset_index()
        .rename(columns={'mean': 'mu_cell', 'std': 'sigma_cell'})
    )
    # Global fallback
    mu_global = float(df_all['temp_celsius'].mean())
    sigma_global = float(df_all['temp_celsius'].std())
    print(f"  Climatology built: {len(clim_doy):,} cell-DOY rows; cells={len(clim_cell):,}")
    return clim_doy, clim_cell, { 'mu_global': mu_global, 'sigma_global': sigma_global }


def _add_temp_anomaly_features(df, clim_doy, clim_cell, clim_global):
    """Merge climatology into df and compute temp anomaly features with fallbacks."""
    if 'time' not in df.columns:
        raise ValueError('Expected time column for DOY computation')
    df['time'] = pd.to_datetime(df['time'])
    df['day_of_year'] = df['time'].dt.dayofyear.astype(np.int16)
    # Merge DOY-level
    df = df.merge(clim_doy, how='left', on=['y', 'x', 'day_of_year'])
    # Fallback: merge cell-level for missing mu/sigma
    missing_mask = df['mu'].isna() | df['sigma'].isna()
    if missing_mask.any():
        df = df.merge(clim_cell, how='left', on=['y', 'x'])
        df.loc[df['mu'].isna(), 'mu'] = df.loc[df['mu'].isna(), 'mu_cell']
        df.loc[df['sigma'].isna(), 'sigma'] = df.loc[df['sigma'].isna(), 'sigma_cell']
        df.drop(columns=['mu_cell', 'sigma_cell'], inplace=True)
    # Global fallback
    df['mu'].fillna(clim_global['mu_global'], inplace=True)
    df['sigma'].fillna(max(clim_global['sigma_global'], 1e-6), inplace=True)
    # Compute features
    df['temp_anom'] = (df['temp_celsius'] - df['mu']).astype(np.float32)
    # Avoid divide-by-zero
    sigma_safe = df['sigma'].where(df['sigma'] > 1e-6, 1e-6)
    df['temp_z'] = ((df['temp_celsius'] - df['mu']) / sigma_safe).astype(np.float32)
    return df


def process_year_data(year, input_file, output_dir, clim_doy=None, clim_cell=None, clim_global=None, enable_spatial=False, raw_dir: str | None = None):
    """Process data for a single year with feature engineering."""
    print(f"\n=== Processing Year {year} ===")
    
    # Load the year's parquet data
    parquet_file = os.path.join(raw_dir or 'data/ml_datasets1', f'master_table_{year}.parquet')
    
    if not os.path.exists(parquet_file):
        print(f"Warning: Parquet file for {year} not found, skipping...")
        return False
    
    print(f"Loading data from {parquet_file}...")
    df = pd.read_parquet(parquet_file)
    print(f"Loaded {len(df):,} samples")
    
    # Original feature count
    original_features = len(df.columns)
    
    # Apply feature engineering
    df = create_temporal_features(df)
    # Temperature anomaly features (requires climatology)
    if clim_doy is not None and clim_cell is not None and clim_global is not None:
        print("Adding temperature anomaly features (mu_DOY, z-score)...")
        df = _add_temp_anomaly_features(df, clim_doy, clim_cell, clim_global)
    df = create_fire_weather_index(df)
    df = create_rolling_statistics(df)
    df = create_lag_features(df)
    if enable_spatial:
        df = create_spatial_features(df)
    else:
        print("Skipping spatial features (use --enable-spatial to compute)")
    df = create_vegetation_dynamics(df)
    
    # Final feature count
    final_features = len(df.columns)
    print(f"Features: {original_features} → {final_features} (+{final_features - original_features})")
    
    # Save enhanced dataset
    output_file = os.path.join(output_dir, f'features_{year}.parquet')
    df.to_parquet(output_file, compression='snappy')
    file_size = os.path.getsize(output_file) / 1024 / 1024
    print(f"Saved enhanced dataset: {output_file} ({file_size:.1f} MB)")
    
    return True

def main():
    parser = argparse.ArgumentParser(description='Feature Engineering for Wildfire Prediction')
    parser.add_argument('--input', required=True, help='Input master dataset NetCDF file')
    parser.add_argument('--output', required=True, help='Output directory for enhanced features')
    parser.add_argument('--years', nargs='+', type=int, default=list(range(2016, 2026)),
                       help='Years to process (default: 2016-2025)')
    parser.add_argument('--enable-spatial', action='store_true', help='Compute spatial features (slower)')
    parser.add_argument('--clim-years', nargs='+', type=int, default=[2016, 2017, 2018, 2019, 2020, 2021, 2022],
                       help='Years to build temperature climatology (default: 2016-2022)')
    parser.add_argument('--clim-save', type=str, default=str(Path('data/processed/ml_artifacts') / 'temp_climatology.parquet'),
                       help='Path to save computed climatology parquet')
    parser.add_argument('--clim-load', type=str, default='', help='Path to load existing climatology parquet')
    parser.add_argument('--raw-dir', type=str, default=str(Path('data') / 'ml_datasets1'), help='Directory of raw per-year Parquet tables')
    
    args = parser.parse_args()
    
    print("=== Wildfire Prediction Feature Engineering Pipeline ===")
    print(f"Input: {args.input}")
    print(f"Output: {args.output}")
    print(f"Years: {args.years}")
    
    # Create output directory
    os.makedirs(args.output, exist_ok=True)
    
    # Verify input file exists
    if not os.path.exists(args.input):
        print(f"Error: Input file not found: {args.input}")
        return 1
    
    # Build or load temperature climatology
    os.makedirs(Path(args.clim_save).parent, exist_ok=True)
    clim_doy = clim_cell = clim_global = None
    if args.clim_load and os.path.exists(args.clim_load):
        print(f"Loading existing climatology from {args.clim_load} ...")
        clim_df = pd.read_parquet(args.clim_load)
        clim_doy = clim_df[['y', 'x', 'day_of_year', 'mu', 'sigma']].copy()
        clim_cell = clim_df.drop(columns=['day_of_year', 'mu', 'sigma']).drop_duplicates().rename(columns={'mu_fallback': 'mu_cell', 'sigma_fallback': 'sigma_cell'})
        # Global stored?
        if 'mu_global' in clim_df.columns and 'sigma_global' in clim_df.columns:
            clim_global = { 'mu_global': float(clim_df['mu_global'].iloc[0]), 'sigma_global': float(clim_df['sigma_global'].iloc[0]) }
        else:
            # derive from mu distribution as fallback
            clim_global = { 'mu_global': float(clim_doy['mu'].mean()), 'sigma_global': float(clim_doy['sigma'].mean()) }
    else:
        clim_doy, clim_cell, clim_global = _compute_temp_climatology(args.clim_years, raw_dir=args.raw_dir)
        # Persist a compact parquet with DOY stats and cell/global fallbacks replicated
        save_df = clim_doy.copy()
        # Attach cell-level fallbacks for convenience
        save_df = save_df.merge(clim_cell, on=['y', 'x'], how='left')
        save_df['mu_global'] = clim_global['mu_global']
        save_df['sigma_global'] = clim_global['sigma_global']
        save_path = args.clim_save
        try:
            save_dir = Path(save_path).parent
            os.makedirs(save_dir, exist_ok=True)
            save_df.to_parquet(save_path, index=False)
            print(f"Saved climatology to {save_path}")
        except Exception as e:
            print(f"Warning: failed to save climatology to {save_path}: {e}")

    # Process each year
    successful_years = []
    failed_years = []
    
    for year in args.years:
        success = process_year_data(
            year, args.input, args.output,
            clim_doy=clim_doy, clim_cell=clim_cell, clim_global=clim_global,
            enable_spatial=args.enable_spatial,
            raw_dir=args.raw_dir,
        )
        if success:
            successful_years.append(year)
        else:
            failed_years.append(year)
    
    # Summary
    print(f"\n=== Feature Engineering Complete ===")
    print(f"Successfully processed: {successful_years}")
    if failed_years:
        print(f"Failed: {failed_years}")
    
    # Create summary file
    summary = {
        'timestamp': datetime.now().isoformat(),
        'successful_years': successful_years,
        'failed_years': failed_years,
        'total_features': 'Variable per year',
        'output_directory': args.output
    }
    
    summary_file = os.path.join(args.output, 'feature_engineering_summary.txt')
    with open(summary_file, 'w') as f:
        for key, value in summary.items():
            f.write(f"{key}: {value}\n")
    
    print(f"Summary saved to: {summary_file}")
    
    return 0 if not failed_years else 1

if __name__ == '__main__':
    sys.exit(main())