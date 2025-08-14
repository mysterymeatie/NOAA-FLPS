#!/usr/bin/env python3
"""
Minimal Feature Engineering for Wildfire Risk Prediction
Phase 1 Implementation - Essential Features Only

Creates core features needed for baseline Random Forest model.
"""

import argparse
import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime

def create_minimal_features(df):
    """Create minimal essential features for wildfire prediction."""
    print("Creating minimal feature set...")
    
    # Ensure time is datetime
    df['time'] = pd.to_datetime(df['time'])
    
    # Basic temporal features
    df['day_of_year'] = df['time'].dt.dayofyear
    df['month'] = df['time'].dt.month
    df['fire_season'] = ((df['month'] >= 5) & (df['month'] <= 10)).astype(int)
    
    # Cyclical encoding for seasonality
    df['day_of_year_sin'] = np.sin(2 * np.pi * df['day_of_year'] / 365.25)
    df['day_of_year_cos'] = np.cos(2 * np.pi * df['day_of_year'] / 365.25)
    
    # Simplified Fire Weather Index
    temp_c = df['temp_celsius']
    wind_speed = df['WIND']
    vpd = df['vpd']
    
    # Simple fire danger index
    df['fire_danger_index'] = (temp_c / 40) * (wind_speed / 20) * (vpd / 5000)
    df['fire_danger_index'] = np.clip(df['fire_danger_index'], 0, 1)
    
    # Temperature and humidity categories
    df['temp_high'] = (temp_c > temp_c.quantile(0.75)).astype(int)
    df['vpd_high'] = (vpd > vpd.quantile(0.75)).astype(int)
    df['wind_high'] = (wind_speed > wind_speed.quantile(0.75)).astype(int)
    
    # Elevation categories
    if 'elevation' in df.columns:
        df['elevation_low'] = (df['elevation'] < 500).astype(int)
        df['elevation_med'] = ((df['elevation'] >= 500) & (df['elevation'] < 1500)).astype(int)
        df['elevation_high'] = (df['elevation'] >= 1500).astype(int)
    
    # Vegetation health indicators
    if 'NDVI' in df.columns:
        df['vegetation_dry'] = (df['NDVI'] < df['NDVI'].quantile(0.25)).astype(int)
        df['vegetation_healthy'] = (df['NDVI'] > df['NDVI'].quantile(0.75)).astype(int)
    
    print(f"  Added {len([c for c in df.columns if c not in ['time', 'y', 'x', 'latitude', 'longitude']])} total features")
    return df

def process_sample_data(year, sample_size=50000):
    """Process a sample of data for testing."""
    print(f"Processing sample of {sample_size} records from {year}...")
    
    parquet_file = os.path.join('data/ml_datasets', f'master_table_{year}.parquet')
    
    if not os.path.exists(parquet_file):
        print(f"File not found: {parquet_file}")
        return None
    
    # Load sample of data
    df = pd.read_parquet(parquet_file)
    print(f"Loaded full dataset: {len(df):,} samples")
    
    # Take random sample
    if len(df) > sample_size:
        df = df.sample(n=sample_size, random_state=42)
        print(f"Sampled to: {len(df):,} samples")
    
    original_features = len(df.columns)
    
    # Apply minimal feature engineering
    df = create_minimal_features(df)
    
    final_features = len(df.columns)
    print(f"Features: {original_features} -> {final_features}")
    
    # Check fire distribution
    fire_count = df['fire_present'].sum()
    print(f"Fire samples: {fire_count} ({fire_count/len(df)*100:.3f}%)")
    
    return df

def main():
    parser = argparse.ArgumentParser(description='Minimal Feature Engineering Test')
    parser.add_argument('--year', type=int, default=2016, help='Year to test')
    parser.add_argument('--sample-size', type=int, default=50000, help='Sample size')
    parser.add_argument('--output', default='data/ml_features', help='Output directory')
    
    args = parser.parse_args()
    
    print(f"=== Minimal Feature Engineering Test ===")
    print(f"Year: {args.year}, Sample size: {args.sample_size}")
    
    os.makedirs(args.output, exist_ok=True)
    
    # Process sample data
    df = process_sample_data(args.year, args.sample_size)
    
    if df is not None:
        # Save sample
        output_file = os.path.join(args.output, f'features_sample_{args.year}.parquet')
        df.to_parquet(output_file, compression='snappy')
        
        file_size = os.path.getsize(output_file) / 1024 / 1024
        print(f"Saved: {output_file} ({file_size:.1f} MB)")
        
        # Show feature summary
        print(f"\nFeature Summary:")
        feature_cols = [c for c in df.columns if c not in ['time', 'y', 'x', 'latitude', 'longitude', 'fire_present']]
        print(f"Total features for ML: {len(feature_cols)}")
        
        # Show target distribution
        target_dist = df['fire_present'].value_counts()
        print(f"\nTarget distribution:")
        print(target_dist)
        print(f"Imbalance ratio: 1:{target_dist[0]/target_dist[1]:.0f}")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())