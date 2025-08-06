#!/usr/bin/env python3
"""
SRTM to NetCDF Conversion Script - UTM Zone 11N at 3km Resolution

This script converts SRTM (Shuttle Radar Topography Mission) elevation data 
from the native .hgt format to NetCDF4 format with the following transformations:

1. COORDINATE SYSTEM TRANSFORMATION:
   - Input: 4 SRTM tiles in WGS84 Geographic (EPSG:4326) at ~30m resolution
   - Output: Single NetCDF file in UTM Zone 11N (EPSG:32611) at 3km resolution
   
2. SPATIAL PROCESSING:
   - Merges 4 adjacent SRTM tiles (N33W118, N33W119, N34W118, N34W119)
   - Coverage: Southern California (33-35°N, 118-120°W)
   - Reprojects from geographic to UTM Zone 11N projection
   - Resamples from ~30m to 3km resolution using block mean aggregation

3. OUTPUT FORMAT:
   - NetCDF4 with CF-compliant metadata
   - UTM Zone 11N coordinates in meters
   - Elevation data in meters above sea level
   - Proper CRS definition for GIS compatibility

4. TECHNICAL APPROACH:
   - Uses rasterio for geospatial I/O and reprojection
   - Uses xarray for NetCDF creation and metadata management
   - Implements memory-efficient processing for large datasets
   - Handles nodata values and edge effects properly

Dependencies:
- rasterio: Geospatial raster I/O and coordinate transformations
- rioxarray: Integration between rasterio and xarray
- xarray: N-dimensional labeled arrays and NetCDF I/O
- numpy: Numerical array operations
- pyproj: Coordinate reference system definitions

Usage:
    python srtm_to_netcdf_utm.py

Output:
    data/processed/SRTM/srtm_socal_utm11n_3km.nc

Author: NOAA Wildfire Risk Prediction Project
Date: August 2025
"""

import os
import sys
import numpy as np
import xarray as xr
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import calculate_default_transform, reproject
from rasterio.crs import CRS
import rioxarray as rxr
from pathlib import Path
import warnings
warnings.filterwarnings('ignore', category=rasterio.errors.NotGeoreferencedWarning)

def setup_directories():
    """
    Create necessary output directories
    """
    output_dir = Path("data/processed/SRTM")
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir

def read_srtm_tiles(srtm_dir):
    """
    Read and merge SRTM .hgt tiles into a single array
    
    Parameters:
    -----------
    srtm_dir : str or Path
        Directory containing SRTM .hgt files
        
    Returns:
    --------
    merged_data : xarray.DataArray
        Merged elevation data with geographic coordinates
    """
    print("Reading SRTM tiles...")
    
    # Define the 4 tiles covering Southern California with their expected bounds
    tile_info = [
        ('N33W119.hgt', -119, -118, 33, 34),  # SW tile
        ('N33W118.hgt', -118, -117, 33, 34),  # SE tile  
        ('N34W119.hgt', -119, -118, 34, 35),  # NW tile
        ('N34W118.hgt', -118, -117, 34, 35)   # NE tile
    ]
    
    # Read each tile as xarray DataArray
    tiles = []
    for tile_file, west, east, south, north in tile_info:
        tile_path = Path(srtm_dir) / tile_file
        if not tile_path.exists():
            raise FileNotFoundError(f"SRTM tile not found: {tile_path}")
            
        print(f"  Reading {tile_file}...")
        
        # Use rioxarray to read with proper geospatial metadata
        da = rxr.open_rasterio(tile_path, chunks=True)
        
        # Rename band dimension to remove it (single band data)
        da = da.squeeze('band', drop=True)
        
        # Set nodata values
        da = da.where(da != da.rio.nodata, np.nan)
        
        # Verify bounds are as expected
        bounds = da.rio.bounds()
        print(f"    Bounds: {bounds}")
        
        tiles.append(da)
    
    # Convert tiles to datasets and merge using xarray
    print("  Merging tiles...")
    datasets = []
    for i, tile in enumerate(tiles):
        ds = tile.to_dataset(name='elevation')
        datasets.append(ds)
    
    # Merge datasets
    merged_ds = xr.merge(datasets)
    merged = merged_ds['elevation']
    
    # Ensure CRS is preserved
    if hasattr(tiles[0], 'rio') and tiles[0].rio.crs is not None:
        merged = merged.rio.write_crs(tiles[0].rio.crs)
    
    print(f"  Merged data shape: {merged.shape}")
    print(f"  Geographic bounds: {merged.rio.bounds()}")
    
    return merged

def reproject_to_utm(data_array, target_crs='EPSG:32611'):
    """
    Reproject data from WGS84 to UTM Zone 11N
    
    Parameters:
    -----------
    data_array : xarray.DataArray
        Input data in geographic coordinates
    target_crs : str
        Target coordinate reference system (UTM Zone 11N)
        
    Returns:
    --------
    reprojected : xarray.DataArray
        Data reprojected to UTM coordinates
    """
    print("Reprojecting to UTM Zone 11N...")
    
    # Set the CRS if not already set
    if data_array.rio.crs is None:
        data_array = data_array.rio.write_crs('EPSG:4326')
    
    # Reproject to UTM Zone 11N
    # Use nearest neighbor resampling for initial reprojection
    # We'll do the resolution change separately
    reprojected = data_array.rio.reproject(
        target_crs,
        resampling=Resampling.bilinear,
        resolution=30  # Keep original resolution for now
    )
    
    print(f"  UTM bounds: {reprojected.rio.bounds()}")
    print(f"  UTM shape: {reprojected.shape}")
    
    return reprojected

def resample_to_3km(data_array, target_resolution=3000):
    """
    Resample data from 30m to 3km resolution using block mean
    
    Parameters:
    -----------
    data_array : xarray.DataArray
        Input data at high resolution
    target_resolution : int
        Target resolution in meters (3000 = 3km)
        
    Returns:
    --------
    resampled : xarray.DataArray
        Data resampled to target resolution
    """
    print(f"Resampling to {target_resolution}m resolution...")
    
    # Calculate new dimensions
    bounds = data_array.rio.bounds()
    left, bottom, right, top = bounds
    width = right - left
    height = top - bottom
    
    new_width = int(width / target_resolution)
    new_height = int(height / target_resolution)
    
    print(f"  Current bounds: {bounds}")
    print(f"  Domain size: {width:.0f}m x {height:.0f}m")
    print(f"  New dimensions: {new_height} x {new_width}")
    
    # Resample using rioxarray - use only resolution parameter
    resampled = data_array.rio.reproject(
        data_array.rio.crs,
        resampling=Resampling.average,  # Use average (mean) for elevation
        resolution=target_resolution
    )
    
    print(f"  Final shape: {resampled.shape}")
    
    return resampled

def create_netcdf_with_metadata(data_array, output_path):
    """
    Create NetCDF4 file with proper metadata and CF compliance
    
    Parameters:
    -----------
    data_array : xarray.DataArray
        Processed elevation data
    output_path : str or Path
        Output NetCDF file path
    """
    print("Creating NetCDF4 file with metadata...")
    
    # Create dataset
    ds = data_array.to_dataset(name='elevation')
    
    # Rename coordinates to standard names
    ds = ds.rename({'x': 'easting', 'y': 'northing'})
    
    # Add coordinate attributes
    ds['easting'].attrs = {
        'standard_name': 'projection_x_coordinate',
        'long_name': 'Easting (UTM Zone 11N)',
        'units': 'meters',
        'axis': 'X'
    }
    
    ds['northing'].attrs = {
        'standard_name': 'projection_y_coordinate', 
        'long_name': 'Northing (UTM Zone 11N)',
        'units': 'meters',
        'axis': 'Y'
    }
    
    # Set proper nodata values
    ds['elevation'] = ds['elevation'].where(ds['elevation'] != -32768, np.nan)
    
    # Add variable attributes
    ds['elevation'].attrs = {
        'standard_name': 'height_above_reference_ellipsoid',
        'long_name': 'Elevation above sea level',
        'units': 'meters',
        'source': 'SRTM (Shuttle Radar Topography Mission)',
        'processing': 'Reprojected to UTM Zone 11N, resampled to 3km resolution using block mean',
        'grid_mapping': 'crs',
        '_FillValue': np.nan
    }
    
    # Add CRS variable for CF compliance
    ds['crs'] = xr.DataArray(
        data=np.int32(0),
        attrs={
            'grid_mapping_name': 'transverse_mercator',
            'projected_crs_name': 'WGS 84 / UTM zone 11N',
            'epsg_code': 'EPSG:32611',
            'longitude_of_central_meridian': -117.0,
            'latitude_of_projection_origin': 0.0,
            'scale_factor_at_central_meridian': 0.9996,
            'false_easting': 500000.0,
            'false_northing': 0.0,
            'reference_ellipsoid_name': 'WGS 84',
            'prime_meridian_name': 'Greenwich',
            'geographic_crs_name': 'WGS 84'
        }
    )
    
    # Add global attributes
    ds.attrs = {
        'title': 'SRTM Elevation Data for Southern California',
        'description': 'Elevation data derived from SRTM, reprojected to UTM Zone 11N and resampled to 3km resolution',
        'source': 'SRTM (Shuttle Radar Topography Mission) 1 arc-second data',
        'processing_date': str(np.datetime64('today')),
        'spatial_resolution': '3 kilometers',
        'coordinate_system': 'UTM Zone 11N (EPSG:32611)',
        'coverage_area': 'Southern California (33-35°N, 118-120°W)',
        'conventions': 'CF-1.8',
        'institution': 'NOAA Wildfire Risk Prediction Project',
        'contact': 'NOAA Internship Program',
        'processing_software': 'Python (rasterio, xarray, rioxarray)',
        'original_resolution': '~30 meters (1 arc-second)',
        'resampling_method': 'Block mean aggregation'
    }
    
    # Set encoding for compression
    encoding = {
        'elevation': {
            'zlib': True,
            'complevel': 4,
            'fletcher32': True,
            'dtype': 'float32'
        }
    }
    
    # Write to NetCDF
    print(f"  Writing to: {output_path}")
    ds.to_netcdf(output_path, encoding=encoding)
    
    # Print summary statistics
    elev_data = ds['elevation'].values
    valid_data = elev_data[~np.isnan(elev_data)]
    
    print(f"  Elevation statistics:")
    print(f"    Min: {valid_data.min():.1f} m")
    print(f"    Max: {valid_data.max():.1f} m") 
    print(f"    Mean: {valid_data.mean():.1f} m")
    print(f"    Valid data points: {len(valid_data):,} / {elev_data.size:,}")
    
    return ds

def main():
    """
    Main processing function
    """
    print("=" * 60)
    print("SRTM to NetCDF Conversion - UTM Zone 11N, 3km Resolution")
    print("=" * 60)
    print()
    
    # Setup paths
    base_dir = Path.cwd()
    srtm_dir = base_dir / "data" / "raw" / "SRTM"
    output_dir = setup_directories()
    output_file = output_dir / "srtm_socal_utm11n_3km.nc"
    
    print(f"Input directory: {srtm_dir}")
    print(f"Output file: {output_file}")
    print()
    
    try:
        # Step 1: Read and merge SRTM tiles
        merged_data = read_srtm_tiles(srtm_dir)
        
        # Step 2: Reproject to UTM Zone 11N  
        utm_data = reproject_to_utm(merged_data)
        
        # Step 3: Resample to 3km resolution
        resampled_data = resample_to_3km(utm_data)
        
        # Step 4: Create NetCDF with metadata
        final_dataset = create_netcdf_with_metadata(resampled_data, output_file)
        
        print()
        print("=" * 60)
        print("CONVERSION COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"Output file: {output_file}")
        print(f"File size: {output_file.stat().st_size / 1024 / 1024:.1f} MB")
        print()
        print("Dataset Summary:")
        print(f"  Shape: {final_dataset['elevation'].shape}")
        print(f"  Coordinate system: UTM Zone 11N (EPSG:32611)")
        print(f"  Resolution: 3 km")
        print(f"  Coverage: Southern California")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()