# Data Documentation

This file documents the characteristics, sources, and specifications of datasets used in the NOAA wildfire prediction project.

## Data Unification and Projection Strategy

To ensure all datasets are spatially compatible for machine learning, a universal coordinate reference system (CRS) has been established for all processed data.

**Target CRS**: **UTM Zone 11N (EPSG:32611)**

All raw data, regardless of its original projection (e.g., Lambert Conformal for HRRR, Geographic EPSG:4326 for SRTM), will be reprojected to UTM Zone 11N during the processing phase. This transformation is handled within the respective data processing scripts, and all final, analysis-ready datasets will be in this target system.

This standardization is critical for several reasons:

- **Accurate Spatial Measurement**: UTM uses a metric grid (meters), allowing for consistent and accurate calculations of distance, area, and direction directly from the coordinates. This is not possible with geographic systems like EPSG:4326 where coordinate units are in degrees.
- **Minimized Distortion**: As a projected coordinate system, UTM Zone 11N is optimized for the Southern California region, ensuring that the shape and area of geographic features are represented with high fidelity.
- **Pixel Alignment for Machine Learning**: By forcing all data onto a single, common grid, we ensure that every pixel from every data layer represents the exact same location on the ground. This spatial alignment is non-negotiable for training a machine learning model to learn valid geographic patterns.
- **Analytical Integrity**: It provides a robust and mathematically sound foundation for any quantitative spatial analysis or modeling.

## SRTM Digital Elevation Data

**Source**: https://dwtkns.com/srtm30m/

**Access Requirements**: NASA Earthdata login required for file download

**Data Specifications**:
- **Resolution**: 1 arc-second (~30 meters)
- **Format**: SRTMHGT files (binary .hgt format)
- **Array Dimensions**: 3601 × 3601 pixels per tile
- **Projection**: Latitude/longitude (EPSG:4326)
- **Coverage**: 1° × 1° per tile
- **File Size**: ~25.9 MB per unzipped .hgt file
- **Distribution**: Downloaded as zipped files from NASA servers

**Current Coverage**:
- 4 tiles covering Southern California (33°-35°N, 119°-117°W)
- Files: N33W118.hgt, N33W119.hgt, N34W118.hgt, N34W119.hgt
- Combined area: 2° × 2° region including coastal and mountainous terrain

## HRRR Weather Data

**Source**: NOAA High-Resolution Rapid Refresh (HRRR) Model  
**Access**: Downloaded via Herbie library from NOAA/NCEP servers

**Data Specifications**:
- **Model**: HRRR (High-Resolution Rapid Refresh)
- **Format**: GRIB2 (GRIdded Binary, Edition 2)
- **Projection**: Lambert Conformal Conic
  - False origin: 38.5°N, 97.5°W
  - Standard parallels: 38.5°N
  - Grid mapping: EPSG Lambert Conic Conformal (2SP)
- **Resolution**: 3 km (~3000m grid spacing)
- **Grid Dimensions**: 1799 × 1059 pixels per file
- **Temporal Resolution**: Analysis data (forecast hour 0)
- **File Size**: ~5-8 MB per GRIB2 file
- **Coverage**: Continental United States (CONUS)

**Variables Included**:
- **2m Height**: Temperature (°C), Specific humidity (kg/kg), Dew point temperature (°C)
- **10m Height**: U-wind component (m/s), V-wind component (m/s), Wind speed (m/s)
- **Surface**: Precipitation rate (kg/m²/s)

**Current Dataset**:
- **Date Range**: July 30, 2014 to January 31, 2015 (6 months)
- **Total Days**: 181 days (97.3% complete, 5 days missing)
- **Total Files**: 191 GRIB2 files
- **File Organization**: Daily directories (YYYYMMDD format)
- **Geographic Subset**: Spatially subset for Southern California region
- **Coordinate System**: Lambert Conformal Conic projection (different from SRTM's EPSG:4326)

**Data Quality Notes**:
- **2014-2015 data**: Lower quality with missing columns and data gaps - kept for reference/testing only
- **2016+ data**: Primary dataset for model training and analysis due to improved data quality and completeness
- **Default download configuration**: Script configured to start from 2016-07-01 for operational use

**Processing Notes**:
- Files include GRIB index files (.idx) for efficient data access
- Multiple coordinate levels require separate processing to avoid cfgrib conflicts
- Projection differs from SRTM data, requiring coordinate transformation for integration