# Data Documentation

This file documents the characteristics, sources, and specifications of datasets used in the NOAA wildfire prediction project.

## Data Unification and Processing Strategy

To ensure all datasets are spatially and temporally compatible for machine learning, a standardized two-stage processing pipeline has been established that transforms heterogeneous raw data sources into analysis-ready formats.

### **Stage 1: Unified NetCDF Creation**

**Target CRS**: **UTM Zone 11N (EPSG:32611)**  
**Target Resolution**: **3 kilometers** (matching HRRR native resolution)  
**Output Format**: **NetCDF4 with CF conventions**

All raw data sources are converted to standardized NetCDF files with:
- **Unified coordinate system**: UTM Zone 11N projected coordinates
- **Consistent spatial grid**: 3km resolution aligned to the HRRR grid
- **Standardized dimensions**: `(time, y, x)` for time series data, `(y, x)` for static data
- **Embedded metadata**: CF-compliant attributes for coordinate systems, units, and data provenance
- **Compression**: LZ4/Zlib compression for efficient storage

**Output Structure**:
```
data/unified/
├── weather_hrrr.nc      # Daily weather variables (reprojected from native Lambert Conformal)
├── vegetation_modis.nc  # 16-day vegetation indices (aggregated from 250m to 3km)
├── elevation_srtm.nc    # Static terrain features (aggregated from 30m to 3km)
└── fires_calfire.nc     # Daily fire occurrence masks (rasterized to 3km grid)
```

### **Stage 2: Machine Learning Dataset Creation**

**Target Format**: **Apache Parquet**  
**Structure**: **Tabular format optimized for ML workflows**

NetCDF files are converted to analysis-ready Parquet datasets with:
- **Temporal alignment**: All data sources synchronized to daily timestamps
- **Spatial flattening**: 2D grids converted to rows (one row per grid cell per day)
- **Feature engineering**: Derived variables, temporal lags, spatial neighborhoods
- **Efficient storage**: Column-oriented format with optimal compression
- **ML optimization**: Direct compatibility with pandas, scikit-learn, XGBoost

**Output Structure**:
```
data/ml_ready/
├── training_dataset.parquet     # Features + targets for model training
├── validation_dataset.parquet   # Temporal holdout for model validation
├── test_dataset.parquet         # Final evaluation dataset
└── prediction_dataset.parquet   # Most recent data for operational forecasting
```

### **Coordinate System Rationale**

**UTM Zone 11N (EPSG:32611)** is optimal for this project because:

- **Accurate Spatial Measurement**: UTM uses a metric grid (meters), enabling precise distance, area, and direction calculations essential for wildfire modeling
- **Minimized Distortion**: Optimized for Southern California (118°W-114°W), ensuring accurate representation of terrain and fire behavior patterns  
- **Pixel Alignment**: Forces all data onto identical grid cells, guaranteeing spatial correspondence between weather, vegetation, terrain, and fire data
- **ML Compatibility**: Metric coordinates simplify spatial feature engineering (buffers, neighborhoods, gradients)
- **Analytical Integrity**: Mathematically consistent foundation for quantitative spatial analysis

### **Resolution and Aggregation Strategy**

**3km target resolution** was chosen as the unifying grid for all datasets. This decision is based on using the coarsest resolution dataset (HRRR weather data) as the foundational grid.

- **HRRR as Baseline**: The 3km HRRR grid serves as the target for all other data sources. This avoids creating artificial, interpolated weather data at resolutions finer than it was originally measured.
- **Downsampling of High-Resolution Data**: Finer-grained datasets (SRTM at 30m, MODIS at 250m) are downsampled to the 3km grid. This is the most computationally sound and efficient approach.
- **Hybrid Multi-Scale Feature Engineering**: To avoid losing critical information from high-resolution sources during downsampling, we will generate a suite of statistical features that describe the sub-grid variability within each 3km cell. Instead of just calculating the `mean`, we will also compute the `median`, `standard deviation`, `minimum`, and `maximum` for variables like elevation and vegetation indices. This allows the model to learn about landscape heterogeneity (e.g., distinguishing flat vs. mountainous cells or uniform vs. patchy vegetation) which is a critical driver of fire behavior.

This strategy provides a computationally efficient and robust framework while retaining the essential multi-scale characteristics of the landscape.

### **Processing Pipeline Benefits**

1. **Separation of Concerns**: Coordinate transformation isolated from ML preprocessing
2. **Quality Control**: NetCDF stage enables visual verification and data validation
3. **Flexibility**: Multiple ML datasets can be created from unified NetCDF sources  
4. **Performance**: Parquet format optimized for fast ML training and inference
5. **Reproducibility**: Standardized intermediate formats ensure consistent results
6. **Scalability**: Pipeline handles large datasets through chunking and compression

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

**Processing Pipeline Status**:
- **Raw SRTM**: Available as individual 1°×1° tiles (current status)
- **Unified NetCDF**: Target output `data/unified/elevation_srtm.nc` (UTM Zone 11N, 3km resolution)
- **ML-Ready Parquet**: Target output `data/ml_ready/*.parquet` (static terrain features)

**Processing Notes**:
- Tiles require mosaicking and reprojection from EPSG:4326 to UTM Zone 11N.
- 30m native resolution downsampled to the 3km target grid.
- Aggregation will include calculating the `mean`, `median`, `standard deviation`, `min`, and `max` elevation within each 3km cell to capture terrain variability.
- Derived variables (slope, aspect, terrain roughness) will be calculated from the aggregated 3km data.

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
- **Temporal Resolution**: Analysis data (forecast hour 0, daily at 21:00 UTC / 1:00 PM PST)
- **File Size**: ~7.4 MB per GRIB2 file (spatially subset)
- **Coverage**: Southern California region (spatially subset from CONUS)

**Variables Included**:
- **2m Height**: Temperature (t2m), Specific humidity (sh2), Dew point temperature (d2m), Relative humidity (r2)
- **10m Height**: U-wind component (u10), V-wind component (v10), Wind speed maximum (max_10si)
- **Surface**: Precipitation rate (prate)

**Raw Data Structure**:
- **Location**: `data/raw/NOAA_HRRR/hrrr/`
- **Directory Structure**: `YYYYMMDD/` (daily directories)
- **File Naming**: `subset_[hash]__hrrr.t21z.wrfsfcf00.grib2`
- **Index Files**: Two .idx files per GRIB2 file for efficient data access
- **Total Files**: 3,263 daily files (some gaps exist in the record)

**Current Dataset Coverage**:
- **Date Range**: July 1, 2016 to July 28, 2025 (9+ years)
- **Temporal Gaps**: Some missing days throughout the record
- **Data Completeness**: ~89% coverage (3,263 files out of ~3,650 possible days)
- **Geographic Subset**: Pre-cropped to Southern California bounding box during download

**Data Quality Notes**:
- **2014-2015 data**: Excluded from raw dataset due to missing variables (r2 relative humidity)
- **2016+ data**: Primary dataset with complete variable coverage
- **Spatial Subsetting**: Files already cropped to reduce size and focus on study region
- **Daily Timing**: Consistent 21:00 UTC (1:00 PM PST) snapshots for fire risk modeling

**Processing Pipeline Status**:
- **Raw GRIB2**: Available in daily directories (current status)
- **Legacy NetCDF**: Available as yearly aggregates in `data/processed/HRRR/` (Lambert Conformal projection)
- **Unified NetCDF**: Target output `data/unified/weather_hrrr.nc` (UTM Zone 11N, 3km resolution)
- **ML-Ready Parquet**: Target output `data/ml_ready/*.parquet` (tabular format for training)

**Processing Notes**:
- The native 3km HRRR grid will be used as the base grid for the project.
- Files include GRIB index files (.idx) for efficient variable extraction.
- Multiple coordinate levels require separate cfgrib processing to avoid conflicts.
- Projection transformation from Lambert Conformal to UTM Zone 11N is required.
- Herbie library handles download with spatial subsetting parameters.

## MODIS Vegetation Indices

**Source**: NASA MODIS Terra/Aqua satellites  
**Product**: MOD13Q1 - MODIS/Terra Vegetation Indices 16-Day L3 Global 250m SIN Grid V061  
**Access**: Downloaded via NASA Earthdata portal with authentication

**Data Specifications**:
- **Satellite**: MODIS Terra (MOD13Q1)
- **Format**: HDF4-EOS (Hierarchical Data Format v4)
- **Projection**: Sinusoidal
  - MODIS Sinusoidal Grid projection
  - Equal-area projection optimized for global vegetation monitoring
- **Resolution**: 250 meters per pixel
- **Temporal Resolution**: 16-day composite periods
- **File Size**: ~185 MB per HDF file (uncompressed)
- **Tile Coverage**: h08v05 (Southern California region)

**Variables Included**:
- **NDVI (250m_16_days_NDVI)**: Normalized Difference Vegetation Index (-1 to +1, scaled)
- **EVI (250m_16_days_EVI)**: Enhanced Vegetation Index (-1 to +1, scaled)  
- **VI Quality (250m_16_days_VI_Quality)**: Per-pixel quality assessment flags
- **Red Reflectance (250m_16_days_red_reflectance)**: Surface reflectance band 1
- **NIR Reflectance (250m_16_days_NIR_reflectance)**: Surface reflectance band 2
- **Blue Reflectance (250m_16_days_blue_reflectance)**: Surface reflectance band 3
- **MIR Reflectance (250m_16_days_MIR_reflectance)**: Surface reflectance band 7
- **ViewZenith/SolarZenith Angles**: Geometric metadata
- **Composite Day of Year**: Day of year for each pixel's best observation

**Raw Data Structure**:
- **Location**: `data/raw/MODIS_NDVI_250m/`
- **File Naming**: `MOD13Q1.A[YYYY][DDD].h08v05.061.[PROCESSING_TIMESTAMP].hdf`
  - `A[YYYY][DDD]`: Acquisition date (Year + Day of Year)
  - `h08v05`: MODIS tile identifier (Southern California)
  - `061`: Collection version
  - `[PROCESSING_TIMESTAMP]`: NASA processing date/time
- **Metadata Files**: `.cmr.xml` files containing NASA Earth data metadata
- **Total Files**: 587 HDF files + corresponding XML metadata

**Current Dataset Coverage**:
- **Date Range**: February 18, 2000 to June 26, 2025 (25+ years)
- **Temporal Resolution**: 16-day composites (23 files per year)
- **Data Completeness**: 101.7% coverage (587 files vs. 577 expected)
- **Acquisition Timing**: Mid-month composites optimized for phenology monitoring

**Data Quality Features**:
- **Atmospheric Correction**: Pre-corrected for atmospheric effects
- **Cloud Screening**: Automated cloud and shadow masking
- **Composite Algorithm**: Best pixel selection over 16-day periods based on:
  - Highest NDVI values
  - Lowest cloud contamination
  - Optimal viewing geometry
- **Quality Flags**: Per-pixel reliability indicators for filtering

**Spatial Characteristics**:
- **Tile System**: MODIS Sinusoidal Grid tile h08v05
- **Tile Bounds**: Approximately 31.5°N to 40.0°N, 126.0°W to 117.0°W
- **Array Dimensions**: 4800 × 4800 pixels per tile (250m resolution)
- **Geographic Coverage**: Covers Southern California, parts of Nevada, Arizona, Baja California
- **Overlap with Study Area**: Complete coverage of Santiago Canyon region

**Processing Pipeline Status**:
- **Raw HDF4**: Available with full variable suite (current status)
- **Unified NetCDF**: Target output `data/unified/vegetation_modis.nc` (UTM Zone 11N, 3km resolution)
- **ML-Ready Parquet**: Target output `data/ml_ready/*.parquet` (daily interpolated values for training)

**Processing Notes**:
- HDF4 format requires specialized libraries (pyhdf, gdal, rasterio).
- Sinusoidal projection needs transformation to UTM Zone 11N for integration.
- 250m data will be aggregated to the 3km grid. This will include calculating `mean`, `median`, `std`, `min`, and `max` for indices like NDVI and EVI to represent sub-grid vegetation patchiness.
- Quality flags are essential for filtering unreliable pixels before aggregation.
- 16-day temporal resolution requires interpolation for daily ML features.
- Scale factors and fill values must be applied during data extraction.
- Large file sizes require efficient chunking strategies for processing.

## CALFIRE Historical Fire Data

**Source**: California Department of Forestry and Fire Protection (CAL FIRE)  
**Product**: Fire Perimeters Database - Post 1980  
**Access**: Public data available via CAL FIRE GIS portal

**Data Specifications**:
- **Format**: Shapefile (.shp) with polygon geometries
- **Projection**: Geographic (EPSG:4326)
- **Temporal Coverage**: 1980 to present
- **Spatial Coverage**: Statewide California fire perimeters
- **Update Frequency**: Ongoing (fires added as they are contained)

**Variables Included**:
- **Fire Perimeter**: Polygon boundary of burned area
- **Fire Name**: Official incident name
- **Alarm Date**: Fire start date and time
- **Containment Date**: Date fire was contained
- **Cause**: Fire cause classification (human, lightning, unknown, etc.)
- **Acres Burned**: Total area within fire perimeter
- **Agency**: Responsible fire agency (CAL FIRE, USFS, etc.)
- **Fire Number**: Unique incident identifier

**Raw Data Structure**:
- **Location**: `data/raw/CALFIRE_PERIMETERS/Post1980SHP/`
- **Files**: Standard shapefile components (.shp, .shx, .dbf, .prj)
- **Record Count**: ~15,000+ individual fire records
- **File Size**: ~500 MB total for shapefile components

**Processing Pipeline Status**:
- **Raw Shapefile**: Available with complete attribute table (current status)
- **Unified NetCDF**: Target output `data/unified/fires_calfire.nc` (UTM Zone 11N, 3km resolution)
- **ML-Ready Parquet**: Target output `data/ml_ready/*.parquet` (daily fire occurrence targets)

**Processing Notes**:
- Vector polygons will be rasterized to the 3km grid using fire occurrence dates.
- Multiple fires per day/cell handled through priority rules (largest fire takes precedence).
- Fire progression modeling creates daily fire masks for ML target creation.
- Temporal buffering applied to create prediction targets (e.g., fire in next 7 days).
- Causation filtering available for targeted analysis (e.g., human vs. lightning fires)
```