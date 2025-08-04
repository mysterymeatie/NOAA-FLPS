# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a NOAA internship project focused on wildfire risk prediction for Southern California. The goal is to construct a model that combines multiple datasets and machine learning approaches to predict and evaluate wildfire risk in the region. The project emphasizes data quality and quantity over model architecture complexity.

**Key Deliverables:**
- Package of 30-50 datasets with varying granularity, modality, and spatial/temporal coverage
- Meta-learner model combining outputs from various base models (CNNs for satellite imagery, XGBoost for tabular data, etc.)

## Environment Setup

The project uses conda for environment management:
```bash
conda env create -f environment.yml
conda activate wf
```

The environment (`wf`) includes comprehensive packages for:
- Geospatial analysis (geopandas, cartopy, rasterio, folium)
- Weather data processing (xarray, cfgrib, herbie-data)
- Machine learning (scikit-learn, scipy)
- Visualization (matplotlib, seaborn, plotly, altair)
- Jupyter notebook development

## Data Architecture

### Data Directory Structure
```
data/
├── raw/               # Raw downloaded data
│   ├── CALFIRE_PERIMETERS/
│   ├── NOAA_HRRR/    # HRRR GRIB2 files by date folders (YYYYMMDD)
│   ├── MODIS_NDVI_250m/
│   └── SRTM/
├── processed/         # Processed/cleaned data
│   └── HRRR/         # Yearly NetCDF files (hrrr_socal_hourly_YYYY.nc)
└── scripts/          # Data processing scripts
    ├── HRRR/
    └── MODIS/
```

### Key Datasets
- **HRRR (High-Resolution Rapid Refresh)**: Weather data with variables like temperature, humidity, wind, precipitation
- **CALFIRE Perimeters**: Wildfire boundary data from California Department of Forestry
- **MODIS**: Satellite imagery for vegetation indices (NDVI, EVI)
- **SRTM**: Digital elevation model data

## Data Processing Workflow

### HRRR Weather Data Pipeline
The HRRR data processing follows a two-step approach:

1. **Download**: `data/scripts/HRRR/download_hrrr.py`
   ```bash
   # Download with defaults (2014-present, daily at 1 PM PST)
   python data/scripts/HRRR/download_hrrr.py --use-defaults
   
   # Custom date range
   python data/scripts/HRRR/download_hrrr.py --start-date 2022-01-01 --end-date 2022-12-31
   ```

2. **Process**: `data/scripts/HRRR/process_hrrr.py`
   ```bash
   # Process GRIB2 files to yearly NetCDF files
   python data/scripts/HRRR/process_hrrr.py
   ```

**Important Notes:**
- GRIB2 files are downloaded in monthly batches to prevent hanging
- Processing handles cfgrib coordinate conflicts by opening different atmospheric levels separately
- Output NetCDF files are clipped to Southern California bounding box: lat 31-38°N, lon 236-245°E
- Data includes multiple levels: 2m (temperature, humidity), 10m (wind), surface (precipitation)

### MODIS Data Processing
Located in `data/scripts/MODIS/` with:
- `download_modis.py`: Download MODIS data
- `modis_processor.py`: Process MODIS imagery

## Analysis and Visualization

### Jupyter Notebooks
Located in `EDA/notebooks/`:
- `HRRR.ipynb`: Comprehensive weather data analysis with time series and geospatial visualization
- `CALFIRE.ipynb`: Wildfire perimeter analysis
- `MODIS_Advanced_Analysis.ipynb`: Satellite imagery analysis
- `MODIS_EVI_NDVI.ipynb`: Vegetation index analysis
- `SRTM.ipynb`: Elevation data analysis

### Key Analysis Features
- Time series analysis of regional weather patterns
- Geospatial mapping with Santiago Fire perimeter overlay
- Wind vector visualization
- Multi-variable correlation analysis
- Seasonal and diurnal cycle identification

## Development Patterns

### Data Loading
Use xarray for NetCDF files:
```python
import xarray as xr
ds = xr.open_dataset('data/processed/HRRR/hrrr_socal_hourly_2024.nc')
```

### Geospatial Visualization
Standard pattern using cartopy:
```python
import cartopy.crs as ccrs
import cartopy.feature as cfeature

ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_extent([-118.8, -116.8, 32.7, 34.5], crs=ccrs.PlateCarree())  # SoCal extent
```

### Error Handling for GRIB Files
The codebase handles common GRIB processing issues:
- Coordinate conflicts between atmospheric levels
- Corrupted/incomplete files with PrematureEndOfFileError
- Missing index files for historical data

## Common Tasks

### Add New Data Source
1. Create download script in `data/scripts/[SOURCE]/`
2. Create processing script to convert to analysis-ready format
3. Update data directory structure
4. Create EDA notebook for initial analysis

### Run Complete HRRR Pipeline
```bash
# Download all historical data (warning: ~32GB)
python data/scripts/HRRR/download_hrrr.py --use-defaults

# Process to yearly NetCDF files
python data/scripts/HRRR/process_hrrr.py
```

### Launch Jupyter Analysis
```bash
conda activate wf
jupyter lab
# Navigate to EDA/notebooks/
```

## Important Considerations

- **Data Volume**: HRRR dataset is ~32GB for full historical download
- **Memory Usage**: NetCDF files can be large; use chunking for big datasets
- **Coordinate Systems**: All geospatial data uses standard lat/lon (EPSG:4326)
- **Time Zones**: HRRR data is in UTC; default download time 21:00 UTC = 1 PM PST
- **Regional Focus**: All processing is optimized for Southern California bounding box
- **File Formats**: Raw data in native formats (GRIB2, GeoTIFF), processed data in NetCDF/CSV

## Reference Information

- **Herbie Documentation**: https://herbie-data.readthedocs.io/en/latest/
- **HRRR Variables**: Temperature (t2m), humidity (r2, sh2, d2m), wind (u10, v10, max_10si), precipitation (prate)
- **Coordinate Conventions**: heightAboveGround levels (2m, 10m), surface level
- **Santiago Fire Perimeter**: Available as GeoJSON for overlay visualization