# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a NOAA internship project focused on wildfire risk prediction in Southern California. The project aims to construct a model using GCP infrastructure with emphasis on data quality and quantity over model architecture complexity.

**Key Deliverables:**
- Package of 30-50 datasets with varying granularity, modality, spatial/temporal coverage
- Meta-learner model combining outputs from multiple base models (CNNs for satellite imagery, XGBoost for tabular data)

## Development Environment

**Primary Environment:** Conda environment named `wf`
- Activate with: `conda activate wf`
- Environment definition: `environment.yml`
- Python 3.11.13 with extensive geospatial, machine learning, and data science libraries

**Key Dependencies:**
- Geospatial: `geopandas`, `rasterio`, `cartopy`, `pyproj`, `shapely`, `folium`
- Weather/Climate: `herbie-data`, `metpy`, `cfgrib`, `pygrib`, `eccodes`
- Data Processing: `xarray`, `pandas`, `numpy`, `dask`, `zarr`
- ML/Analysis: `scikit-learn`, `matplotlib`, `seaborn`, `plotly`, `bokeh`
- Cloud/Storage: `fsspec`, `s3fs`, `gcsfs`, `google-cloud-storage`
- Jupyter: `jupyterlab`, `ipywidgets`, `ipyleaflet`

## Repository Structure

### Data Organization
```
data/
├── raw/                    # Original datasets
│   ├── CALFIRE_PERIMETERS/ # California fire perimeter data
│   └── NOAA_HRRR/         # High-Resolution Rapid Refresh weather data
├── processed/             # Processed datasets ready for analysis
│   └── HRRR/             # Processed HRRR NetCDF files
└── scripts/              # Data processing pipelines
    ├── HRRR/             # HRRR weather data processing
    └── MODIS/            # MODIS satellite data processing
```

### Analysis and Exploration
```
EDA/
├── CALFIRE_PERIMETERS/   # Fire perimeter analysis notebooks
├── NASA/                 # NASA data analysis (HRRR, MODIS)
```

### Visualization Scripts
- `interactive_wildfire_map.py` - Interactive wildfire visualization
- `wildfire_perimeter_map.py` - Fire perimeter mapping

## Data Processing Pipelines

### HRRR Weather Data (`data/scripts/HRRR/`)
**Download Pipeline:** `download_hrrr_data.py`
- Downloads High-Resolution Rapid Refresh (HRRR) weather data from NOAA
- Uses FastHerbie library for efficient multi-level atmospheric data retrieval
- Downloads variables from 2m, 10m, and surface levels
- Monthly batching to prevent hanging on problematic files
- Default download size: ~32GB
- Run with `--use-defaults` flag for standard configuration

**Processing Pipeline:** `process_hrrr_data.py` 
- Converts GRIB2 files to analysis-ready NetCDF format
- Handles cfgrib coordinate conflicts by processing different atmospheric levels separately
- Chunks output by year for performance
- Clips data to Southern California bounding box (31-38°N, 236-245°E)
- Uses netcdf4 engine with compression

### MODIS Satellite Data (`data/scripts/MODIS/`)
- `download_modis.py` - MODIS satellite data retrieval
- `modis_processor.py` - MODIS data processing and analysis

### Test Pipeline
`EDA/NASA/test_pipeline_ultimate.py` - Ultimate HRRR processing test script with Lambert Conformal Conic projection handling

## Development Commands

**Environment Setup:**
```bash
conda env create -f environment.yml
conda activate wf
```

**Data Processing:**
```bash
# Download HRRR data with defaults
python data/scripts/HRRR/download_hrrr_data.py --use-defaults

# Process HRRR GRIB files to NetCDF
python data/scripts/HRRR/process_hrrr_data.py

# Run HRRR pipeline test
python EDA/NASA/test_pipeline_ultimate.py
```

**Jupyter Lab:**
```bash
jupyter lab  # Start JupyterLab for interactive analysis
```

## Technical Notes

### HRRR Data Specifics
- Uses Lambert Conformal Conic projection
- Focus region: Southern California (31-38°N, 115-123°W)
- Variables: Temperature, humidity, wind (2m/10m), precipitation (surface)
- Analysis data (fxx=0) preferred over forecasts

### Geospatial Considerations
- Longitude coordinates use 0-360° system (236-245° = 115-123°W)
- California fire perimeter data includes pre-1980 and post-1980 datasets
- Shapefiles available in both full and LASSO'd (region-specific) formats

### File Handling
- GRIB2 files may have coordinate conflicts requiring separate processing by atmospheric level
- NetCDF outputs use compression and yearly chunking for efficiency
- Graceful handling of corrupted/incomplete weather data files

## Common Workflows

1. **Data Acquisition:** Use download scripts for HRRR and MODIS data
2. **Data Processing:** Convert raw formats to analysis-ready NetCDF/CSV
3. **Exploratory Analysis:** Use Jupyter notebooks in EDA/ directory
4. **Visualization:** Generate maps and plots using provided Python scripts
5. **Model Development:** Combine processed datasets for wildfire risk modeling

## Important Notes

- All operations target GCP deployment
- Data quality and quantity prioritized over model complexity
- Focus on Southern California wildfire risk prediction
- Multiple data modalities (satellite, weather, fire perimeters) integration required