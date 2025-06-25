# FPA FOD v6 Augmented Wildfire Dataset Documentation

## Overview

The FPA FOD v6 Augmented dataset is a comprehensive wildfire database that enhances the Fire Program Analysis Fire-Occurrence Database (FPA FOD) with nearly 270 additional physical, biological, social, and administrative attributes. This dataset is stored in Google Cloud Storage at `gs://fpa_fod_v6_augmented`.

## Dataset Characteristics

- **Total Records**: Over 2.3 million wildfire occurrences
- **Time Period**: 1992-2020 (29 years)
- **Geographic Coverage**: All U.S. states and territories
- **Total Attributes**: ~270 variables
- **Total Area Burned**: Over 180 million acres (72.8 million hectares)

## Core Fire Attributes (Original FPA FOD)

### Identification
- `FOD_ID`: Unique numeric record identifier
- `FPA_ID`: Unique identifier for tracking to source dataset
- `FIRE_NAME`: Name of the incident
- `FIRE_CODE`: Code for tracking suppression costs

### Temporal Information
- `FIRE_YEAR`: Calendar year of discovery
- `DISCOVERY_DATE`: Date fire was discovered (mm/dd/yyyy)
- `DISCOVERY_DOY`: Day of year discovered
- `DISCOVERY_TIME`: Time of day discovered (hhmm)
- `CONT_DATE`: Containment date
- `CONT_DOY`: Day of year contained
- `CONT_TIME`: Time of day contained

### Spatial Information
- `LATITUDE`: Latitude (NAD83, decimal degrees)
- `LONGITUDE`: Longitude (NAD83, decimal degrees)
- `STATE`: Two-letter state code
- `COUNTY`: County name
- `FIPS_CODE`: Five-digit FIPS county code
- `FIPS_NAME`: County name from FIPS

### Fire Characteristics
- `FIRE_SIZE`: Acres within final fire perimeter
- `FIRE_SIZE_CLASS`: Size classification (A-G scale)
  - A: 0-0.25 acres
  - B: 0.26-9.9 acres
  - C: 10.0-99.9 acres
  - D: 100-299 acres
  - E: 300-999 acres
  - F: 1000-4999 acres
  - G: 5000+ acres

### Cause Information
- `NWCG_CAUSE_CLASSIFICATION`: Broad classification (Human/Natural/Missing)
- `NWCG_GENERAL_CAUSE`: Specific cause category
  - Natural (lightning)
  - Recreation and ceremony
  - Equipment and vehicle use
  - Debris and open burning
  - Smoking
  - Arson/incendiarism
  - Railroad operations
  - Misuse of fire by minor
  - Power generation/transmission
  - Fireworks
  - Firearms and explosives
  - Other causes
  - Missing/undetermined

### Administrative
- `NWCG_REPORTING_AGENCY`: Agency preparing report
- `NWCG_REPORTING_UNIT_ID`: Unit identifier
- `SOURCE_SYSTEM`: Source database name
- `OWNER_DESCR`: Land owner/manager at ignition point

## Augmented Attributes (FPA FOD-Attributes)

### Weather and Climate Variables

#### Daily Weather (at ignition date and location)
- `precipitation`: Daily precipitation (mm)
- `temperature_max`: Maximum temperature (°C)
- `temperature_min`: Minimum temperature (°C)
- `relative_humidity`: Relative humidity (%)
- `specific_humidity`: Specific humidity (kg/kg)
- `wind_velocity`: Wind speed at 10m (m/s)
- `vapor_pressure_deficit`: VPD (kPa)
- `surface_radiation`: Downward shortwave radiation (W/m²)
- `evapotranspiration`: Reference ET (mm)

#### Fire Danger Indices
- `ERC`: Energy Release Component
- `BI`: Burning Index
- `FM100`: 100-hour dead fuel moisture (%)
- `FM1000`: 1000-hour dead fuel moisture (%)

#### Climate Context
- Climate normals (30-year averages) for all weather variables
- Climate percentiles (<10th, 10-30th, 30-50th, 50-70th, 70-90th, >90th)
- 5-day window statistics (min, max, mean) centered on ignition date

### Land Cover and Vegetation

#### Vegetation Indices
- `NDVI`: Normalized Difference Vegetation Index (monthly, 12 months prior)
- `EVI`: Enhanced Vegetation Index (monthly, 12 months prior)
- Daily NDVI values and monthly statistics

#### Land Cover Classifications
- `land_cover`: NLCD land cover type at ignition
- `EVC`: Existing vegetation cover
- `EVH`: Existing vegetation height
- `EVT`: Existing vegetation type
- Top 3 land covers within 1km radius

#### Special Vegetation Types
- `cheatgrass_cover`: Percent cover of cheatgrass
- `exotic_annual_grass_cover`: Percent cover of 17 exotic annual grasses
- `medusahead_cover`: Percent cover of medusahead
- `sandberg_bluegrass_cover`: Percent cover of native perennial grass
- `rangeland_production`: Annual biomass production (kg/ha)

### Topography

#### Elevation and Terrain
- `elevation`: Elevation above sea level (m)
- `slope`: Slope (degrees)
- `aspect`: Aspect (degrees from north)
- `TPI`: Topographic Position Index
- `TRI`: Terrain Ruggedness Index
- 1km radius statistics for all topographic variables

### Social and Economic Context

#### Population and Demographics
- `population_density`: People per hectare
- `population_density_1km`: Average within 1km radius

#### Social Vulnerability
- `social_vulnerability_index`: Overall SVI (0-1 scale)
- Four vulnerability dimensions:
  - Socioeconomic status
  - Household composition and disability
  - Housing type and transportation
  - Minority status and language
- 15 subdimensions of vulnerability

#### Economic Indicators
- `GDP_per_capita`: Gross domestic product per capita ($USD)
- `global_human_modification`: Human modification index (0-1)

#### Environmental Justice (CEJST)
- 107 variables related to:
  - Climate change burdens
  - Energy burdens
  - Health burdens
  - Housing burdens
  - Legacy pollution
  - Transportation access
  - Water and wastewater
  - Workforce development
- `disadvantaged_community`: Binary indicator

### Infrastructure and Accessibility

#### Roads and Access
- `distance_to_primary_road`: Distance to nearest primary road (m)
- `distance_to_secondary_road`: Distance to secondary road (m)
- `distance_to_local_road`: Distance to local road (m)
- `distance_to_trail`: Distance to trails (m)

#### Emergency Services
- `fire_stations_1km`: Number within 1km
- `fire_stations_5km`: Number within 5km
- `fire_stations_10km`: Number within 10km
- `fire_stations_20km`: Number within 20km
- `evacuation_time`: Time to medical facility (minutes)
- `suppression_difficulty_index`: SDI score

### Administrative and Management

#### Preparedness Levels
- `NPL`: National Preparedness Level (1-5)
- `GACC_preparedness_level`: Regional preparedness (1-5)
- `GACC`: Geographic Area Coordination Center

#### Land Management
- `GAP_status_code`: Conservation priority (1-4)
- `management_agency`: Managing agency
- `land_designation`: Management designation
- `pyrome`: Fire regime region (1-128)

#### Ecological Classification
- `ecoregion_l2`: Level 2 ecoregion (50 categories)
- `ecoregion_l3`: Level 3 ecoregion (182 categories)
- `fire_regime_group`: Historical fire regime

## Data Quality Considerations

### Spatial Accuracy
- Ignition locations accurate to at least 1 square mile (PLSS section)
- Many small fires may have reporting location rather than actual ignition
- 1km buffer statistics recommended for fine-resolution variables

### Temporal Coverage
- Complete coverage: 1992-2020 for core fire attributes
- Weather/climate data: 1979-present
- NDVI/EVI: 2000-present
- Social vulnerability: 2000, 2010, 2014, 2016, 2018, 2020
- Preparedness levels: 2007-2020
- Exotic grasses: 2016-2020

### Missing Data Patterns
- Non-federal fires may be underreported in some states/years
- Early years (1992-1999) have fewer augmented attributes
- Alaska and Hawaii have limited coverage for some attributes
- Small fires (<0.25 acres) may be inconsistently reported

## Analysis Recommendations

### Data Access
1. Requires Google Cloud authentication
2. Large dataset (>4GB) - consider sampling for initial exploration
3. Available in CSV or Parquet format

### Key Analysis Areas
1. **Climate-Fire Relationships**: Analyze weather/fire danger correlations
2. **Social Vulnerability**: Assess wildfire impacts on disadvantaged communities
3. **Vegetation Dynamics**: Study fuel type influences on fire behavior
4. **Temporal Trends**: Examine changes in fire patterns over 29 years
5. **Spatial Patterns**: Map fire hotspots and regional variations
6. **Human-Fire Interface**: Analyze proximity to infrastructure and populations

### Best Practices
- Use Polars for efficient large-scale data processing
- Consider spatial and temporal autocorrelation
- Account for reporting biases in small fires
- Validate findings with independent datasets
- Consider ensemble approaches for predictive modeling

## References

1. Pourmohamad et al. (2024). Physical, social, and biological attributes for improved understanding and prediction of wildfires: FPA FOD-Attributes dataset. Earth System Science Data, 16, 3045-3060.

2. Short, K.C. (2022). Spatial wildfire occurrence data for the United States, 1992-2020 [FPA_FOD_20221014]. 6th Edition. Fort Collins, CO: Forest Service Research Data Archive.

3. Additional documentation available at:
   - https://doi.org/10.5281/zenodo.8381129
   - https://fpafod.boisestate.edu

## Data Citation

When using this dataset, please cite:

```
Pourmohamad, Y., Abatzoglou, J., Belval, E., Short, K., Fleishman, E., Reeves, M., 
Nauslar, N., Higuera, P., Henderson, E., Ball, S., AghaKouchak, A., Prestemon, J., 
Olszewski, J., and Sadegh, M. (2023). Physical, Social, and Biological Attributes 
for Improved Understanding and Prediction of Wildfires: FPA FOD-Attributes Dataset 
(1.0). Zenodo. https://doi.org/10.5281/zenodo.8381129
```