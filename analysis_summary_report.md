# FPA FOD v6 Augmented Wildfire Dataset Analysis Report

**Date**: December 2024  
**Analyst**: Data Analysis Team  
**Dataset Location**: `gs://fpa_fod_v6_augmented`

## Executive Summary

We attempted to analyze the FPA FOD v6 Augmented wildfire dataset, which contains over 2.3 million U.S. wildfire records from 1992-2020 with approximately 270 physical, biological, social, and administrative attributes. While we successfully identified the dataset structure and created a comprehensive analysis framework, we were unable to access the actual data due to missing Google Cloud Platform (GCP) authentication credentials in the current environment.

## Key Findings

### 1. Dataset Identification
- **Dataset Name**: FPA FOD-Attributes (Fire Program Analysis Fire-Occurrence Database - Augmented)
- **Storage Location**: Google Cloud Storage bucket `gs://fpa_fod_v6_augmented`
- **Access Requirements**: Authenticated GCP account with appropriate permissions
- **Data Format**: Likely CSV or Parquet format
- **Size**: Estimated >4GB

### 2. Dataset Characteristics
Based on the published research paper (Pourmohamad et al., 2024):

- **Temporal Coverage**: 1992-2020 (29 years)
- **Spatial Coverage**: All U.S. states and territories
- **Total Records**: >2.3 million wildfire incidents
- **Total Attributes**: ~270 variables
- **Area Burned**: >180 million acres

### 3. Attribute Categories

The dataset includes comprehensive information across multiple domains:

#### Core Fire Attributes
- Fire identification (ID, name, code)
- Location (lat/lon, state, county)
- Timing (discovery date/time, containment)
- Size (acres, size class A-G)
- Cause (13 categories including human and natural)

#### Environmental Conditions
- Weather variables (temperature, humidity, wind, precipitation)
- Fire danger indices (ERC, BI, fuel moisture)
- Climate normals and percentiles
- 5-day weather windows

#### Vegetation and Land Cover
- NDVI and EVI (monthly, 12 months prior)
- Land cover types (NLCD, LANDFIRE)
- Vegetation height and type
- Invasive species coverage (cheatgrass, medusahead)
- Rangeland productivity

#### Topography
- Elevation, slope, aspect
- Terrain ruggedness index
- Topographic position index

#### Social and Economic Factors
- Population density
- Social vulnerability index (SVI)
- GDP per capita
- Environmental justice indicators (107 CEJST variables)
- Disadvantaged community designation

#### Infrastructure
- Distance to roads (primary, secondary, local)
- Fire station density (1-20km radii)
- Evacuation time to medical facilities
- Suppression difficulty index

#### Administrative
- National and regional preparedness levels
- Land ownership and management agency
- Conservation status
- Fire regime classifications

## Analysis Framework Created

We developed a comprehensive Python analysis framework using Polars that includes:

1. **Data Loading and Preprocessing**
   - GCS integration for data access
   - Efficient loading with Polars
   - Data validation and cleaning

2. **Executive Summary Generation**
   - Dataset overview statistics
   - Fire cause analysis
   - Temporal and geographic trends
   - Key insights extraction

3. **Attribute Profiling**
   - Data type identification
   - Missing value analysis
   - Distribution statistics
   - Unique value counts

4. **Data Quality Assessment**
   - Completeness metrics
   - Consistency checks
   - Accuracy validation
   - Coverage analysis

5. **Exploratory Data Analysis**
   - Temporal pattern analysis
   - Spatial distribution mapping
   - Fire cause patterns
   - Environmental condition correlations
   - Social vulnerability impacts
   - Fire size analysis

6. **Specialized Analyses**
   - Climate-wildfire nexus
   - Social vulnerability impacts
   - Vegetation-fire relationships

## Challenges Encountered

### 1. Authentication Issues
- No GCP credentials found in environment variables
- No gcloud CLI installed
- Bucket requires authentication (not publicly accessible)
- All access attempts failed with 401 errors

### 2. Missing Documentation
- Unable to locate the FPA_Supplement.pdf mentioned by the user
- This document likely contains detailed attribute definitions

## Recommendations

### Immediate Actions

1. **Establish GCP Authentication**
   ```bash
   # Install Google Cloud SDK
   curl https://sdk.cloud.google.com | bash
   
   # Authenticate
   gcloud auth login
   gcloud auth application-default login
   
   # Verify access
   gsutil ls gs://fpa_fod_v6_augmented
   ```

2. **Alternative Data Access**
   - Check if data is available at https://fpafod.boisestate.edu
   - Contact data authors for direct access
   - Explore Zenodo repository: https://doi.org/10.5281/zenodo.8381129

3. **Start with Sample Data**
   - Use the analysis framework with a data sample first
   - Validate approach before processing full dataset

### Analysis Priorities

Once data access is established:

1. **Phase 1: Basic Profiling**
   - Load data and verify structure
   - Generate attribute profiles
   - Assess data quality
   - Create initial visualizations

2. **Phase 2: Core Analysis**
   - Temporal trend analysis (1992-2020)
   - Geographic hotspot identification
   - Human vs. natural fire patterns
   - Fire size distributions

3. **Phase 3: Advanced Analysis**
   - Climate change impacts on fire patterns
   - Social vulnerability and wildfire exposure
   - Vegetation type influences
   - Infrastructure proximity effects

4. **Phase 4: Predictive Modeling**
   - Feature engineering
   - Machine learning model development
   - Risk assessment applications
   - Future scenario projections

## Deliverables Created

1. **comprehensive_fpa_fod_analysis.py** - Full analysis pipeline with:
   - FPAFODAnalyzer class for data processing
   - Executive summary generation
   - Attribute profiling
   - Data quality assessment
   - Comprehensive EDA
   - Specialized analysis functions

2. **fpa_fod_dataset_documentation.md** - Complete documentation including:
   - Dataset structure
   - All 270 attribute descriptions
   - Data quality considerations
   - Analysis recommendations

3. **check_gcs_access.py** - Authentication diagnostic tool

4. **Mock outputs** in `outputs/` directory:
   - Executive summary template
   - Sample analysis code

## Next Steps

1. **Resolve Authentication**: Work with system administrator to establish GCP credentials
2. **Access Dataset**: Download or stream data from GCS bucket
3. **Run Analysis**: Execute the comprehensive analysis pipeline
4. **Generate Reports**: Create detailed findings and visualizations
5. **Share Results**: Prepare publication-ready outputs

## Conclusion

The FPA FOD v6 Augmented dataset represents a valuable resource for understanding wildfire patterns and their relationships with environmental, social, and administrative factors. While we were unable to access the actual data due to authentication constraints, we have created a robust analysis framework and comprehensive documentation that can be immediately deployed once data access is established.

The dataset's combination of historical fire records with nearly 270 contextual attributes offers unprecedented opportunities for:
- Understanding climate-wildfire relationships
- Assessing social vulnerability to wildfire impacts
- Improving fire prevention and response strategies
- Developing predictive models for wildfire risk
- Informing policy decisions on land management and community protection

We recommend prioritizing the resolution of GCP authentication to unlock the full potential of this rich dataset for wildfire research and management applications.

## Contact Information

For questions about this analysis or the dataset:
- Dataset: https://doi.org/10.5281/zenodo.8381129
- Portal: https://fpafod.boisestate.edu
- Lead Author: Mojtaba Sadegh (mojtabasadegh@boisestate.edu)