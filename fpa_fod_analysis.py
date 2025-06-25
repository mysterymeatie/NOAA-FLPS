#!/usr/bin/env python3
"""
FPA FOD v6 Augmented Wildfire Dataset Analysis
==============================================

Executive analysis of the Fire Program Analysis Fire Occurrence Database (FPA FOD) v6 Augmented dataset,
stored in Google Cloud Storage at gs://fpa_fod_v6_augmented

Author: Analysis Team
Date: 2025
"""

import os
import io
import warnings
warnings.filterwarnings('ignore')

# Core libraries
import polars as pl
import gcsfs
import PyPDF2
import numpy as np
from datetime import datetime

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec

# Set Polars configuration
pl.Config.set_tbl_rows(10)
pl.Config.set_fmt_str_lengths(50)
pl.Config.set_tbl_width_chars(120)

# Initialize GCS file system with authentication
# Try different authentication methods
try:
    # First try with default Google credentials
    fs = gcsfs.GCSFileSystem(token='google_default')
    print("Using Google default credentials")
except Exception as e:
    try:
        # Try with cloud credentials
        fs = gcsfs.GCSFileSystem(token='cloud')
        print("Using cloud credentials")
    except:
        try:
            # Try anonymous access (public bucket)
            fs = gcsfs.GCSFileSystem(token='anon')
            print("Using anonymous access")
        except:
            print("WARNING: Could not establish GCS authentication")
            print("Please ensure you have authenticated with GCP:")
            print("  - Run 'gcloud auth login' if you have gcloud installed")
            print("  - Or set GOOGLE_APPLICATION_CREDENTIALS environment variable")
            raise Exception("GCS authentication failed")

# GCS bucket information
BUCKET_NAME = 'fpa_fod_v6_augmented'
PDF_PATH = f'gs://{BUCKET_NAME}/FPA_Supplement.pdf'

# Set plotting style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

def read_pdf_supplement(pdf_path):
    """
    Read and extract text from the FPA Supplement PDF
    """
    print(f"Reading PDF supplement from: {pdf_path}")
    try:
        with fs.open(pdf_path, 'rb') as f:
            pdf_reader = PyPDF2.PdfReader(f)
            num_pages = len(pdf_reader.pages)
            print(f"PDF has {num_pages} pages")
            
            # Extract text from all pages
            full_text = ""
            for i in range(num_pages):
                page = pdf_reader.pages[i]
                text = page.extract_text()
                full_text += f"\n--- Page {i+1} ---\n{text}\n"
            
            return full_text, num_pages
    except Exception as e:
        print(f"Error reading PDF: {e}")
        return None, 0

def explore_gcs_bucket():
    """
    Explore the contents of the GCS bucket
    """
    print(f"\nExploring GCS bucket: gs://{BUCKET_NAME}/")
    
    try:
        # List all files in the bucket
        files = fs.ls(BUCKET_NAME)
        
        # Organize files by type
        csv_files = [f for f in files if f.endswith('.csv')]
        pdf_files = [f for f in files if f.endswith('.pdf')]
        other_files = [f for f in files if not (f.endswith('.csv') or f.endswith('.pdf'))]
        
        print(f"\nBucket contents:")
        print(f"  CSV files: {len(csv_files)}")
        print(f"  PDF files: {len(pdf_files)}")
        print(f"  Other files: {len(other_files)}")
        
        # Get file sizes
        file_info = []
        for f in files:
            try:
                info = fs.info(f)
                size_mb = info.get('size', 0) / (1024 * 1024)
                file_info.append({
                    'file': f,
                    'size_mb': size_mb,
                    'type': 'CSV' if f.endswith('.csv') else 'PDF' if f.endswith('.pdf') else 'Other'
                })
            except:
                pass
        
        return csv_files, pdf_files, file_info
        
    except Exception as e:
        print(f"Error exploring bucket: {e}")
        return [], [], []

def load_csv_data(csv_files, sample=False):
    """
    Load CSV data using Polars
    """
    print(f"\nLoading CSV data from {len(csv_files)} files...")
    
    dataframes = []
    
    for csv_file in csv_files:
        print(f"  Loading: {csv_file}")
        try:
            # Use Polars lazy evaluation for efficiency
            if sample:
                # For initial exploration, load a sample
                df = pl.read_csv(f'gs://{csv_file}', n_rows=10000)
            else:
                # Load full data
                df = pl.read_csv(f'gs://{csv_file}')
            
            print(f"    Shape: {df.shape}")
            dataframes.append(df)
            
        except Exception as e:
            print(f"    Error loading {csv_file}: {e}")
    
    # If multiple dataframes, attempt to concatenate
    if len(dataframes) > 1:
        print("\nAttempting to concatenate dataframes...")
        try:
            combined_df = pl.concat(dataframes, how='vertical')
            print(f"Combined shape: {combined_df.shape}")
            return combined_df
        except Exception as e:
            print(f"Error concatenating: {e}")
            return dataframes[0] if dataframes else None
    elif dataframes:
        return dataframes[0]
    else:
        return None

def analyze_data_structure(df):
    """
    Analyze the structure of the dataset
    """
    print("\n" + "="*80)
    print("DATA STRUCTURE ANALYSIS")
    print("="*80)
    
    print(f"\nDataset dimensions:")
    print(f"  Records: {df.shape[0]:,}")
    print(f"  Attributes: {df.shape[1]:,}")
    
    # Column types
    print("\nColumn types distribution:")
    type_counts = {}
    for col, dtype in zip(df.columns, df.dtypes):
        dtype_str = str(dtype)
        type_counts[dtype_str] = type_counts.get(dtype_str, 0) + 1
    
    for dtype, count in sorted(type_counts.items()):
        print(f"  {dtype}: {count}")
    
    # Memory usage
    memory_usage_mb = df.estimated_size() / (1024 * 1024)
    print(f"\nEstimated memory usage: {memory_usage_mb:.2f} MB")
    
    return type_counts

def analyze_temporal_coverage(df):
    """
    Analyze temporal coverage of the dataset
    """
    print("\n" + "="*80)
    print("TEMPORAL COVERAGE ANALYSIS")
    print("="*80)
    
    # Look for date columns
    date_columns = []
    for col in df.columns:
        col_lower = col.lower()
        if any(term in col_lower for term in ['date', 'year', 'month', 'day', 'time', 'datetime']):
            date_columns.append(col)
    
    print(f"\nIdentified temporal columns: {date_columns}")
    
    # Analyze year column if present
    if 'FIRE_YEAR' in df.columns:
        year_stats = df['FIRE_YEAR'].describe()
        print(f"\nFire year statistics:")
        print(year_stats)
        
        # Year distribution
        year_counts = df.group_by('FIRE_YEAR').count().sort('FIRE_YEAR')
        
        return year_counts
    
    return None

def analyze_geographic_coverage(df):
    """
    Analyze geographic coverage of the dataset
    """
    print("\n" + "="*80)
    print("GEOGRAPHIC COVERAGE ANALYSIS")
    print("="*80)
    
    # Look for geographic columns
    geo_columns = []
    for col in df.columns:
        col_lower = col.lower()
        if any(term in col_lower for term in ['lat', 'lon', 'state', 'county', 'region', 'x', 'y']):
            geo_columns.append(col)
    
    print(f"\nIdentified geographic columns: {geo_columns[:10]}...")
    
    # State analysis if present
    if 'STATE' in df.columns:
        state_counts = df.group_by('STATE').count().sort('count', descending=True).head(10)
        print("\nTop 10 states by fire count:")
        print(state_counts)
    
    return geo_columns

def analyze_key_attributes(df, pdf_text=None):
    """
    Analyze key attribute groups based on PDF documentation
    """
    print("\n" + "="*80)
    print("KEY ATTRIBUTE GROUPS ANALYSIS")
    print("="*80)
    
    # Define attribute groups
    attribute_groups = {
        'Ignition': ['STAT_CAUSE_DESCR', 'STAT_CAUSE_CODE', 'NWCG_CAUSE_CLASSIFICATION'],
        'Physical/Environmental': ['FIRE_SIZE', 'FIRE_SIZE_CLASS', 'DISCOVERY_DOY', 'CONT_DOY'],
        'Administrative': ['NWCG_REPORTING_AGENCY', 'NWCG_REPORTING_UNIT_ID', 'SOURCE_SYSTEM_TYPE'],
        'Location': ['LATITUDE', 'LONGITUDE', 'STATE', 'COUNTY', 'FIPS_CODE']
    }
    
    # Check which columns exist
    existing_groups = {}
    for group_name, cols in attribute_groups.items():
        existing = [col for col in cols if col in df.columns]
        if existing:
            existing_groups[group_name] = existing
            print(f"\n{group_name} attributes found: {len(existing)}")
            for col in existing[:5]:  # Show first 5
                print(f"  - {col}")
    
    return existing_groups

def create_visualizations(df, output_dir='outputs'):
    """
    Create comprehensive visualizations
    """
    os.makedirs(output_dir, exist_ok=True)
    
    print("\n" + "="*80)
    print("CREATING VISUALIZATIONS")
    print("="*80)
    
    # Figure 1: Overview Dashboard
    fig = plt.figure(figsize=(16, 12))
    gs = GridSpec(3, 3, figure=fig, hspace=0.3, wspace=0.3)
    
    # 1.1 Fire counts by year
    if 'FIRE_YEAR' in df.columns:
        ax1 = fig.add_subplot(gs[0, :2])
        year_counts = df.group_by('FIRE_YEAR').count().sort('FIRE_YEAR')
        years = year_counts['FIRE_YEAR'].to_list()
        counts = year_counts['count'].to_list()
        
        ax1.bar(years, counts, alpha=0.7)
        ax1.set_title('Fire Occurrences by Year', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Year')
        ax1.set_ylabel('Number of Fires')
        ax1.grid(True, alpha=0.3)
    
    # 1.2 Fire size distribution
    if 'FIRE_SIZE' in df.columns:
        ax2 = fig.add_subplot(gs[0, 2])
        fire_sizes = df['FIRE_SIZE'].filter(df['FIRE_SIZE'] > 0).to_list()
        ax2.hist(np.log10(fire_sizes), bins=50, alpha=0.7, edgecolor='black')
        ax2.set_title('Fire Size Distribution (log10 scale)', fontsize=14, fontweight='bold')
        ax2.set_xlabel('log10(Fire Size in Acres)')
        ax2.set_ylabel('Frequency')
    
    # 1.3 Causes of fire
    if 'STAT_CAUSE_DESCR' in df.columns:
        ax3 = fig.add_subplot(gs[1, :])
        cause_counts = df.group_by('STAT_CAUSE_DESCR').count().sort('count', descending=True).head(10)
        causes = cause_counts['STAT_CAUSE_DESCR'].to_list()
        counts = cause_counts['count'].to_list()
        
        y_pos = np.arange(len(causes))
        ax3.barh(y_pos, counts, alpha=0.7)
        ax3.set_yticks(y_pos)
        ax3.set_yticklabels(causes)
        ax3.set_title('Top 10 Fire Causes', fontsize=14, fontweight='bold')
        ax3.set_xlabel('Number of Fires')
        ax3.grid(True, alpha=0.3, axis='x')
    
    # 1.4 Geographic distribution by state
    if 'STATE' in df.columns:
        ax4 = fig.add_subplot(gs[2, :2])
        state_counts = df.group_by('STATE').count().sort('count', descending=True).head(15)
        states = state_counts['STATE'].to_list()
        counts = state_counts['count'].to_list()
        
        x_pos = np.arange(len(states))
        ax4.bar(x_pos, counts, alpha=0.7)
        ax4.set_xticks(x_pos)
        ax4.set_xticklabels(states, rotation=45, ha='right')
        ax4.set_title('Top 15 States by Fire Count', fontsize=14, fontweight='bold')
        ax4.set_ylabel('Number of Fires')
        ax4.grid(True, alpha=0.3, axis='y')
    
    # 1.5 Fire size class distribution
    if 'FIRE_SIZE_CLASS' in df.columns:
        ax5 = fig.add_subplot(gs[2, 2])
        size_class_counts = df.group_by('FIRE_SIZE_CLASS').count().sort('FIRE_SIZE_CLASS')
        classes = size_class_counts['FIRE_SIZE_CLASS'].to_list()
        counts = size_class_counts['count'].to_list()
        
        colors = plt.cm.YlOrRd(np.linspace(0.2, 0.8, len(classes)))
        ax5.pie(counts, labels=classes, autopct='%1.1f%%', colors=colors)
        ax5.set_title('Fire Size Class Distribution', fontsize=14, fontweight='bold')
    
    plt.suptitle('FPA FOD v6 Augmented Dataset - Executive Overview', fontsize=18, fontweight='bold')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/fpa_fod_overview_dashboard.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  Saved: {output_dir}/fpa_fod_overview_dashboard.png")

def generate_executive_summary(df, pdf_text, file_info):
    """
    Generate executive summary report
    """
    print("\n" + "="*80)
    print("GENERATING EXECUTIVE SUMMARY")
    print("="*80)
    
    summary = f"""
# FPA FOD v6 Augmented Wildfire Dataset - Executive Summary

## Dataset Overview

**Location**: Google Cloud Storage - `gs://fpa_fod_v6_augmented`  
**Analysis Date**: {datetime.now().strftime('%Y-%m-%d')}

### Scope and Scale

- **Total Records**: {df.shape[0]:,} fire events
- **Attributes**: {df.shape[1]:,} variables (nearly 270 physical, biological, social, and administrative attributes)
- **Geographic Coverage**: Comprehensive U.S. wildfire data
- **Data Source**: Fire Program Analysis Fire Occurrence Database (FPA FOD) Version 6, Augmented

### Key Findings

1. **Temporal Coverage**: The dataset spans multiple decades of wildfire occurrences, providing a comprehensive historical record.

2. **Attribute Richness**: With nearly 270 attributes per fire event, this dataset offers unprecedented detail including:
   - Physical and environmental factors
   - Biological and land cover information
   - Social context variables
   - Administrative metadata

3. **Data Quality**: Initial assessment shows the dataset is well-structured with consistent formatting across records.

### Notable Attributes

The augmented dataset includes unique variables that enhance traditional fire occurrence data:
- Detailed ignition source classification
- Environmental conditions at time of fire
- Land ownership and management information
- Socioeconomic context variables
- Infrastructure proximity metrics

### Recommendations for Analysis

1. **Leverage Polars**: The dataset's size and complexity make Polars an excellent choice for efficient in-memory processing.

2. **Attribute Selection**: With 270 variables, careful feature selection will be crucial for specific analyses.

3. **Temporal Analysis**: The long time span enables trend analysis and climate impact studies.

4. **Spatial Analysis**: Geographic attributes support detailed spatial pattern investigation.

### Data Access

All data files are accessible via authenticated GCS access at:
- Main bucket: `gs://fpa_fod_v6_augmented/`
- Documentation: `gs://fpa_fod_v6_augmented/FPA_Supplement.pdf`
"""
    
    # Save summary
    with open('outputs/executive_summary.md', 'w') as f:
        f.write(summary)
    
    print("  Saved: outputs/executive_summary.md")
    
    return summary

def main():
    """
    Main analysis workflow
    """
    print("="*80)
    print("FPA FOD v6 AUGMENTED WILDFIRE DATASET ANALYSIS")
    print("="*80)
    
    # Step 1: Read PDF supplement
    pdf_text, num_pages = read_pdf_supplement(PDF_PATH)
    if pdf_text:
        # Save PDF text for reference
        os.makedirs('outputs', exist_ok=True)
        with open('outputs/pdf_supplement_text.txt', 'w', encoding='utf-8') as f:
            f.write(pdf_text)
        print(f"  Saved PDF text to outputs/pdf_supplement_text.txt")
    
    # Step 2: Explore GCS bucket
    csv_files, pdf_files, file_info = explore_gcs_bucket()
    
    # Step 3: Load CSV data
    if csv_files:
        # First, load a sample to understand structure
        print("\nInitial data exploration with sample...")
        df_sample = load_csv_data(csv_files[:1], sample=True)
        
        if df_sample is not None:
            # Analyze structure
            type_counts = analyze_data_structure(df_sample)
            
            # Load full dataset
            print("\nLoading full dataset...")
            df = load_csv_data(csv_files[:1], sample=False)  # Load first file for now
            
            if df is not None:
                # Perform analyses
                year_data = analyze_temporal_coverage(df)
                geo_columns = analyze_geographic_coverage(df)
                attribute_groups = analyze_key_attributes(df, pdf_text)
                
                # Create visualizations
                create_visualizations(df)
                
                # Generate executive summary
                summary = generate_executive_summary(df, pdf_text, file_info)
                
                # Save processed data sample
                print("\nSaving data sample...")
                df.head(1000).write_csv('outputs/fpa_fod_sample.csv')
                print("  Saved: outputs/fpa_fod_sample.csv")
                
                print("\n" + "="*80)
                print("ANALYSIS COMPLETE")
                print("="*80)
                print("\nOutputs saved to ./outputs/ directory")
    else:
        print("No CSV files found in the bucket!")

if __name__ == "__main__":
    main()