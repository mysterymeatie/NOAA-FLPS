#!/usr/bin/env python3
"""
FPA FOD v6 Augmented Wildfire Dataset Analysis - Using Google Cloud Storage Client
==================================================================================
"""

import os
import io
import warnings
warnings.filterwarnings('ignore')

# Core libraries
import polars as pl
from google.cloud import storage
import PyPDF2
import numpy as np
from datetime import datetime

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.gridspec import GridSpec

# Set Polars configuration
pl.Config.set_tbl_rows(10)
pl.Config.set_fmt_str_lengths(50)
pl.Config.set_tbl_width_chars(120)

# GCS bucket information
BUCKET_NAME = 'fpa_fod_v6_augmented'

# Set plotting style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

def test_gcs_access():
    """Test GCS access and authentication"""
    print("Testing GCS access...")
    try:
        # Create a client
        client = storage.Client()
        
        # Try to get the bucket
        bucket = client.bucket(BUCKET_NAME)
        
        # List some files to test access
        blobs = list(bucket.list_blobs(max_results=5))
        print(f"✓ Successfully connected to bucket: {BUCKET_NAME}")
        print(f"  Found {len(blobs)} files (showing first 5)")
        
        return client, bucket
    except Exception as e:
        print(f"✗ Failed to access GCS bucket: {e}")
        print("\nTroubleshooting steps:")
        print("1. Ensure you have authenticated with GCP")
        print("2. Check if you have access to the bucket: gs://fpa_fod_v6_augmented")
        print("3. Try running: gcloud auth application-default login")
        return None, None

def explore_bucket_contents(bucket):
    """Explore and list bucket contents"""
    print(f"\nExploring bucket contents...")
    
    csv_files = []
    pdf_files = []
    other_files = []
    
    # List all blobs
    for blob in bucket.list_blobs():
        if blob.name.endswith('.csv'):
            csv_files.append(blob)
        elif blob.name.endswith('.pdf'):
            pdf_files.append(blob)
        else:
            other_files.append(blob)
    
    print(f"\nBucket statistics:")
    print(f"  CSV files: {len(csv_files)}")
    print(f"  PDF files: {len(pdf_files)}")
    print(f"  Other files: {len(other_files)}")
    
    # Show file details
    print("\nCSV files found:")
    for blob in csv_files[:5]:  # Show first 5
        size_mb = blob.size / (1024 * 1024)
        print(f"  - {blob.name} ({size_mb:.2f} MB)")
    
    if len(csv_files) > 5:
        print(f"  ... and {len(csv_files) - 5} more")
    
    print("\nPDF files found:")
    for blob in pdf_files:
        size_mb = blob.size / (1024 * 1024)
        print(f"  - {blob.name} ({size_mb:.2f} MB)")
    
    return csv_files, pdf_files

def read_pdf_from_gcs(bucket, pdf_blob):
    """Read PDF content from GCS"""
    print(f"\nReading PDF: {pdf_blob.name}")
    
    try:
        # Download PDF content to memory
        pdf_bytes = pdf_blob.download_as_bytes()
        
        # Read with PyPDF2
        pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        num_pages = len(pdf_reader.pages)
        print(f"  PDF has {num_pages} pages")
        
        # Extract text from first few pages as sample
        sample_text = ""
        for i in range(min(3, num_pages)):
            page = pdf_reader.pages[i]
            text = page.extract_text()
            sample_text += f"\n--- Page {i+1} ---\n{text[:500]}...\n"
        
        print("\nPDF Sample Content:")
        print(sample_text)
        
        return pdf_reader, num_pages
        
    except Exception as e:
        print(f"Error reading PDF: {e}")
        return None, 0

def load_csv_from_gcs(blob, sample_rows=None):
    """Load CSV data from GCS blob"""
    print(f"\nLoading CSV: {blob.name}")
    
    try:
        # Download to memory and read with Polars
        csv_bytes = blob.download_as_bytes()
        
        if sample_rows:
            df = pl.read_csv(io.BytesIO(csv_bytes), n_rows=sample_rows)
        else:
            df = pl.read_csv(io.BytesIO(csv_bytes))
        
        print(f"  Shape: {df.shape}")
        print(f"  Columns: {len(df.columns)}")
        
        return df
        
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None

def analyze_dataset_structure(df):
    """Analyze the structure of the dataset"""
    print("\n" + "="*80)
    print("DATASET STRUCTURE ANALYSIS")
    print("="*80)
    
    print(f"\nDataset dimensions:")
    print(f"  Records: {df.shape[0]:,}")
    print(f"  Attributes: {df.shape[1]:,}")
    
    # Show first few columns
    print(f"\nFirst 10 columns:")
    for col in df.columns[:10]:
        dtype = df[col].dtype
        null_count = df[col].null_count()
        print(f"  - {col}: {dtype} (nulls: {null_count:,})")
    
    if len(df.columns) > 10:
        print(f"  ... and {len(df.columns) - 10} more columns")
    
    # Memory usage
    memory_mb = df.estimated_size() / (1024 * 1024)
    print(f"\nEstimated memory usage: {memory_mb:.2f} MB")
    
    return df.columns

def create_sample_analysis_notebook(df, output_dir='outputs'):
    """Create a Jupyter notebook with sample analysis code"""
    os.makedirs(output_dir, exist_ok=True)
    
    notebook_content = '''# FPA FOD v6 Augmented Dataset Analysis

## Dataset Overview
This notebook provides sample code for analyzing the FPA FOD v6 Augmented wildfire dataset.

```python
import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns

# Load data (example)
df = pl.read_csv('gs://fpa_fod_v6_augmented/your_file.csv')

# Basic statistics
print(df.describe())

# Example visualizations
# 1. Fire counts by year
if 'FIRE_YEAR' in df.columns:
    year_counts = df.group_by('FIRE_YEAR').count()
    plt.figure(figsize=(12, 6))
    plt.bar(year_counts['FIRE_YEAR'], year_counts['count'])
    plt.title('Fires by Year')
    plt.show()

# 2. Fire size distribution
if 'FIRE_SIZE' in df.columns:
    plt.figure(figsize=(10, 6))
    plt.hist(np.log10(df['FIRE_SIZE'].filter(df['FIRE_SIZE'] > 0)), bins=50)
    plt.title('Fire Size Distribution (log10 scale)')
    plt.show()
```

## Key Attributes to Explore
- Temporal: FIRE_YEAR, DISCOVERY_DOY, CONT_DOY
- Spatial: LATITUDE, LONGITUDE, STATE, COUNTY
- Fire characteristics: FIRE_SIZE, FIRE_SIZE_CLASS, STAT_CAUSE_DESCR
- Administrative: NWCG_REPORTING_AGENCY, SOURCE_SYSTEM_TYPE
'''
    
    with open(f'{output_dir}/fpa_fod_analysis_notebook.md', 'w') as f:
        f.write(notebook_content)
    
    print(f"\nCreated sample analysis notebook: {output_dir}/fpa_fod_analysis_notebook.md")

def main():
    """Main analysis workflow"""
    print("="*80)
    print("FPA FOD v6 AUGMENTED DATASET - GCS ACCESS TEST")
    print("="*80)
    
    # Test GCS access
    client, bucket = test_gcs_access()
    
    if bucket is None:
        print("\nUnable to access GCS bucket. Please check authentication.")
        return
    
    # Explore bucket
    csv_files, pdf_files = explore_bucket_contents(bucket)
    
    # Read PDF if available
    if pdf_files:
        pdf_blob = pdf_files[0]  # Get first PDF
        pdf_reader, num_pages = read_pdf_from_gcs(bucket, pdf_blob)
    
    # Load sample CSV data
    if csv_files:
        # Load first CSV as sample
        csv_blob = csv_files[0]
        
        # First load a small sample
        print("\n" + "="*80)
        print("LOADING DATA SAMPLE")
        print("="*80)
        
        df_sample = load_csv_from_gcs(csv_blob, sample_rows=1000)
        
        if df_sample is not None:
            # Analyze structure
            columns = analyze_dataset_structure(df_sample)
            
            # Show sample data
            print("\nSample data (first 5 rows):")
            print(df_sample.head())
            
            # Create sample analysis notebook
            create_sample_analysis_notebook(df_sample)
            
            # Save column information
            os.makedirs('outputs', exist_ok=True)
            with open('outputs/column_list.txt', 'w') as f:
                f.write(f"FPA FOD v6 Augmented Dataset - Column List\n")
                f.write(f"Total columns: {len(columns)}\n\n")
                for col in columns:
                    f.write(f"{col}\n")
            
            print(f"\nSaved column list to: outputs/column_list.txt")
            
            print("\n" + "="*80)
            print("INITIAL ANALYSIS COMPLETE")
            print("="*80)
            print("\nNext steps:")
            print("1. Review the PDF documentation for attribute definitions")
            print("2. Load full dataset for comprehensive analysis")
            print("3. Use the sample notebook code for further exploration")
        
    else:
        print("\nNo CSV files found in bucket!")

if __name__ == "__main__":
    main()