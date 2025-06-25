#!/usr/bin/env python3
"""
Check GCS Access and Authentication Options
===========================================
"""

import os
import subprocess
import json

def check_environment():
    """Check environment for GCP authentication"""
    print("="*80)
    print("CHECKING GCP AUTHENTICATION ENVIRONMENT")
    print("="*80)
    
    # Check for common GCP environment variables
    env_vars = [
        'GOOGLE_APPLICATION_CREDENTIALS',
        'GOOGLE_CLOUD_PROJECT',
        'GCP_PROJECT',
        'GCLOUD_PROJECT',
        'GOOGLE_AUTH_ACCESS_TOKEN'
    ]
    
    print("\nEnvironment variables:")
    found_any = False
    for var in env_vars:
        value = os.environ.get(var)
        if value:
            print(f"  ✓ {var}: {value[:50]}...")
            found_any = True
        else:
            print(f"  ✗ {var}: Not set")
    
    # Check for gcloud
    print("\nChecking for gcloud CLI:")
    try:
        result = subprocess.run(['which', 'gcloud'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  ✓ gcloud found at: {result.stdout.strip()}")
            
            # Try to get current config
            try:
                config_result = subprocess.run(['gcloud', 'config', 'list'], 
                                             capture_output=True, text=True)
                if config_result.returncode == 0:
                    print("\n  Current gcloud configuration:")
                    for line in config_result.stdout.split('\n'):
                        if line.strip() and not line.startswith('['):
                            print(f"    {line}")
            except:
                pass
        else:
            print("  ✗ gcloud not found")
    except:
        print("  ✗ Could not check for gcloud")
    
    # Check for credentials file in common locations
    print("\nChecking for credential files:")
    common_paths = [
        os.path.expanduser('~/.config/gcloud/application_default_credentials.json'),
        os.path.expanduser('~/.config/gcloud/credentials.db'),
        '/tmp/application_default_credentials.json'
    ]
    
    for path in common_paths:
        if os.path.exists(path):
            print(f"  ✓ Found: {path}")
            found_any = True
        else:
            print(f"  ✗ Not found: {path}")
    
    return found_any

def test_bucket_access():
    """Test different access methods"""
    print("\n" + "="*80)
    print("TESTING BUCKET ACCESS METHODS")
    print("="*80)
    
    bucket_url = "gs://fpa_fod_v6_augmented"
    
    # Method 1: Try with gsutil if available
    print("\n1. Testing with gsutil (if available):")
    try:
        result = subprocess.run(['gsutil', 'ls', bucket_url], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("  ✓ Success! Bucket is accessible via gsutil")
            files = result.stdout.strip().split('\n')
            print(f"  Found {len(files)} items")
            for f in files[:5]:
                print(f"    - {f}")
            return True
        else:
            print(f"  ✗ Failed: {result.stderr}")
    except FileNotFoundError:
        print("  ✗ gsutil not found")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # Method 2: Try anonymous access
    print("\n2. Testing anonymous/public access:")
    try:
        import gcsfs
        fs = gcsfs.GCSFileSystem(token='anon')
        files = fs.ls('fpa_fod_v6_augmented')
        print("  ✓ Success! Bucket is publicly accessible")
        return True
    except Exception as e:
        print(f"  ✗ Failed: Bucket is not publicly accessible")
    
    # Method 3: Check with curl
    print("\n3. Testing with direct HTTP request:")
    storage_api_url = f"https://storage.googleapis.com/storage/v1/b/fpa_fod_v6_augmented/o"
    try:
        result = subprocess.run(['curl', '-s', storage_api_url], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            try:
                data = json.loads(result.stdout)
                if 'error' in data:
                    print(f"  ✗ API Error: {data['error'].get('message', 'Unknown error')}")
                else:
                    print("  ✓ Bucket metadata accessible")
            except:
                print(f"  ✗ Could not parse response")
        else:
            print(f"  ✗ Request failed")
    except:
        print("  ✗ curl not available")
    
    return False

def provide_instructions():
    """Provide authentication instructions"""
    print("\n" + "="*80)
    print("AUTHENTICATION SETUP INSTRUCTIONS")
    print("="*80)
    
    print("""
To access the FPA FOD v6 Augmented dataset in GCS, you need to authenticate with Google Cloud.

Here are the steps to set up authentication:

1. **Install Google Cloud SDK** (if not already installed):
   wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-latest-linux-x86_64.tar.gz
   tar -xzf google-cloud-sdk-latest-linux-x86_64.tar.gz
   ./google-cloud-sdk/install.sh
   
2. **Authenticate with your Google account**:
   gcloud auth login
   
3. **Set up Application Default Credentials**:
   gcloud auth application-default login
   
4. **Verify access to the bucket**:
   gsutil ls gs://fpa_fod_v6_augmented
   
5. **Alternative: Use a service account key**:
   - Create a service account in GCP Console
   - Download the JSON key file
   - Set environment variable:
     export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"

If you're running this in a cloud environment (e.g., Colab, Cloud Shell):
- The environment may already be authenticated
- Try running: !gcloud auth list

For this specific analysis, you mentioned you have an authenticated GCP account.
Please ensure the authentication is available in this environment.
""")

def create_mock_analysis():
    """Create a mock analysis to demonstrate expected outputs"""
    print("\n" + "="*80)
    print("CREATING MOCK ANALYSIS REPORT")
    print("="*80)
    
    os.makedirs('outputs', exist_ok=True)
    
    # Create mock executive summary
    mock_summary = """# FPA FOD v6 Augmented Wildfire Dataset - Executive Summary

## Dataset Overview

**Location**: Google Cloud Storage - `gs://fpa_fod_v6_augmented`  
**Status**: Authentication required for access

### Expected Dataset Characteristics

Based on the dataset name and description, the FPA FOD v6 Augmented dataset likely contains:

1. **Comprehensive Fire Records**: Historical wildfire occurrence data across the United States
2. **~270 Attributes**: Including physical, biological, social, and administrative variables
3. **Temporal Coverage**: Multiple decades of fire events
4. **Geographic Scope**: All U.S. states and territories

### Key Attribute Categories (Expected)

#### Ignition Information
- Statistical cause codes and descriptions
- NWCG cause classifications
- Human vs. natural ignition sources

#### Physical/Environmental Factors
- Fire size (acres)
- Fire size class
- Discovery day of year
- Containment day of year
- Weather conditions

#### Biological/Land Cover
- Vegetation types
- Land use categories
- Ecological zones

#### Social Context
- Population density
- Proximity to infrastructure
- Socioeconomic indicators

#### Administrative Metadata
- Reporting agencies
- Source systems
- Data quality indicators

### Analysis Recommendations

1. **Data Access**: Ensure proper GCP authentication before proceeding
2. **Tool Selection**: Polars is ideal for this large dataset due to:
   - Efficient memory usage
   - Fast columnar operations
   - Lazy evaluation capabilities

3. **Initial Exploration**:
   - Start with data profiling to understand distributions
   - Identify key patterns in fire causes and sizes
   - Analyze temporal trends
   - Map geographic hotspots

4. **Advanced Analysis**:
   - Climate impact assessment
   - Human-wildfire interface analysis
   - Predictive modeling opportunities
   - Risk assessment applications

### Next Steps

1. Resolve GCS authentication to access the actual data
2. Review the FPA_Supplement.pdf for detailed attribute definitions
3. Begin with exploratory data analysis
4. Develop specific research questions based on initial findings
"""
    
    with open('outputs/mock_executive_summary.md', 'w') as f:
        f.write(mock_summary)
    
    print("✓ Created mock executive summary: outputs/mock_executive_summary.md")
    
    # Create sample code
    sample_code = """# FPA FOD v6 Augmented Dataset - Sample Analysis Code

## Setup
```python
import polars as pl
import gcsfs
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize GCS filesystem
fs = gcsfs.GCSFileSystem(token='google_default')

# Read data
df = pl.read_csv('gs://fpa_fod_v6_augmented/fires.csv')
```

## Basic Analysis
```python
# Dataset overview
print(f"Dataset shape: {df.shape}")
print(f"Columns: {df.columns}")

# Temporal analysis
if 'FIRE_YEAR' in df.columns:
    year_stats = df.group_by('FIRE_YEAR').agg([
        pl.count('FIRE_YEAR').alias('count'),
        pl.mean('FIRE_SIZE').alias('avg_size'),
        pl.sum('FIRE_SIZE').alias('total_burned')
    ]).sort('FIRE_YEAR')

# Geographic analysis
if 'STATE' in df.columns:
    state_summary = df.group_by('STATE').agg([
        pl.count('STATE').alias('fire_count'),
        pl.sum('FIRE_SIZE').alias('total_acres')
    ]).sort('fire_count', descending=True)

# Cause analysis
if 'STAT_CAUSE_DESCR' in df.columns:
    cause_summary = df.group_by('STAT_CAUSE_DESCR').count()
```

## Visualization Examples
```python
# Fire trends over time
plt.figure(figsize=(12, 6))
year_counts = df.group_by('FIRE_YEAR').count().sort('FIRE_YEAR')
plt.plot(year_counts['FIRE_YEAR'], year_counts['count'])
plt.title('Wildfire Occurrences by Year')
plt.xlabel('Year')
plt.ylabel('Number of Fires')
plt.grid(True, alpha=0.3)
plt.show()

# Fire size distribution
plt.figure(figsize=(10, 6))
fire_sizes = df['FIRE_SIZE'].filter(df['FIRE_SIZE'] > 0)
plt.hist(np.log10(fire_sizes), bins=50, edgecolor='black', alpha=0.7)
plt.title('Fire Size Distribution (log10 scale)')
plt.xlabel('log10(Acres)')
plt.ylabel('Frequency')
plt.show()

# Top causes
cause_counts = df.group_by('STAT_CAUSE_DESCR').count().sort('count', descending=True).head(10)
plt.figure(figsize=(10, 6))
plt.barh(cause_counts['STAT_CAUSE_DESCR'], cause_counts['count'])
plt.title('Top 10 Fire Causes')
plt.xlabel('Number of Fires')
plt.tight_layout()
plt.show()
```
"""
    
    with open('outputs/sample_analysis_code.py', 'w') as f:
        f.write(sample_code)
    
    print("✓ Created sample analysis code: outputs/sample_analysis_code.py")

def main():
    """Main workflow"""
    print("FPA FOD v6 AUGMENTED DATASET - ACCESS CHECK")
    print("="*80)
    
    # Check environment
    has_auth = check_environment()
    
    # Test bucket access
    accessible = test_bucket_access()
    
    if not accessible:
        # Provide instructions
        provide_instructions()
        
        # Create mock analysis
        create_mock_analysis()
        
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print("""
The FPA FOD v6 Augmented dataset requires Google Cloud authentication to access.
Since authentication is not currently available in this environment, I've created:

1. Mock executive summary with expected dataset characteristics
2. Sample analysis code for when access is available
3. Authentication setup instructions

To proceed with the actual analysis:
1. Set up GCP authentication as described above
2. Verify access to gs://fpa_fod_v6_augmented
3. Run the full analysis scripts

The dataset promises to be a rich resource for wildfire analysis with its
270 attributes covering physical, biological, social, and administrative aspects.
""")

if __name__ == "__main__":
    main()