# FPA FOD v6 Augmented Dataset - Sample Analysis Code

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
