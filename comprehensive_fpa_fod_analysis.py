#!/usr/bin/env python3
"""
Comprehensive Analysis of FPA FOD v6 Augmented Wildfire Dataset
==============================================================

This script provides a complete analysis framework for the FPA FOD-Attributes dataset
stored in Google Cloud Storage at gs://fpa_fod_v6_augmented.

The dataset contains over 2.3 million wildfire records from 1992-2020 with ~270 attributes
covering physical, biological, social, and administrative factors.

Author: Data Analysis Team
Date: December 2024
"""

import polars as pl
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import gcsfs
from pathlib import Path
import warnings
import json
from typing import Dict, List, Tuple, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Set up plotting style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

class FPAFODAnalyzer:
    """
    Comprehensive analyzer for the FPA FOD v6 Augmented wildfire dataset.
    
    This class provides methods for:
    - Data loading and preprocessing
    - Executive summary generation
    - Attribute profiling
    - Data quality assessment
    - Exploratory data analysis
    - Visualization creation
    """
    
    def __init__(self, gcs_path: str = "gs://fpa_fod_v6_augmented", 
                 output_dir: str = "analysis_outputs"):
        """
        Initialize the FPA FOD Analyzer.
        
        Parameters:
        -----------
        gcs_path : str
            Google Cloud Storage path to the dataset
        output_dir : str
            Directory for saving analysis outputs
        """
        self.gcs_path = gcs_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize GCS filesystem
        try:
            self.fs = gcsfs.GCSFileSystem(token='google_default')
            logger.info("GCS filesystem initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize GCS filesystem: {e}")
            raise
        
        # Dataset attributes based on the research paper
        self.attribute_categories = {
            'temporal': ['FIRE_YEAR', 'DISCOVERY_DATE', 'DISCOVERY_DOY', 'DISCOVERY_TIME'],
            'spatial': ['LATITUDE', 'LONGITUDE', 'STATE', 'COUNTY', 'FIPS_CODE'],
            'fire_characteristics': ['FIRE_SIZE', 'FIRE_SIZE_CLASS', 'NWCG_CAUSE_CLASSIFICATION', 
                                   'NWCG_GENERAL_CAUSE'],
            'weather_climate': ['temperature_max', 'temperature_min', 'relative_humidity', 
                              'wind_velocity', 'precipitation', 'vapor_pressure_deficit'],
            'fire_danger': ['ERC', 'BI', 'FM100', 'FM1000'],
            'vegetation': ['NDVI', 'EVI', 'land_cover', 'vegetation_type', 'cheatgrass_cover'],
            'topography': ['elevation', 'slope', 'aspect', 'TPI', 'TRI'],
            'social': ['population_density', 'social_vulnerability_index', 'GDP_per_capita'],
            'infrastructure': ['distance_to_road', 'fire_stations_within_10km', 'evacuation_time'],
            'administrative': ['NPL', 'GACC_preparedness_level', 'land_ownership']
        }
        
        self.df = None
        self.metadata = {}
        
    def load_data(self, sample_size: Optional[int] = None) -> pl.DataFrame:
        """
        Load the FPA FOD dataset from Google Cloud Storage.
        
        Parameters:
        -----------
        sample_size : int, optional
            Number of rows to sample for testing (None for full dataset)
        
        Returns:
        --------
        pl.DataFrame : Loaded dataset
        """
        logger.info(f"Loading data from {self.gcs_path}")
        
        try:
            # List files in the bucket
            files = self.fs.ls(self.gcs_path)
            logger.info(f"Found {len(files)} files in bucket")
            
            # Find the main data file (likely CSV or Parquet)
            data_files = [f for f in files if f.endswith(('.csv', '.parquet'))]
            
            if not data_files:
                raise FileNotFoundError("No data files found in the bucket")
            
            # Load the main data file
            main_file = data_files[0]
            logger.info(f"Loading main data file: {main_file}")
            
            if main_file.endswith('.csv'):
                if sample_size:
                    self.df = pl.read_csv(f"gs://{main_file}", n_rows=sample_size)
                else:
                    self.df = pl.read_csv(f"gs://{main_file}")
            else:  # Parquet
                self.df = pl.read_parquet(f"gs://{main_file}")
                if sample_size:
                    self.df = self.df.head(sample_size)
            
            logger.info(f"Loaded {len(self.df)} records with {len(self.df.columns)} attributes")
            
            # Store metadata
            self.metadata['total_records'] = len(self.df)
            self.metadata['total_attributes'] = len(self.df.columns)
            self.metadata['date_range'] = (
                self.df['FIRE_YEAR'].min(),
                self.df['FIRE_YEAR'].max()
            )
            
            return self.df
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise
    
    def generate_executive_summary(self) -> Dict:
        """
        Generate an executive summary of the dataset.
        
        Returns:
        --------
        Dict : Executive summary statistics
        """
        logger.info("Generating executive summary")
        
        if self.df is None:
            raise ValueError("Data not loaded. Call load_data() first.")
        
        summary = {
            'dataset_overview': {
                'total_fires': len(self.df),
                'date_range': f"{self.df['FIRE_YEAR'].min()}-{self.df['FIRE_YEAR'].max()}",
                'total_acres_burned': self.df['FIRE_SIZE'].sum(),
                'states_covered': self.df['STATE'].n_unique(),
                'total_attributes': len(self.df.columns)
            },
            'fire_causes': self._analyze_fire_causes(),
            'temporal_trends': self._analyze_temporal_trends(),
            'geographic_distribution': self._analyze_geographic_distribution(),
            'fire_size_statistics': self._analyze_fire_sizes(),
            'key_insights': self._generate_key_insights()
        }
        
        # Save executive summary
        self._save_json(summary, 'executive_summary.json')
        self._generate_executive_summary_report(summary)
        
        return summary
    
    def profile_attributes(self) -> pl.DataFrame:
        """
        Profile all attributes in the dataset.
        
        Returns:
        --------
        pl.DataFrame : Attribute profile statistics
        """
        logger.info("Profiling dataset attributes")
        
        profiles = []
        
        for col in self.df.columns:
            profile = {
                'attribute': col,
                'data_type': str(self.df[col].dtype),
                'non_null_count': self.df[col].count(),
                'null_count': self.df[col].null_count(),
                'null_percentage': (self.df[col].null_count() / len(self.df)) * 100,
                'unique_values': self.df[col].n_unique()
            }
            
            # Add statistics for numeric columns
            if self.df[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]:
                profile.update({
                    'mean': self.df[col].mean(),
                    'median': self.df[col].median(),
                    'std': self.df[col].std(),
                    'min': self.df[col].min(),
                    'max': self.df[col].max(),
                    'q25': self.df[col].quantile(0.25),
                    'q75': self.df[col].quantile(0.75)
                })
            
            profiles.append(profile)
        
        profile_df = pl.DataFrame(profiles)
        
        # Save profile
        profile_df.write_csv(self.output_dir / 'attribute_profiles.csv')
        
        return profile_df
    
    def assess_data_quality(self) -> Dict:
        """
        Assess the quality of the dataset.
        
        Returns:
        --------
        Dict : Data quality metrics
        """
        logger.info("Assessing data quality")
        
        quality_report = {
            'completeness': self._assess_completeness(),
            'consistency': self._assess_consistency(),
            'accuracy': self._assess_accuracy(),
            'temporal_coverage': self._assess_temporal_coverage(),
            'spatial_coverage': self._assess_spatial_coverage(),
            'recommendations': self._generate_quality_recommendations()
        }
        
        # Save quality report
        self._save_json(quality_report, 'data_quality_report.json')
        
        return quality_report
    
    def perform_eda(self) -> None:
        """
        Perform comprehensive exploratory data analysis.
        """
        logger.info("Performing exploratory data analysis")
        
        # Create output subdirectory for plots
        plots_dir = self.output_dir / 'plots'
        plots_dir.mkdir(exist_ok=True)
        
        # 1. Temporal Analysis
        self._analyze_temporal_patterns(plots_dir)
        
        # 2. Spatial Analysis
        self._analyze_spatial_patterns(plots_dir)
        
        # 3. Fire Cause Analysis
        self._analyze_fire_cause_patterns(plots_dir)
        
        # 4. Environmental Conditions Analysis
        self._analyze_environmental_conditions(plots_dir)
        
        # 5. Social Vulnerability Analysis
        self._analyze_social_vulnerability(plots_dir)
        
        # 6. Fire Size Analysis
        self._analyze_fire_size_patterns(plots_dir)
        
        # 7. Correlation Analysis
        self._analyze_correlations(plots_dir)
        
        logger.info("EDA completed. Plots saved to analysis_outputs/plots/")
    
    # Helper methods for analysis
    def _analyze_fire_causes(self) -> Dict:
        """Analyze fire causes distribution."""
        cause_counts = self.df.group_by('NWCG_GENERAL_CAUSE').agg(
            pl.count().alias('count'),
            pl.sum('FIRE_SIZE').alias('total_acres')
        ).sort('count', descending=True)
        
        return {
            'top_causes': cause_counts.head(10).to_dicts(),
            'human_vs_natural': {
                'human_caused': self.df.filter(
                    pl.col('NWCG_CAUSE_CLASSIFICATION') == 'Human'
                ).shape[0],
                'natural_caused': self.df.filter(
                    pl.col('NWCG_CAUSE_CLASSIFICATION') == 'Natural'
                ).shape[0]
            }
        }
    
    def _analyze_temporal_trends(self) -> Dict:
        """Analyze temporal trends in fire occurrence."""
        yearly_stats = self.df.group_by('FIRE_YEAR').agg([
            pl.count().alias('fire_count'),
            pl.sum('FIRE_SIZE').alias('total_acres'),
            pl.mean('FIRE_SIZE').alias('avg_fire_size')
        ]).sort('FIRE_YEAR')
        
        return {
            'yearly_trends': yearly_stats.to_dicts(),
            'peak_fire_year': yearly_stats.sort('fire_count', descending=True).head(1)['FIRE_YEAR'][0],
            'peak_acres_year': yearly_stats.sort('total_acres', descending=True).head(1)['FIRE_YEAR'][0]
        }
    
    def _analyze_geographic_distribution(self) -> Dict:
        """Analyze geographic distribution of fires."""
        state_stats = self.df.group_by('STATE').agg([
            pl.count().alias('fire_count'),
            pl.sum('FIRE_SIZE').alias('total_acres'),
            pl.mean('FIRE_SIZE').alias('avg_fire_size')
        ]).sort('fire_count', descending=True)
        
        return {
            'top_states_by_count': state_stats.head(10).to_dicts(),
            'top_states_by_acres': state_stats.sort('total_acres', descending=True).head(10).to_dicts()
        }
    
    def _analyze_fire_sizes(self) -> Dict:
        """Analyze fire size distribution."""
        size_stats = {
            'mean_size': self.df['FIRE_SIZE'].mean(),
            'median_size': self.df['FIRE_SIZE'].median(),
            'max_size': self.df['FIRE_SIZE'].max(),
            'size_class_distribution': self.df.group_by('FIRE_SIZE_CLASS').count().to_dicts()
        }
        
        # Calculate percentiles
        percentiles = [0.5, 0.75, 0.9, 0.95, 0.99]
        for p in percentiles:
            size_stats[f'p{int(p*100)}'] = self.df['FIRE_SIZE'].quantile(p)
        
        return size_stats
    
    def _generate_key_insights(self) -> List[str]:
        """Generate key insights from the data."""
        insights = []
        
        # Insight 1: Fire frequency trend
        fire_counts = self.df.group_by('FIRE_YEAR').count().sort('FIRE_YEAR')
        recent_avg = fire_counts.tail(5)['count'].mean()
        early_avg = fire_counts.head(5)['count'].mean()
        
        if recent_avg > early_avg * 1.2:
            insights.append(f"Fire frequency has increased by {((recent_avg/early_avg - 1) * 100):.1f}% "
                          f"in recent years compared to the early period of the dataset.")
        
        # Insight 2: Human vs Natural causes
        human_fires = self.df.filter(pl.col('NWCG_CAUSE_CLASSIFICATION') == 'Human').shape[0]
        total_fires = len(self.df)
        human_pct = (human_fires / total_fires) * 100
        
        insights.append(f"Human activities account for {human_pct:.1f}% of all wildfires in the dataset.")
        
        # Add more insights based on the data...
        
        return insights
    
    def _save_json(self, data: Dict, filename: str) -> None:
        """Save dictionary as JSON file."""
        with open(self.output_dir / filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    def _generate_executive_summary_report(self, summary: Dict) -> None:
        """Generate a markdown executive summary report."""
        report = f"""# FPA FOD v6 Augmented Dataset - Executive Summary

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Dataset Overview

- **Total Fire Records**: {summary['dataset_overview']['total_fires']:,}
- **Time Period**: {summary['dataset_overview']['date_range']}
- **Total Acres Burned**: {summary['dataset_overview']['total_acres_burned']:,.0f}
- **States Covered**: {summary['dataset_overview']['states_covered']}
- **Total Attributes**: {summary['dataset_overview']['total_attributes']}

## Key Findings

### Fire Causes
- **Human-caused fires**: {summary['fire_causes']['human_vs_natural']['human_caused']:,} 
  ({summary['fire_causes']['human_vs_natural']['human_caused'] / summary['dataset_overview']['total_fires'] * 100:.1f}%)
- **Natural fires**: {summary['fire_causes']['human_vs_natural']['natural_caused']:,}
  ({summary['fire_causes']['human_vs_natural']['natural_caused'] / summary['dataset_overview']['total_fires'] * 100:.1f}%)

### Top Fire Causes
"""
        
        for i, cause in enumerate(summary['fire_causes']['top_causes'][:5], 1):
            report += f"{i}. **{cause['NWCG_GENERAL_CAUSE']}**: {cause['count']:,} fires "
            report += f"({cause['total_acres']:,.0f} acres)\n"
        
        report += f"""
### Temporal Trends
- **Peak fire year by count**: {summary['temporal_trends']['peak_fire_year']}
- **Peak fire year by acres**: {summary['temporal_trends']['peak_acres_year']}

### Geographic Distribution

#### Top 5 States by Fire Count:
"""
        
        for i, state in enumerate(summary['geographic_distribution']['top_states_by_count'][:5], 1):
            report += f"{i}. **{state['STATE']}**: {state['fire_count']:,} fires\n"
        
        report += "\n## Key Insights\n\n"
        
        for insight in summary['key_insights']:
            report += f"- {insight}\n"
        
        # Save report
        with open(self.output_dir / 'executive_summary.md', 'w') as f:
            f.write(report)
    
    # Additional helper methods for EDA visualizations
    def _analyze_temporal_patterns(self, plots_dir: Path) -> None:
        """Create temporal analysis plots."""
        # Annual fire count and acres burned
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        yearly_stats = self.df.group_by('FIRE_YEAR').agg([
            pl.count().alias('count'),
            pl.sum('FIRE_SIZE').alias('total_acres')
        ]).sort('FIRE_YEAR')
        
        years = yearly_stats['FIRE_YEAR'].to_list()
        counts = yearly_stats['count'].to_list()
        acres = yearly_stats['total_acres'].to_list()
        
        # Fire count plot
        ax1.plot(years, counts, 'b-', linewidth=2)
        ax1.fill_between(years, counts, alpha=0.3)
        ax1.set_title('Annual Wildfire Count (1992-2020)', fontsize=14)
        ax1.set_xlabel('Year')
        ax1.set_ylabel('Number of Fires')
        ax1.grid(True, alpha=0.3)
        
        # Acres burned plot
        ax2.plot(years, acres, 'r-', linewidth=2)
        ax2.fill_between(years, acres, alpha=0.3, color='red')
        ax2.set_title('Annual Acres Burned (1992-2020)', fontsize=14)
        ax2.set_xlabel('Year')
        ax2.set_ylabel('Acres Burned')
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(plots_dir / 'temporal_trends.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # Monthly patterns
        fig, ax = plt.subplots(figsize=(12, 6))
        
        monthly_stats = self.df.with_columns(
            pl.col('DISCOVERY_DOY').apply(lambda x: (x-1)//30 + 1).alias('month')
        ).group_by('month').count().sort('month')
        
        months = monthly_stats['month'].to_list()
        counts = monthly_stats['count'].to_list()
        
        ax.bar(months, counts, color='orange', alpha=0.7)
        ax.set_title('Fire Occurrence by Month', fontsize=14)
        ax.set_xlabel('Month')
        ax.set_ylabel('Number of Fires')
        ax.set_xticks(range(1, 13))
        ax.set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(plots_dir / 'monthly_patterns.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def _analyze_spatial_patterns(self, plots_dir: Path) -> None:
        """Create spatial analysis plots."""
        # State-level fire density map
        fig, ax = plt.subplots(figsize=(14, 8))
        
        state_stats = self.df.group_by('STATE').agg([
            pl.count().alias('count'),
            pl.mean('FIRE_SIZE').alias('avg_size')
        ]).sort('count', descending=True)
        
        top_states = state_stats.head(20)
        states = top_states['STATE'].to_list()
        counts = top_states['count'].to_list()
        
        colors = plt.cm.Reds(np.linspace(0.3, 1, len(states)))
        bars = ax.barh(states, counts, color=colors)
        
        ax.set_title('Top 20 States by Fire Count', fontsize=14)
        ax.set_xlabel('Number of Fires')
        ax.set_ylabel('State')
        ax.grid(True, alpha=0.3, axis='x')
        
        # Add value labels
        for i, (bar, count) in enumerate(zip(bars, counts)):
            ax.text(bar.get_width() + 1000, bar.get_y() + bar.get_height()/2, 
                   f'{count:,}', va='center')
        
        plt.tight_layout()
        plt.savefig(plots_dir / 'state_fire_counts.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def _analyze_fire_cause_patterns(self, plots_dir: Path) -> None:
        """Create fire cause analysis plots."""
        # Cause distribution pie chart
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
        
        # Human vs Natural
        cause_class = self.df.group_by('NWCG_CAUSE_CLASSIFICATION').count()
        ax1.pie(cause_class['count'].to_list(), 
                labels=cause_class['NWCG_CAUSE_CLASSIFICATION'].to_list(),
                autopct='%1.1f%%', startangle=90, colors=['#ff6b6b', '#4ecdc4'])
        ax1.set_title('Human vs Natural Causes', fontsize=14)
        
        # Detailed causes
        detailed_causes = self.df.group_by('NWCG_GENERAL_CAUSE').count().sort('count', descending=True).head(10)
        ax2.barh(detailed_causes['NWCG_GENERAL_CAUSE'].to_list()[::-1], 
                 detailed_causes['count'].to_list()[::-1])
        ax2.set_title('Top 10 Specific Fire Causes', fontsize=14)
        ax2.set_xlabel('Number of Fires')
        ax2.grid(True, alpha=0.3, axis='x')
        
        plt.tight_layout()
        plt.savefig(plots_dir / 'fire_causes.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def run_full_analysis(self, sample_size: Optional[int] = None) -> None:
        """
        Run the complete analysis pipeline.
        
        Parameters:
        -----------
        sample_size : int, optional
            Number of rows to sample for testing
        """
        logger.info("Starting full analysis pipeline")
        
        try:
            # 1. Load data
            self.load_data(sample_size=sample_size)
            logger.info("✓ Data loaded successfully")
            
            # 2. Generate executive summary
            summary = self.generate_executive_summary()
            logger.info("✓ Executive summary generated")
            
            # 3. Profile attributes
            profiles = self.profile_attributes()
            logger.info("✓ Attribute profiling completed")
            
            # 4. Assess data quality
            quality = self.assess_data_quality()
            logger.info("✓ Data quality assessment completed")
            
            # 5. Perform EDA
            self.perform_eda()
            logger.info("✓ Exploratory data analysis completed")
            
            logger.info(f"Analysis complete! Results saved to {self.output_dir}")
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            raise

# Additional analysis functions for specific research questions

def analyze_climate_wildfire_nexus(analyzer: FPAFODAnalyzer) -> Dict:
    """
    Analyze the relationship between climate variables and wildfire patterns.
    """
    logger.info("Analyzing climate-wildfire relationships")
    
    climate_vars = ['temperature_max', 'relative_humidity', 'wind_velocity', 
                   'vapor_pressure_deficit', 'ERC', 'BI']
    
    results = {}
    
    # Analyze correlations with fire size
    for var in climate_vars:
        if var in analyzer.df.columns:
            correlation = analyzer.df.select([
                pl.corr(var, 'FIRE_SIZE').alias(f'{var}_fire_size_corr')
            ]).to_dicts()[0]
            results[var] = correlation
    
    # Analyze extreme weather conditions
    if 'ERC' in analyzer.df.columns:
        extreme_erc = analyzer.df.filter(pl.col('ERC') > analyzer.df['ERC'].quantile(0.9))
        results['extreme_erc_stats'] = {
            'count': len(extreme_erc),
            'avg_fire_size': extreme_erc['FIRE_SIZE'].mean(),
            'total_acres': extreme_erc['FIRE_SIZE'].sum()
        }
    
    return results

def analyze_social_vulnerability_impacts(analyzer: FPAFODAnalyzer) -> Dict:
    """
    Analyze how wildfires impact socially vulnerable communities.
    """
    logger.info("Analyzing social vulnerability impacts")
    
    if 'social_vulnerability_index' not in analyzer.df.columns:
        logger.warning("Social vulnerability index not found in dataset")
        return {}
    
    # Categorize by vulnerability level
    vulnerability_analysis = analyzer.df.with_columns(
        pl.when(pl.col('social_vulnerability_index') < 0.33).then('Low')
        .when(pl.col('social_vulnerability_index') < 0.67).then('Medium')
        .otherwise('High').alias('vulnerability_level')
    ).group_by('vulnerability_level').agg([
        pl.count().alias('fire_count'),
        pl.sum('FIRE_SIZE').alias('total_acres'),
        pl.mean('population_density').alias('avg_pop_density')
    ])
    
    return vulnerability_analysis.to_dicts()

def analyze_vegetation_fire_relationships(analyzer: FPAFODAnalyzer) -> Dict:
    """
    Analyze relationships between vegetation types and fire behavior.
    """
    logger.info("Analyzing vegetation-fire relationships")
    
    results = {}
    
    # Analyze NDVI trends
    if 'NDVI' in analyzer.df.columns:
        ndvi_fire_size = analyzer.df.group_by(
            pl.col('NDVI').cut(breaks=[0, 0.2, 0.4, 0.6, 0.8, 1.0])
        ).agg([
            pl.count().alias('count'),
            pl.mean('FIRE_SIZE').alias('avg_fire_size')
        ])
        results['ndvi_fire_relationship'] = ndvi_fire_size.to_dicts()
    
    # Analyze cheatgrass impact
    if 'cheatgrass_cover' in analyzer.df.columns:
        cheatgrass_impact = analyzer.df.filter(
            pl.col('cheatgrass_cover') > 0
        ).select([
            pl.mean('FIRE_SIZE').alias('avg_size_with_cheatgrass'),
            pl.count().alias('fires_with_cheatgrass')
        ]).to_dicts()[0]
        results['cheatgrass_impact'] = cheatgrass_impact
    
    return results


def main():
    """
    Main function to run the FPA FOD analysis.
    """
    # Initialize analyzer
    analyzer = FPAFODAnalyzer()
    
    # Run full analysis
    # Note: Use sample_size parameter for testing with smaller dataset
    # analyzer.run_full_analysis(sample_size=10000)  # For testing
    analyzer.run_full_analysis()  # Full dataset
    
    # Run specialized analyses
    climate_results = analyze_climate_wildfire_nexus(analyzer)
    social_results = analyze_social_vulnerability_impacts(analyzer)
    vegetation_results = analyze_vegetation_fire_relationships(analyzer)
    
    # Save specialized analysis results
    with open(analyzer.output_dir / 'specialized_analyses.json', 'w') as f:
        json.dump({
            'climate_analysis': climate_results,
            'social_vulnerability': social_results,
            'vegetation_analysis': vegetation_results
        }, f, indent=2, default=str)
    
    logger.info("All analyses completed successfully!")


if __name__ == "__main__":
    main()