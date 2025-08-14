#!/usr/bin/env python3
"""
Fire Risk Prediction Generator
Phase 1 Implementation - Generate 0-1 Risk Scores

Generates calibrated fire risk predictions using trained Random Forest model.
"""

import argparse
import os
import sys
import numpy as np
import pandas as pd
import joblib
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def load_model(model_file):
    """Load trained models."""
    print(f"Loading models from {model_file}...")
    models = joblib.load(model_file)
    return models

def generate_predictions(data_file, models, output_file):
    """Generate fire risk predictions."""
    print(f"Loading prediction data from {data_file}...")
    
    df = pd.read_parquet(data_file)
    print(f"Loaded {len(df):,} samples")
    
    # Prepare features (same as training)
    feature_cols = models['feature_cols']
    X = df[feature_cols].fillna(df[feature_cols].median())
    
    print(f"Using {len(feature_cols)} features for prediction")
    
    # Generate risk predictions using calibrated model
    print("Generating calibrated fire risk scores...")
    calibrated_rf = models['calibrated_rf']
    risk_scores = calibrated_rf.predict_proba(X)[:, 1]
    
    print(f"Risk score range: {risk_scores.min():.6f} to {risk_scores.max():.6f}")
    print(f"Risk score statistics:")
    print(f"  Mean: {risk_scores.mean():.6f}")
    print(f"  Median: {np.median(risk_scores):.6f}")
    print(f"  95th percentile: {np.percentile(risk_scores, 95):.6f}")
    print(f"  99th percentile: {np.percentile(risk_scores, 99):.6f}")
    
    # Add predictions to dataframe
    result_df = df[['time', 'y', 'x', 'latitude', 'longitude', 'fire_present']].copy()
    result_df['fire_risk_score'] = risk_scores
    
    # Create risk categories
    result_df['risk_category'] = pd.cut(
        risk_scores,
        bins=[0, 0.1, 0.3, 0.6, 1.0],
        labels=['Low', 'Moderate', 'High', 'Extreme'],
        include_lowest=True
    )
    
    # Save predictions
    result_df.to_parquet(output_file, compression='snappy')
    file_size = os.path.getsize(output_file) / 1024 / 1024
    print(f"Saved predictions to: {output_file} ({file_size:.1f} MB)")
    
    # Generate summary statistics
    print(f"\nRisk Category Distribution:")
    risk_dist = result_df['risk_category'].value_counts().sort_index()
    for category, count in risk_dist.items():
        pct = count / len(result_df) * 100
        print(f"  {category}: {count:,} ({pct:.2f}%)")
    
    # Validation against actual fires
    if 'fire_present' in result_df.columns:
        actual_fires = result_df['fire_present'] == 1
        if actual_fires.any():
            print(f"\nFire Risk Validation:")
            print(f"  Samples with actual fires: {actual_fires.sum()}")
            fire_risk_scores = risk_scores[actual_fires]
            print(f"  Mean risk score for fire locations: {fire_risk_scores.mean():.6f}")
            print(f"  Median risk score for fire locations: {np.median(fire_risk_scores):.6f}")
            
            # Risk category for actual fires
            fire_categories = result_df[actual_fires]['risk_category'].value_counts()
            print(f"  Fire locations by risk category:")
            for category, count in fire_categories.items():
                pct = count / actual_fires.sum() * 100
                print(f"    {category}: {count} ({pct:.1f}%)")
    
    return result_df

def create_risk_summary(predictions_df, output_dir):
    """Create summary visualizations and statistics."""
    print("\nCreating risk summary...")
    
    import matplotlib.pyplot as plt
    import seaborn as sns
    
    # Risk score distribution
    plt.figure(figsize=(15, 10))
    
    # Risk score histogram
    plt.subplot(2, 3, 1)
    plt.hist(predictions_df['fire_risk_score'], bins=50, alpha=0.7, edgecolor='black')
    plt.xlabel('Fire Risk Score')
    plt.ylabel('Frequency')
    plt.title('Distribution of Fire Risk Scores')
    plt.grid(True, alpha=0.3)
    
    # Log-scale histogram for better visualization
    plt.subplot(2, 3, 2)
    plt.hist(predictions_df['fire_risk_score'], bins=50, alpha=0.7, edgecolor='black')
    plt.xlabel('Fire Risk Score')
    plt.ylabel('Frequency')
    plt.title('Fire Risk Scores (Log Scale)')
    plt.yscale('log')
    plt.grid(True, alpha=0.3)
    
    # Risk categories pie chart
    plt.subplot(2, 3, 3)
    risk_counts = predictions_df['risk_category'].value_counts()
    plt.pie(risk_counts.values, labels=risk_counts.index, autopct='%1.1f%%')
    plt.title('Risk Category Distribution')
    
    # Box plot of risk scores by category
    plt.subplot(2, 3, 4)
    sns.boxplot(data=predictions_df, x='risk_category', y='fire_risk_score')
    plt.title('Risk Scores by Category')
    plt.xticks(rotation=45)
    
    # Time series of mean risk (if time available)
    if 'time' in predictions_df.columns:
        plt.subplot(2, 3, 5)
        predictions_df['time'] = pd.to_datetime(predictions_df['time'])
        daily_risk = predictions_df.groupby(predictions_df['time'].dt.date)['fire_risk_score'].mean()
        plt.plot(daily_risk.index, daily_risk.values, alpha=0.7)
        plt.xlabel('Date')
        plt.ylabel('Mean Risk Score')
        plt.title('Daily Mean Risk Score')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
    
    # Actual vs predicted (if fire_present available)
    if 'fire_present' in predictions_df.columns:
        plt.subplot(2, 3, 6)
        fire_mask = predictions_df['fire_present'] == 1
        no_fire_mask = predictions_df['fire_present'] == 0
        
        plt.hist(predictions_df[no_fire_mask]['fire_risk_score'], 
                bins=30, alpha=0.5, label='No Fire', density=True)
        plt.hist(predictions_df[fire_mask]['fire_risk_score'], 
                bins=30, alpha=0.7, label='Fire', density=True)
        plt.xlabel('Risk Score')
        plt.ylabel('Density')
        plt.title('Risk Score Distribution by Fire Occurrence')
        plt.legend()
        plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    summary_plot = os.path.join(output_dir, 'fire_risk_summary.png')
    plt.savefig(summary_plot, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"Risk summary plots saved to: {summary_plot}")
    
    # Create CSV summary
    summary_stats = {
        'total_predictions': len(predictions_df),
        'mean_risk_score': predictions_df['fire_risk_score'].mean(),
        'median_risk_score': predictions_df['fire_risk_score'].median(),
        'max_risk_score': predictions_df['fire_risk_score'].max(),
        'high_risk_count': (predictions_df['risk_category'].isin(['High', 'Extreme'])).sum(),
        'high_risk_percentage': (predictions_df['risk_category'].isin(['High', 'Extreme'])).mean() * 100
    }
    
    if 'fire_present' in predictions_df.columns:
        actual_fires = predictions_df['fire_present'] == 1
        if actual_fires.any():
            summary_stats.update({
                'actual_fires': actual_fires.sum(),
                'mean_risk_for_fires': predictions_df[actual_fires]['fire_risk_score'].mean(),
                'fires_in_high_risk': ((predictions_df['fire_present'] == 1) & 
                                     (predictions_df['risk_category'].isin(['High', 'Extreme']))).sum()
            })
    
    summary_file = os.path.join(output_dir, 'risk_summary.csv')
    pd.DataFrame([summary_stats]).to_csv(summary_file, index=False)
    print(f"Summary statistics saved to: {summary_file}")

def main():
    parser = argparse.ArgumentParser(description='Generate Fire Risk Predictions')
    parser.add_argument('--model', required=True, help='Trained model file (.pkl)')
    parser.add_argument('--data', required=True, help='Input data file (parquet)')
    parser.add_argument('--output', required=True, help='Output directory')
    parser.add_argument('--summary', action='store_true', help='Create summary visualizations')
    
    args = parser.parse_args()
    
    print("=== Fire Risk Prediction Generator ===")
    print(f"Model: {args.model}")
    print(f"Data: {args.data}")
    print(f"Output: {args.output}")
    
    os.makedirs(args.output, exist_ok=True)
    
    # Load model
    models = load_model(args.model)
    
    # Generate predictions
    output_file = os.path.join(args.output, 'fire_risk_predictions.parquet')
    predictions_df = generate_predictions(args.data, models, output_file)
    
    # Create summary if requested
    if args.summary:
        create_risk_summary(predictions_df, args.output)
    
    print(f"\n=== Prediction Complete ===")
    print(f"Generated {len(predictions_df):,} fire risk predictions")
    print(f"Risk scores range from 0 to 1 (1 = highest fire risk)")
    print(f"Results saved to: {args.output}")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())