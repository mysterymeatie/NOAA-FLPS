#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build ML manifests:
- config/ml_splits.json: file paths for train/val/test Parquet years
- config/ml_schema.json: feature list and target name, inferred from a sample file

Assumptions:
- Yearly Parquet files exist in data/ml_datasets/master_table_YYYY.parquet
- Target column is 'fire_present'
- Exclude columns: time, y, x, latitude, longitude, fire_present
- Keep only numeric feature columns

Usage:
  conda run -n wf python scripts/build_ml_manifests.py
"""

from __future__ import annotations
import json
from pathlib import Path
import sys
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
PARQUET_DIR = REPO_ROOT / "data" / "ml_datasets"
CONFIG_DIR = REPO_ROOT / "config"

TRAIN_YEARS = list(range(2016, 2023))
VAL_YEARS = [2023]
TEST_YEARS = [2024, 2025]

PARQUET_TEMPLATE = "master_table_{year}.parquet"
TARGET_COL = "fire_present"
EXCLUDE_COLS = {"time", "y", "x", "latitude", "longitude", TARGET_COL}


def ensure_exists(p: Path) -> None:
    if not p.exists():
        print(f"ERROR: Missing required file: {p}", file=sys.stderr)
        sys.exit(1)


def build_paths(years: list[int]) -> list[str]:
    paths: list[str] = []
    for y in years:
        p = PARQUET_DIR / PARQUET_TEMPLATE.format(year=y)
        ensure_exists(p)
        paths.append(str(p))
    return paths


def infer_feature_list(sample_year: int | None = None) -> list[str]:
    # Choose a sample training year to infer schema (default: first TRAIN year present)
    years = TRAIN_YEARS if sample_year is None else [sample_year]
    sample_path: Path | None = None
    for y in years:
        p = PARQUET_DIR / PARQUET_TEMPLATE.format(year=y)
        if p.exists():
            sample_path = p
            break
    if sample_path is None:
        # Fallback to any VAL/TEST
        for y in VAL_YEARS + TEST_YEARS:
            p = PARQUET_DIR / PARQUET_TEMPLATE.format(year=y)
            if p.exists():
                sample_path = p
                break
    if sample_path is None:
        print("ERROR: No Parquet files found to infer schema.", file=sys.stderr)
        sys.exit(1)

    # Read minimal rows to get dtypes and columns
    df = pd.read_parquet(sample_path)
    cols = []
    for col in df.columns:
        if col in EXCLUDE_COLS:
            continue
        dt = df[col].dtype
        if pd.api.types.is_numeric_dtype(dt):
            cols.append(col)
    if not cols:
        print("ERROR: No numeric feature columns inferred.", file=sys.stderr)
        sys.exit(1)
    return sorted(cols)


def main() -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    train_paths = build_paths(TRAIN_YEARS)
    val_paths = build_paths(VAL_YEARS)
    test_paths = build_paths(TEST_YEARS)

    splits = {
        "train": train_paths,
        "val": val_paths,
        "test": test_paths,
    }
    with open(CONFIG_DIR / "ml_splits.json", "w", encoding="utf-8") as f:
        json.dump(splits, f, indent=2)

    features = infer_feature_list()
    schema = {
        "features": features,
        "target": TARGET_COL,
        "exclude_cols": sorted(list(EXCLUDE_COLS)),
    }
    with open(CONFIG_DIR / "ml_schema.json", "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)

    print("Wrote:")
    print(f"  {CONFIG_DIR / 'ml_splits.json'}")
    print(f"  {CONFIG_DIR / 'ml_schema.json'}")


if __name__ == "__main__":
    main()