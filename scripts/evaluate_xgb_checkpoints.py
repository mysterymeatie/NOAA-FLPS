#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Evaluate XGBoost checkpoint artifacts (xgb_partial_round_*.joblib) on a chosen split
and report PR-AUC for each, highlighting the best.

Usage:
  python -u scripts/evaluate_xgb_checkpoints.py \
    --checkpoints_dir models/baseline \
    --pattern xgb_partial_round_*.joblib \
    --split val
"""

from __future__ import annotations
import argparse
import json
import re
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import joblib
from sklearn.metrics import average_precision_score


def load_manifest(splits_path: Path, schema_path: Path) -> Tuple[dict, dict]:
    with open(splits_path, "r", encoding="utf-8") as f:
        splits = json.load(f)
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    return splits, schema


def load_features(paths: List[str], features: List[str], target: str, downcast_float32: bool = True) -> Tuple[pd.DataFrame, pd.Series]:
    use_cols = list(set(features + [target]))
    frames: List[pd.DataFrame] = []
    for p in paths:
        frames.append(pd.read_parquet(p, columns=use_cols))
    df = pd.concat(frames, axis=0, ignore_index=True)
    X = df.loc[:, features].copy()
    y = df[target].astype(np.uint8)
    if downcast_float32:
        for c in X.columns:
            if np.issubdtype(X[c].dtype, np.floating):
                X.loc[:, c] = X[c].astype(np.float32)
            elif np.issubdtype(X[c].dtype, np.integer):
                X.loc[:, c] = X[c].astype(np.int32)
    return X, y


def extract_round_num(path: Path) -> int:
    m = re.search(r"round_(\d+)", path.name)
    return int(m.group(1)) if m else -1


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate PR-AUC of XGBoost checkpoints on a split")
    parser.add_argument("--splits", default=str(Path("config") / "ml_splits.json"))
    parser.add_argument("--schema", default=str(Path("config") / "ml_schema.json"))
    parser.add_argument("--checkpoints_dir", default=str(Path("models") / "baseline"))
    parser.add_argument("--pattern", default="xgb_partial_round_*.joblib")
    parser.add_argument("--split", default="val", choices=["train", "val", "test"])
    parser.add_argument("--topk", type=int, default=10)
    args = parser.parse_args()

    splits_path = Path(args.splits)
    schema_path = Path(args.schema)
    ckpt_dir = Path(args.checkpoints_dir)

    splits, schema = load_manifest(splits_path, schema_path)
    features: List[str] = schema["features"]
    target: str = schema["target"]

    # Load data for chosen split
    X, y = load_features(splits[args.split], features, target)
    print(f"Loaded {len(y):,} rows for split='{args.split}' with {X.shape[1]} features")

    # Enumerate checkpoints
    paths = sorted(ckpt_dir.glob(args.pattern), key=extract_round_num)
    if not paths:
        raise FileNotFoundError(f"No checkpoints found in {ckpt_dir} matching {args.pattern}")
    print(f"Found {len(paths)} checkpoints")

    results = []
    for p in paths:
        try:
            bundle = joblib.load(p)
            model = bundle["xgb"]
            proba = model.predict_proba(X)[:, 1]
            ap = average_precision_score(y, proba)
            rnd = extract_round_num(p)
            results.append((rnd, ap, p.name))
            print(f"{p.name}: PR-AUC={ap:.6f}")
        except Exception as e:
            print(f"[warn] Failed to evaluate {p.name}: {e}")

    # Sort by metric
    results.sort(key=lambda t: t[1], reverse=True)
    print("\nTop checkpoints by PR-AUC:")
    for rnd, ap, name in results[: max(1, args.topk)]:
        print(f"round={rnd:>5} | PR-AUC={ap:.6f} | {name}")

    if results:
        best_rnd, best_ap, best_name = results[0]
        print(f"\nBest: round={best_rnd} | PR-AUC={best_ap:.6f} | file={best_name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

