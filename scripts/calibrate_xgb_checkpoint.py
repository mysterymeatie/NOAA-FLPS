#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Calibrate a saved XGBoost checkpoint (xgb_partial_round_*.joblib) on 2023 (val)
and evaluate on test (2024–2025). Saves a calibrated artifact and plots.

Usage:
  python -u scripts/calibrate_xgb_checkpoint.py \
    --checkpoint models/baseline/xgb_partial_round_200.joblib \
    --output models/baseline
"""

from __future__ import annotations
import argparse
import json
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import joblib
from sklearn.metrics import (
    average_precision_score,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
    confusion_matrix,
    classification_report,
)
from sklearn.calibration import CalibratedClassifierCV
from sklearn.isotonic import IsotonicRegression
import matplotlib.pyplot as plt
import seaborn as sns


def load_manifest(splits_path: Path, schema_path: Path) -> Tuple[dict, dict]:
    with open(splits_path, "r", encoding="utf-8") as f:
        splits = json.load(f)
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    return splits, schema


def load_features(paths: List[str], features: List[str], target: str) -> Tuple[pd.DataFrame, pd.Series]:
    use_cols = list(set(features + [target]))
    frames: List[pd.DataFrame] = []
    for p in paths:
        frames.append(pd.read_parquet(p, columns=use_cols))
    df = pd.concat(frames, axis=0, ignore_index=True)
    X = df.loc[:, features].copy()
    y = df[target].astype(np.uint8)
    # Downcast for memory
    for c in X.columns:
        if np.issubdtype(X[c].dtype, np.floating):
            X.loc[:, c] = X[c].astype(np.float32)
        elif np.issubdtype(X[c].dtype, np.integer):
            X.loc[:, c] = X[c].astype(np.int32)
    return X, y


class IsoWrap:
    """Top-level, picklable isotonic calibration wrapper around a base classifier.

    Exposes a predict_proba API to mimic a calibrated classifier.
    """
    def __init__(self, base) -> None:
        self.base = base
        self.iso = IsotonicRegression(out_of_bounds="clip")

    def fit(self, X: pd.DataFrame, y: pd.Series) -> "IsoWrap":
        p = self.base.predict_proba(X)[:, 1]
        self.iso.fit(p, y.values)
        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        p = self.base.predict_proba(X)[:, 1]
        pc = self.iso.transform(p)
        pc = np.clip(pc, 0.0, 1.0).astype(np.float32)
        return np.column_stack([1.0 - pc, pc])


def evaluate_and_plot(y_true: np.ndarray, y_proba: np.ndarray, out_dir: Path, name: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    pr_auc = average_precision_score(y_true, y_proba)
    roc_auc = roc_auc_score(y_true, y_proba)
    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    f1_scores = 2 * (precision * recall) / (precision + recall + 1e-8)
    optimal_idx = int(np.argmax(f1_scores))
    optimal_threshold = thresholds[max(0, min(optimal_idx, len(thresholds) - 1))] if len(thresholds) else 0.5
    y_pred_opt = (y_proba >= optimal_threshold).astype(int)
    cm = confusion_matrix(y_true, y_pred_opt)

    plt.figure(figsize=(12, 4))
    plt.subplot(1, 3, 1)
    plt.plot(recall, precision, label=f'PR AUC={pr_auc:.3f}')
    plt.xlabel('Recall'); plt.ylabel('Precision'); plt.title('Precision-Recall')
    plt.legend(); plt.grid(True)
    plt.subplot(1, 3, 2)
    fpr, tpr, _ = roc_curve(y_true, y_proba)
    plt.plot(fpr, tpr, label=f'ROC AUC={roc_auc:.3f}')
    plt.plot([0, 1], [0, 1], 'k--', alpha=0.4)
    plt.xlabel('FPR'); plt.ylabel('TPR'); plt.title('ROC'); plt.legend(); plt.grid(True)
    plt.subplot(1, 3, 3)
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
    plt.title('Confusion Matrix'); plt.ylabel('Actual'); plt.xlabel('Predicted')
    plt.tight_layout()
    plt.savefig(out_dir / f"{name}_evaluation.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"PR-AUC={pr_auc:.6f}; ROC-AUC={roc_auc:.6f}; optimal_threshold≈{optimal_threshold:.6f}")
    print("Classification report (optimal threshold):\n" + classification_report(y_true, y_pred_opt))


def main() -> int:
    parser = argparse.ArgumentParser(description="Calibrate and evaluate an XGB checkpoint")
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--splits", default=str(Path("config") / "ml_splits.json"))
    parser.add_argument("--schema", default=str(Path("config") / "ml_schema.json"))
    parser.add_argument("--output", default=str(Path("models") / "baseline"))
    args = parser.parse_args()

    splits, schema = load_manifest(Path(args.splits), Path(args.schema))
    features: List[str] = schema["features"]
    target: str = schema["target"]

    bundle = joblib.load(args.checkpoint)
    model = bundle["xgb"]

    # Load val and test
    X_val, y_val = load_features(splits["val"], features, target)
    X_test, y_test = load_features(splits["test"], features, target)

    # Calibrate on val with robust fallback
    try:
        calibrator = CalibratedClassifierCV(model, method="isotonic", cv="prefit")
        calibrator.fit(X_val, y_val)
    except Exception as e:
        print(f"[calibration] CalibratedClassifierCV failed ({e}); falling back to IsotonicRegression wrapper")
        calibrator = IsoWrap(model).fit(X_val, y_val)

    # Evaluate and save
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    y_proba_test = calibrator.predict_proba(X_test)[:, 1]
    evaluate_and_plot(y_test.values, y_proba_test, out_dir=out_dir, name="xgb_checkpoint")

    out_path = out_dir / "xgb_checkpoint_calibrated.joblib"
    joblib.dump({
        "xgb": model,
        "calibrator": calibrator,
        "features": features,
        "target": target,
        "splits": splits,
    }, out_path)
    print(f"Saved calibrated checkpoint to {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

