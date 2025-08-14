#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Train XGBoost for Wildfire Risk Prediction (wired to manifests)

- Reads split and schema manifests from config/
- Loads train/val/test Parquet files
- Trains XGBClassifier with class imbalance handling and early stopping (on 2023 val)
- Calibrates probabilities (isotonic) on validation
- Evaluates on test; saves artifacts and plots
"""

from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys
from typing import List, Tuple
import threading
import time
from datetime import datetime

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import (
    average_precision_score,
    precision_recall_curve,
    roc_auc_score,
    classification_report,
    confusion_matrix,
    roc_curve,
)
from sklearn.calibration import CalibratedClassifierCV
from sklearn.isotonic import IsotonicRegression
import joblib
import matplotlib.pyplot as plt
import seaborn as sns


def load_manifest(splits_path: Path, schema_path: Path) -> Tuple[dict, dict]:
    with open(splits_path, "r", encoding="utf-8") as f:
        splits = json.load(f)
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    return splits, schema


essential_cols = ["time", "y", "x", "latitude", "longitude"]


def _try_get_memory_mb() -> float | None:
    try:
        import psutil  # type: ignore
        process = psutil.Process()
        return process.memory_info().rss / (1024 * 1024)
    except Exception:
        return None


class HeartbeatReporter:
    def __init__(self, interval_seconds: int = 180) -> None:
        self.interval_seconds = max(30, int(interval_seconds))
        self._status: str = "starting"
        self._start_time = datetime.utcnow()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def update(self, status: str) -> None:
        self._status = status

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _run(self) -> None:
        time.sleep(min(60, self.interval_seconds))
        while not self._stop_event.is_set():
            elapsed = datetime.utcnow() - self._start_time
            mem_mb = _try_get_memory_mb()
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            elapsed_str = str(elapsed).split(".")[0]
            if mem_mb is None:
                print(f"[heartbeat] {timestamp} | elapsed={elapsed_str} | status={self._status}")
            else:
                print(f"[heartbeat] {timestamp} | elapsed={elapsed_str} | rss_mem={mem_mb:.0f} MB | status={self._status}")
            self._stop_event.wait(self.interval_seconds)


def load_parquet_paths(
    paths: List[str],
    features: List[str],
    target: str,
    downcast_float32: bool = False,
    limit_rows: int | None = None,
    log_prefix: str = "",
) -> Tuple[pd.DataFrame, pd.Series]:
    use_cols = list(set(features + essential_cols + [target]))
    frames: List[pd.DataFrame] = []
    total_files = len(paths)
    print(f"{log_prefix}Loading {total_files} Parquet file(s) with columns={len(use_cols)}...")
    for idx, parquet_path in enumerate(paths, start=1):
        print(f"{log_prefix}[{idx}/{total_files}] {parquet_path}")
        df = pd.read_parquet(parquet_path, columns=use_cols)
        frames.append(df)
    df_all = pd.concat(frames, axis=0, ignore_index=True)
    if limit_rows is not None and limit_rows > 0 and len(df_all) > limit_rows:
        df_all = df_all.sample(n=limit_rows, random_state=42)
        print(f"{log_prefix}Applied row limit: {limit_rows:,} rows")
    X = df_all.loc[:, features].copy()
    y = df_all[target].astype(np.uint8)
    if downcast_float32:
        for c in X.columns:
            if np.issubdtype(X[c].dtype, np.floating):
                X.loc[:, c] = X[c].astype(np.float32)
            elif np.issubdtype(X[c].dtype, np.integer):
                X.loc[:, c] = X[c].astype(np.int32)
        print(f"{log_prefix}Downcasted features to float32/int32 where applicable")
    print(f"{log_prefix}Loaded rows: {len(df_all):,}; features shape: {X.shape}")
    mem_mb = (X.memory_usage(deep=True).sum() + y.memory_usage(deep=True)) / (1024 * 1024)
    print(f"{log_prefix}Approx memory for X+y: {mem_mb:.1f} MB")
    return X, y


def evaluate_and_plot(y_true: np.ndarray, y_proba: np.ndarray, model_name: str, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    pr_auc = average_precision_score(y_true, y_proba)
    roc_auc = roc_auc_score(y_true, y_proba)

    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    f1_scores = 2 * (precision * recall) / (precision + recall + 1e-8)
    optimal_idx = int(np.argmax(f1_scores))
    optimal_threshold = thresholds[max(0, min(optimal_idx, len(thresholds) - 1))]

    print(f"PR-AUC: {pr_auc:.4f}")
    print(f"ROC-AUC: {roc_auc:.4f}")
    print(f"Optimal threshold (by F1 on PR curve): {optimal_threshold:.4f}")

    y_pred_opt = (y_proba >= optimal_threshold).astype(int)
    print("\nClassification Report (Optimal Threshold):")
    print(classification_report(y_true, y_pred_opt))

    cm = confusion_matrix(y_true, y_pred_opt)
    print("Confusion Matrix:\n", cm)

    # Plots
    plt.figure(figsize=(12, 4))

    # PR curve
    plt.subplot(1, 3, 1)
    plt.plot(recall, precision, label=f'PR AUC={pr_auc:.3f}')
    plt.xlabel('Recall'); plt.ylabel('Precision'); plt.title('Precision-Recall')
    plt.legend(); plt.grid(True)

    # ROC curve
    plt.subplot(1, 3, 2)
    fpr, tpr, _ = roc_curve(y_true, y_proba)
    plt.plot(fpr, tpr, label=f'ROC AUC={roc_auc:.3f}')
    plt.plot([0, 1], [0, 1], 'k--', alpha=0.4)
    plt.xlabel('FPR'); plt.ylabel('TPR'); plt.title('ROC'); plt.legend(); plt.grid(True)

    # Confusion matrix
    plt.subplot(1, 3, 3)
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
    plt.title('Confusion Matrix'); plt.ylabel('Actual'); plt.xlabel('Predicted')

    plt.tight_layout()
    plt.savefig(out_dir / f"{model_name}_evaluation.png", dpi=150, bbox_inches="tight")
    plt.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Train XGBoost using manifests")
    parser.add_argument("--splits", default=str(Path("config") / "ml_splits.json"))
    parser.add_argument("--schema", default=str(Path("config") / "ml_schema.json"))
    parser.add_argument("--output", default=str(Path("models") / "baseline"))
    parser.add_argument("--downcast_float32", action="store_true", help="Downcast features to float32/int32 to reduce memory")
    parser.add_argument("--limit_rows", type=int, default=0, help="If >0, randomly sample this many rows from each split")
    parser.add_argument("--tree_method", default="hist", choices=["hist", "approx", "gpu_hist"], help="XGBoost tree method")
    parser.add_argument("--n_jobs", type=int, default=-1, help="Parallel threads for XGBoost (-1 uses all cores)")
    parser.add_argument("--n_estimators", type=int, default=2000, help="Total boosting rounds")
    parser.add_argument("--checkpoint_interval", type=int, default=100, help="Save partial artifacts every N rounds (<= n_estimators)")
    parser.add_argument("--checkpoint_prefix", default="xgb_partial", help="Filename prefix for partial artifacts")
    parser.add_argument("--heartbeat_sec", type=int, default=180, help="Print a heartbeat status every N seconds (>=30)")
    parser.add_argument("--no_heartbeat", action="store_true", help="Disable heartbeat logging")
    args = parser.parse_args()

    splits_path = Path(args.splits)
    schema_path = Path(args.schema)
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    reporter = HeartbeatReporter(interval_seconds=args.heartbeat_sec)
    if not args.no_heartbeat:
        reporter.start()

    print("Loading manifests...")
    splits, schema = load_manifest(splits_path, schema_path)
    features: List[str] = schema["features"]
    target: str = schema["target"]

    reporter.update("loading train")
    print("Loading train set...")
    X_train, y_train = load_parquet_paths(
        splits["train"],
        features,
        target,
        downcast_float32=args.downcast_float32,
        limit_rows=(args.limit_rows if args.limit_rows > 0 else None),
        log_prefix="[train] ",
    )

    reporter.update("loading validation")
    print("Loading validation set...")
    X_val, y_val = load_parquet_paths(
        splits["val"],
        features,
        target,
        downcast_float32=args.downcast_float32,
        limit_rows=(args.limit_rows if args.limit_rows > 0 else None),
        log_prefix="[val] ",
    )

    reporter.update("loading test")
    print("Loading test set...")
    X_test, y_test = load_parquet_paths(
        splits["test"],
        features,
        target,
        downcast_float32=args.downcast_float32,
        limit_rows=(args.limit_rows if args.limit_rows > 0 else None),
        log_prefix="[test] ",
    )

    print(f"Train samples: {len(y_train):,}; fire rate={y_train.mean():.5f}")
    print(f"Val samples:   {len(y_val):,}; fire rate={y_val.mean():.5f}")
    print(f"Test samples:  {len(y_test):,}; fire rate={y_test.mean():.5f}")

    # Compute class weight for XGB
    neg = (y_train == 0).sum()
    pos = (y_train == 1).sum()
    scale_pos_weight = float(neg) / max(1.0, float(pos))
    print(f"scale_pos_weight={scale_pos_weight:.2f}")

    reporter.update("training xgboost")
    print("\nTraining XGBoost with periodic checkpoints...")
    total_rounds = max(1, int(args.n_estimators))
    interval = max(1, min(int(args.checkpoint_interval), total_rounds))
    print(f"Total rounds={total_rounds}, checkpoint_interval={interval}")

    model = xgb.XGBClassifier(
        objective="binary:logistic",
        n_estimators=interval,  # will increase incrementally
        max_depth=7,
        learning_rate=0.04,
        subsample=0.7,
        colsample_bytree=0.7,
        min_child_weight=6,
        reg_alpha=0.1,
        reg_lambda=1.0,
        tree_method=args.tree_method,
        n_jobs=args.n_jobs,
        random_state=42,
        scale_pos_weight=scale_pos_weight,
        eval_metric="aucpr",
    )

    current_rounds = 0
    while current_rounds < total_rounds:
        next_rounds = min(current_rounds + interval, total_rounds)
        model.set_params(n_estimators=next_rounds)
        reporter.update(f"training rounds {current_rounds}->{next_rounds}")
        print(f"\n[train] Fitting rounds {current_rounds} -> {next_rounds} ...")
        if current_rounds == 0:
            model.fit(
                X_train,
                y_train,
                eval_set=[(X_val, y_val)],
                verbose=True,
            )
        else:
            booster = model.get_booster()
            model.fit(
                X_train,
                y_train,
                eval_set=[(X_val, y_val)],
                verbose=True,
                xgb_model=booster,
            )

        current_rounds = next_rounds

        # Eager checkpoint
        ckpt_path = out_dir / f"{args.checkpoint_prefix}_round_{current_rounds}.joblib"
        joblib.dump({
            "xgb": model,
            "features": features,
            "target": target,
            "splits": splits,
            "scale_pos_weight": scale_pos_weight,
        }, ckpt_path)
        print(f"[checkpoint] Saved partial model to {ckpt_path}")

    # Calibrate on validation (prefit)
    reporter.update("calibrating (isotonic) on validation")
    print("Calibrating on validation (isotonic)...")
    class IsotonicCalibratorWrapper:
        def __init__(self, base_model: xgb.XGBClassifier) -> None:
            self.base_model = base_model
            self.iso = IsotonicRegression(out_of_bounds="clip")

        def fit(self, Xc: pd.DataFrame, yc: pd.Series) -> "IsotonicCalibratorWrapper":
            p = self.base_model.predict_proba(Xc)[:, 1]
            self.iso.fit(p, yc.values)
            return self

        def predict_proba(self, Xn: pd.DataFrame) -> np.ndarray:
            p = self.base_model.predict_proba(Xn)[:, 1]
            p_cal = self.iso.transform(p)
            p_cal = np.clip(p_cal, 0.0, 1.0).astype(np.float32)
            return np.column_stack([1.0 - p_cal, p_cal])

    try:
        calibrator = CalibratedClassifierCV(model, method="isotonic", cv="prefit")
        calibrator.fit(X_val, y_val)
    except Exception as e:
        print(f"[calibration] CalibratedClassifierCV(prefit) failed ({e.__class__.__name__}: {e}). Falling back to IsotonicRegression wrapper.")
        calibrator = IsotonicCalibratorWrapper(model).fit(X_val, y_val)

    # Save a pre-calibration checkpoint of the final model
    final_ckpt = out_dir / "xgb_trained_uncalibrated.joblib"
    joblib.dump({
        "xgb": model,
        "features": features,
        "target": target,
        "splits": splits,
        "scale_pos_weight": scale_pos_weight,
    }, final_ckpt)
    print(f"Saved uncalibrated trained model to {final_ckpt}")

    # Evaluate on test
    reporter.update("evaluating on test")
    y_proba_test = calibrator.predict_proba(X_test)[:, 1]
    evaluate_and_plot(y_test.values, y_proba_test, model_name="xgb", out_dir=out_dir)

    # Persist artifacts
    reporter.update("saving artifacts")
    joblib.dump({
        "xgb": model,
        "calibrator": calibrator,
        "features": features,
        "target": target,
        "splits": splits,
        "scale_pos_weight": scale_pos_weight,
    }, out_dir / "xgb_calibrated.joblib")
    print(f"Saved artifacts to {out_dir / 'xgb_calibrated.joblib'}")

    reporter.update("done")
    reporter.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())