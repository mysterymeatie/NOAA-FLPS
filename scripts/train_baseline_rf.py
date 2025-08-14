#!/usr/bin/env python3
"""
Baseline Random Forest for Wildfire Risk Prediction (wired to manifests)

- Reads split and schema manifests from config/
- Loads train/val/test Parquet files accordingly
- Trains class-weighted RF on train
- Calibrates (isotonic) on validation
- Evaluates on test (PR-AUC, ROC-AUC, threshold-based metrics)
- Saves artifacts and plots
"""

import argparse
import json
import os
from pathlib import Path
import sys
from typing import List, Tuple
import threading
import time
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    average_precision_score,
    precision_recall_curve,
    roc_auc_score,
    classification_report,
    confusion_matrix,
    roc_curve,
)
from sklearn.calibration import CalibratedClassifierCV
from sklearn.pipeline import Pipeline
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
        process = psutil.Process(os.getpid())
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
        # Initial delay to avoid spamming on short runs
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
    X = df_all[features].copy()
    y = df_all[target].astype(np.uint8)
    if downcast_float32:
        # Downcast numeric feature columns to float32 to reduce memory
        for c in X.columns:
            if np.issubdtype(X[c].dtype, np.floating):
                X[c] = X[c].astype(np.float32)
            elif np.issubdtype(X[c].dtype, np.integer):
                X[c] = X[c].astype(np.int32)
        print(f"{log_prefix}Downcasted features to float32/int32 where applicable")
    print(f"{log_prefix}Loaded rows: {len(df_all):,}; features shape: {X.shape}")
    mem_mb = (X.memory_usage(deep=True).sum() + y.memory_usage(deep=True)) / (1024 * 1024)
    print(f"{log_prefix}Approx memory for X+y: {mem_mb:.1f} MB")
    return X, y


def build_rf_pipeline(
    n_jobs: int,
    rf_verbose: int,
    n_estimators: int,
    max_depth: int,
    min_samples_leaf: int,
    max_samples: float | None,
    class_weight: str | None,
    max_features: str | float,
    use_balanced_rf: bool = False,
) -> Pipeline:
    imputer = SimpleImputer(strategy="median")
    if use_balanced_rf:
        try:
            from imblearn.ensemble import BalancedRandomForestClassifier  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("BalancedRandomForestClassifier requires imbalanced-learn. Please install 'imbalanced-learn'.") from e
        rf = BalancedRandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_leaf=min_samples_leaf,
            max_features=max_features,
            n_jobs=n_jobs,
            random_state=42,
            bootstrap=True,
            replacement=False,
            verbose=rf_verbose,
        )
    else:
        rf = RandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_leaf=min_samples_leaf,
            max_features=max_features,
            class_weight=class_weight,
            n_jobs=n_jobs,
            random_state=42,
            bootstrap=True,
            oob_score=False,
            verbose=rf_verbose,
            max_samples=max_samples,
        )
    return Pipeline([
        ("imputer", imputer),
        ("rf", rf),
    ])


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
    parser = argparse.ArgumentParser(description="Train Random Forest using manifests")
    parser.add_argument("--splits", default=str(Path("config") / "ml_splits.json"))
    parser.add_argument("--schema", default=str(Path("config") / "ml_schema.json"))
    parser.add_argument("--output", default=str(Path("models") / "baseline"))
    parser.add_argument("--n_jobs", type=int, default=-1, help="Parallel jobs for RF (-1 uses all cores)")
    parser.add_argument("--rf_verbose", type=int, default=1, help="RF training verbosity (0=silent)")
    parser.add_argument("--downcast_float32", action="store_true", help="Downcast features to float32/int32 to reduce memory")
    parser.add_argument("--limit_rows", type=int, default=0, help="If >0, randomly sample this many rows from each split")
    parser.add_argument("--n_estimators", type=int, default=600, help="Number of trees")
    parser.add_argument("--max_depth", type=int, default=20, help="Maximum tree depth")
    parser.add_argument("--min_samples_leaf", type=int, default=20, help="Minimum samples per leaf")
    parser.add_argument("--max_samples", type=float, default=1.0, help="If <1, fraction of samples per tree when bootstrap=True")
    parser.add_argument("--class_weight", type=str, default="balanced_subsample", choices=["balanced_subsample", "balanced", "none"], help="Class weighting strategy for RF")
    parser.add_argument("--max_features", default="sqrt", help="Max features for splits: 'sqrt', 'log2', or float fraction (e.g., 0.7)")
    parser.add_argument("--undersample_ratio", type=float, default=0.0, help="If >0, downsample negatives to undersample_ratio * num_positives in train")
    parser.add_argument("--sample_weight_pos", type=float, default=0.0, help="If >0, multiply positive class weights by this factor during fit")
    parser.add_argument("--balanced_rf", action="store_true", help="Use BalancedRandomForestClassifier (requires imbalanced-learn)")
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

    # Optional undersampling on training data only
    if args.undersample_ratio and args.undersample_ratio > 0:
        pos_idx = y_train[y_train == 1].index
        neg_idx = y_train[y_train == 0].index
        num_pos = len(pos_idx)
        num_neg_keep = int(max(1, args.undersample_ratio * num_pos))
        if num_neg_keep < len(neg_idx):
            neg_keep = np.random.RandomState(42).choice(neg_idx, size=num_neg_keep, replace=False)
            keep_idx = pd.Index(pos_idx).append(pd.Index(neg_keep))
            X_train = X_train.loc[keep_idx]
            y_train = y_train.loc[keep_idx]
            print(f"[train] Applied undersampling: positives={len(pos_idx):,}, negatives kept={num_neg_keep:,}")

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

    # Fit class-weighted RF
    reporter.update("training random_forest")
    print("\nTraining Random Forest...")
    max_samples = None if args.max_samples is None or args.max_samples >= 1.0 else max(args.max_samples, 0.1)
    rf_pipeline = build_rf_pipeline(
        n_jobs=args.n_jobs,
        rf_verbose=args.rf_verbose,
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        min_samples_leaf=args.min_samples_leaf,
        max_samples=max_samples,
        class_weight=(None if args.class_weight == "none" else args.class_weight),
        max_features=(float(args.max_features) if str(args.max_features).replace('.', '', 1).isdigit() else args.max_features),
        use_balanced_rf=bool(args.balanced_rf),
    )
    # Optional sample weights to increase positive influence
    fit_kwargs = {}
    if args.sample_weight_pos and args.sample_weight_pos > 0:
        sample_weight = np.ones(len(y_train), dtype=np.float32)
        sample_weight[y_train.values == 1] = float(args.sample_weight_pos)
        fit_kwargs["rf__sample_weight"] = sample_weight
    rf_pipeline.fit(X_train, y_train, **fit_kwargs)

    # Calibrate with validation (prefit model)
    reporter.update("calibrating (isotonic) on validation")
    print("Calibrating on validation (isotonic)...")
    # Extract fitted RF from pipeline for prefit calibration
    imputer = rf_pipeline.named_steps["imputer"]
    rf = rf_pipeline.named_steps["rf"]

    # Transform val/test with imputer
    X_val_imp = imputer.transform(X_val)
    X_test_imp = imputer.transform(X_test)

    calibrator = CalibratedClassifierCV(rf, method="isotonic", cv="prefit")
    calibrator.fit(X_val_imp, y_val)

    # Evaluate on test
    reporter.update("evaluating on test")
    y_proba_test = calibrator.predict_proba(X_test_imp)[:, 1]
    evaluate_and_plot(y_test.values, y_proba_test, model_name="rf", out_dir=out_dir)

    # Persist artifacts
    reporter.update("saving artifacts")
    joblib.dump({
        "rf_pipeline": rf_pipeline,
        "calibrator": calibrator,
        "features": features,
        "target": target,
        "splits": splits,
    }, out_dir / "rf_calibrated.joblib")
    print(f"Saved artifacts to {out_dir / 'rf_calibrated.joblib'}")

    reporter.update("done")
    reporter.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())