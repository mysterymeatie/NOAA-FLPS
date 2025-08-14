#!/usr/bin/env python3
"""
Spatial GroupKFold tuner for Random Forest on 2016–2022.

- Builds spatial clusters (k-means on latitude/longitude) as groups
- Runs GroupKFold CV and evaluates PR-AUC (average_precision_score)
- Supports small hyperparameter grid and optional positive sample weighting
 - Optional inclusion of BalancedRandomForest if imbalanced-learn is available

Outputs results to models/tuning/rf_spatial_cv_results.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Tuple, Dict, Any
import threading
import time
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import average_precision_score
from sklearn.model_selection import GroupKFold


def load_manifest(splits_path: Path, schema_path: Path) -> Tuple[dict, dict]:
    with open(splits_path, "r", encoding="utf-8") as f:
        splits = json.load(f)
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    return splits, schema


def load_parquet_paths(paths: List[str], features: List[str], target: str, limit_rows_per_file: int | None = None) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    use_cols = list(set(features + [target] + ["latitude", "longitude"]))
    frames: List[pd.DataFrame] = []
    for p in paths:
        df = pd.read_parquet(p, columns=use_cols)
        if limit_rows_per_file and len(df) > limit_rows_per_file:
            df = df.sample(n=limit_rows_per_file, random_state=42)
        frames.append(df)
    df_all = pd.concat(frames, axis=0, ignore_index=True)
    X = df_all[features]
    y = df_all[target].astype(np.uint8)
    coords = df_all[["latitude", "longitude"]]
    return X, y, coords


def build_spatial_groups(coords: pd.DataFrame, n_clusters: int, random_state: int = 42) -> np.ndarray:
    # KMeans on [lat, lon] to produce spatial clusters
    kmeans = MiniBatchKMeans(n_clusters=n_clusters, random_state=random_state, batch_size=10000)
    groups = kmeans.fit_predict(coords.values)
    return groups


def evaluate_config(
    X: pd.DataFrame,
    y: pd.Series,
    groups: np.ndarray,
    config: Dict[str, Any],
    n_splits: int,
) -> Dict[str, Any]:
    gkf = GroupKFold(n_splits=n_splits)
    pr_aucs: List[float] = []
    fold_num = 0
    for train_idx, val_idx in gkf.split(X, y, groups):
        fold_num += 1
        X_tr, X_va = X.iloc[train_idx], X.iloc[val_idx]
        y_tr, y_va = y.iloc[train_idx], y.iloc[val_idx]

        model = None
        if config.get("balanced_rf", False):
            try:
                from imblearn.ensemble import BalancedRandomForestClassifier  # type: ignore
            except Exception as e:  # pragma: no cover
                raise RuntimeError("BalancedRandomForestClassifier requires imbalanced-learn. Please install 'imbalanced-learn'.") from e
            model = BalancedRandomForestClassifier(
                n_estimators=config["n_estimators"],
                max_depth=config["max_depth"],
                min_samples_leaf=config["min_samples_leaf"],
                max_features=config["max_features"],
                n_jobs=config["n_jobs"],
                random_state=42,
                bootstrap=True,
                replacement=False,
                verbose=0,
            )
        else:
            model = RandomForestClassifier(
                n_estimators=config["n_estimators"],
                max_depth=config["max_depth"],
                min_samples_leaf=config["min_samples_leaf"],
                max_features=config["max_features"],
                class_weight=config["class_weight"],
                n_jobs=config["n_jobs"],
                random_state=42,
                bootstrap=True,
                oob_score=False,
                verbose=0,
                max_samples=config.get("max_samples", None),
            )

        fit_kwargs = {}
        pos_w = config.get("sample_weight_pos", 0.0)
        if pos_w and pos_w > 0:
            sw = np.ones(len(y_tr), dtype=np.float32)
            sw[y_tr.values == 1] = float(pos_w)
            fit_kwargs["sample_weight"] = sw

        # Optional undersampling on train fold
        us_ratio = float(config.get("undersample_ratio", 0.0) or 0.0)
        if us_ratio > 0:
            pos_idx = y_tr[y_tr == 1].index
            neg_idx = y_tr[y_tr == 0].index
            num_pos = len(pos_idx)
            num_neg_keep = int(max(1, us_ratio * num_pos))
            if num_neg_keep < len(neg_idx):
                rng = np.random.RandomState(42)
                neg_keep = rng.choice(neg_idx, size=num_neg_keep, replace=False)
                keep_idx = pd.Index(pos_idx).append(pd.Index(neg_keep))
                X_tr = X_tr.loc[keep_idx]
                y_tr = y_tr.loc[keep_idx]
                if fit_kwargs:
                    fit_kwargs["sample_weight"] = fit_kwargs["sample_weight"][keep_idx]

        model.fit(X_tr, y_tr, **fit_kwargs)
        y_proba = model.predict_proba(X_va)[:, 1]
        pr = average_precision_score(y_va, y_proba)
        pr_aucs.append(float(pr))
        print(f"    fold {fold_num}/{n_splits}: PR-AUC={pr:.6f}")

    return {
        "mean_pr_auc": float(np.mean(pr_aucs)),
        "std_pr_auc": float(np.std(pr_aucs)),
        "folds": pr_aucs,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Tune RF with Spatial GroupKFold (2016–2022)")
    parser.add_argument("--splits", default=str(Path("config") / "ml_splits.json"))
    parser.add_argument("--schema", default=str(Path("config") / "ml_schema.json"))
    parser.add_argument("--clusters", type=int, default=200, help="Number of spatial clusters for grouping")
    parser.add_argument("--folds", type=int, default=5, help="Number of GroupKFold splits")
    parser.add_argument("--limit_rows_per_file", type=int, default=0, help="If >0, sample per-file rows for faster tuning")
    parser.add_argument("--output", default=str(Path("models") / "tuning" / "rf_spatial_cv_results.json"))
    parser.add_argument("--n_jobs", type=int, default=-1)
    parser.add_argument("--heartbeat_sec", type=int, default=180, help="Print a heartbeat every N seconds (>=30)")
    parser.add_argument("--no_heartbeat", action="store_true", help="Disable heartbeat logging")
    parser.add_argument("--brf_only", action="store_true", help="Only evaluate Balanced Random Forest configs")
    parser.add_argument("--minimal_grid", action="store_true", help="Use a small grid for <~1 hour runs")
    parser.add_argument("--checkpoint", type=str, default=str(Path("models") / "tuning" / "rf_spatial_cv_results.jsonl"), help="Path to JSONL checkpoints (one line per config)")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint; skip completed configs")
    args = parser.parse_args()

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
                ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
                print(f"[heartbeat] {ts} | elapsed={str(elapsed).split('.')[0]} | status={self._status}")
                self._stop_event.wait(self.interval_seconds)

    reporter = HeartbeatReporter(interval_seconds=args.heartbeat_sec)
    if not args.no_heartbeat:
        reporter.start()

    splits, schema = load_manifest(Path(args.splits), Path(args.schema))
    features: List[str] = schema["features"]
    target: str = schema["target"]

    # Use train years only (2016–2022) from splits
    train_paths = splits.get("train", [])
    if not train_paths:
        raise RuntimeError("No train paths in splits manifest")

    reporter.update("loading train features and coords")
    X, y, coords = load_parquet_paths(train_paths, features, target, limit_rows_per_file=(args.limit_rows_per_file or None))
    print(f"Loaded train rows for tuning: {len(y):,}; positives={int(y.sum()):,} ({y.mean():.5f})")

    reporter.update("building spatial clusters")
    groups = build_spatial_groups(coords, n_clusters=args.clusters)
    print(f"Built {args.clusters} spatial clusters for GroupKFold")

    def build_grid() -> List[Dict[str, Any]]:
        max_features_options: List[Any] = [0.7] if args.minimal_grid else ["sqrt", 0.7]
        class_weight_options: List[Any] = ["balanced_subsample"] if args.minimal_grid else ["balanced_subsample", "balanced"]
        grid: List[Dict[str, Any]] = []
        if not args.brf_only:
            ne_list = [800] if args.minimal_grid else [600, 1000]
            md_list = [24] if args.minimal_grid else [18, 24]
            ml_list = [10] if args.minimal_grid else [8, 20]
            for n_estimators in ne_list:
                for max_depth in md_list:
                    for min_leaf in ml_list:
                        for max_features in max_features_options:
                            for cw in class_weight_options:
                                grid.append({
                                    "balanced_rf": False,
                                    "n_estimators": n_estimators,
                                    "max_depth": max_depth,
                                    "min_samples_leaf": min_leaf,
                                    "max_features": max_features,
                                    "class_weight": cw,
                                    "max_samples": None,
                                    "sample_weight_pos": 0.0,
                                    "undersample_ratio": 0.0,
                                    "n_jobs": args.n_jobs,
                                })
        # BRF
        ne_list_brf = [800] if args.minimal_grid else [800, 1200]
        md_list_brf = [24] if args.minimal_grid else [18, 24]
        ml_list_brf = [10] if args.minimal_grid else [8, 20]
        for n_estimators in ne_list_brf:
            for max_depth in md_list_brf:
                for min_leaf in ml_list_brf:
                    for max_features in max_features_options:
                        grid.append({
                            "balanced_rf": True,
                            "n_estimators": n_estimators,
                            "max_depth": max_depth,
                            "min_samples_leaf": min_leaf,
                            "max_features": max_features,
                            "class_weight": None,
                            "max_samples": None,
                            "sample_weight_pos": 0.0,
                            "undersample_ratio": 0.0,
                            "n_jobs": args.n_jobs,
                        })
        return grid

    grid: List[Dict[str, Any]] = build_grid()
    print(f"Total configs: {len(grid)}")

    # Resume support: load completed config keys from checkpoint
    done_keys: set[str] = set()
    best: Dict[str, Any] | None = None
    best_score = -1.0
    ckpt_path = Path(args.checkpoint)
    ckpt_path.parent.mkdir(parents=True, exist_ok=True)
    if args.resume and ckpt_path.exists():
        with open(ckpt_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    key = json.dumps(rec.get("config", {}), sort_keys=True)
                    done_keys.add(key)
                    m = rec.get("metrics", {})
                    if "mean_pr_auc" in m and float(m["mean_pr_auc"]) > best_score:
                        best_score = float(m["mean_pr_auc"])
                        best = rec
                except Exception:
                    continue
        print(f"Resuming: found {len(done_keys)} completed configs in checkpoint")

    results: List[Dict[str, Any]] = []
    for idx, cfg in enumerate(grid, start=1):
        reporter.update(f"config {idx}/{len(grid)} evaluating")
        print(f"[{idx}/{len(grid)}] {cfg}")
        key = json.dumps(cfg, sort_keys=True)
        if args.resume and key in done_keys:
            print("  Skipping (already completed)")
            continue
        try:
            metrics = evaluate_config(X, y, groups, cfg, n_splits=args.folds)
        except RuntimeError as e:
            print(f"  Skipping config due to error: {e}")
            continue
        record = {"config": cfg, "metrics": metrics}
        results.append(record)
        if metrics["mean_pr_auc"] > best_score:
            best_score = metrics["mean_pr_auc"]
            best = record
        print(f"  mean PR-AUC={metrics['mean_pr_auc']:.6f} ± {metrics['std_pr_auc']:.6f}")
        # Append checkpoint line
        with open(ckpt_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    out = {
        "best": best,
        "results": results,
        "rows": len(y),
        "clusters": int(args.clusters),
        "folds": int(args.folds),
        "limit_rows_per_file": int(args.limit_rows_per_file or 0),
        "checkpoint": str(ckpt_path),
    }
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    print(f"Saved tuning results to {out_path}")
    if best:
        print(f"Best mean PR-AUC={best['metrics']['mean_pr_auc']:.6f} with config: {best['config']}")
    reporter.update("done")
    reporter.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

