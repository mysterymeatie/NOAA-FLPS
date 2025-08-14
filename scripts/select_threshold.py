#!/usr/bin/env python3
"""
Select an operating threshold targeting a desired recall (or precision) on a chosen split
and optionally evaluate that threshold on another split.

Supports RF artifacts saved by train_baseline_rf.py and XGBoost artifacts from train_xgboost.py.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Tuple, Dict, Any

import numpy as np
import pandas as pd
from sklearn.metrics import precision_recall_curve, average_precision_score, classification_report, confusion_matrix


class IsoWrap:
    """
    Pickle-compatibility wrapper for isotonic calibration saved in artifacts.
    When loading artifacts that were calibrated with a locally-defined class,
    joblib expects to find `IsoWrap` in __main__. Defining this top-level class
    enables unpickling and provides a compatible predict_proba interface.
    """
    def __init__(self, base=None) -> None:  # base may be injected by unpickler
        self.base = base
        try:
            from sklearn.isotonic import IsotonicRegression  # local import to avoid global dep timing
            self.iso = IsotonicRegression(out_of_bounds="clip")
        except Exception:
            self.iso = None  # populated by unpickler

    def fit(self, X: pd.DataFrame, y: pd.Series) -> "IsoWrap":
        # Not used during unpickling for already-fitted calibrators, but kept for API parity
        p = self.base.predict_proba(X)[:, 1]
        self.iso.fit(p, y.values)
        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        p = self.base.predict_proba(X)[:, 1]
        pc = self.iso.transform(p)
        pc = np.clip(pc, 0.0, 1.0).astype(np.float32)
        return np.column_stack([1.0 - pc, pc])


def load_manifest(splits_path: Path, schema_path: Path) -> Tuple[dict, dict]:
    with open(splits_path, "r", encoding="utf-8") as f:
        splits = json.load(f)
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    return splits, schema


def load_Xy(paths: List[str], features: List[str], target: str) -> Tuple[pd.DataFrame, pd.Series]:
    use_cols = list(set(features + [target]))
    frames: List[pd.DataFrame] = []
    for p in paths:
        df = pd.read_parquet(p, columns=use_cols)
        frames.append(df)
    df_all = pd.concat(frames, axis=0, ignore_index=True)
    X = df_all.loc[:, features].copy()
    y = df_all[target].astype(int)
    return X, y


def get_proba(artifact: Dict[str, Any], X: pd.DataFrame) -> np.ndarray:
    # RF artifact
    if "rf_pipeline" in artifact and "calibrator" in artifact:
        rf_pipe = artifact["rf_pipeline"]
        imputer = rf_pipe.named_steps.get("imputer")
        if imputer is not None:
            X_imp = imputer.transform(X)
        else:
            X_imp = X
        calibrator = artifact["calibrator"]
        proba = calibrator.predict_proba(X_imp)[:, 1]
        return proba
    # XGB artifact
    if "xgb" in artifact and "calibrator" in artifact:
        calibrator = artifact["calibrator"]
        proba = calibrator.predict_proba(X)[:, 1]
        return proba
    raise RuntimeError("Unsupported artifact structure. Expected rf_pipeline/calibrator or xgb/calibrator.")


def pick_threshold_for_target_recall(y_true: np.ndarray, y_proba: np.ndarray, target_recall: float) -> Tuple[float, Dict[str, float]]:
    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    # thresholds correspond to precision/recall[:-1]
    recall_thr = recall[:-1]
    prec_thr = precision[:-1]
    # choose the highest threshold whose recall >= target
    mask = recall_thr >= target_recall
    if not np.any(mask):
        # fallback to minimum threshold (max recall)
        idx = np.argmax(recall_thr)
    else:
        candidates = np.where(mask)[0]
        # among candidates, choose one with highest threshold (i.e., last index from the end)
        idx = candidates[-1]
    thr = thresholds[idx]
    return float(thr), {"precision": float(prec_thr[idx]), "recall": float(recall_thr[idx])}


def eval_at_threshold(y_true: np.ndarray, y_proba: np.ndarray, threshold: float) -> Dict[str, Any]:
    y_pred = (y_proba >= threshold).astype(int)
    ap = float(average_precision_score(y_true, y_proba))
    cm = confusion_matrix(y_true, y_pred)
    report = classification_report(y_true, y_pred, output_dict=True)
    tp = int(cm[1, 1]); fp = int(cm[0, 1]); fn = int(cm[1, 0]); tn = int(cm[0, 0])
    precision = report["1"]["precision"]
    recall = report["1"]["recall"]
    return {
        "threshold": float(threshold),
        "pr_auc": ap,
        "precision": float(precision),
        "recall": float(recall),
        "confusion_matrix": {"tn": tn, "fp": fp, "fn": fn, "tp": tp},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Select threshold targeting recall on validation and/or evaluate on test")
    parser.add_argument("--artifact", required=True, help="Path to saved joblib artifact")
    parser.add_argument("--splits", default=str(Path("config") / "ml_splits.json"))
    parser.add_argument("--schema", default=str(Path("config") / "ml_schema.json"))
    parser.add_argument("--select_split", default="val", choices=["train", "val", "test"], help="Split to select threshold on (default val)")
    parser.add_argument("--target_recall", type=float, default=0.5, help="Target recall for threshold selection (default 0.5)")
    parser.add_argument("--eval_split", default="", choices=["", "train", "val", "test"], help="Optional split to evaluate the chosen threshold on")
    parser.add_argument("--save", default="", help="Optional path to save selected threshold JSON")
    args = parser.parse_args()

    import joblib

    splits, schema = load_manifest(Path(args.splits), Path(args.schema))
    features: List[str] = schema["features"]
    target: str = schema["target"]

    # Select split
    select_paths = splits[args.select_split]
    X_sel, y_sel = load_Xy(select_paths, features, target)

    artifact = joblib.load(args.artifact)
    y_proba_sel = get_proba(artifact, X_sel)
    thr, sel_stats = pick_threshold_for_target_recall(y_sel.values, y_proba_sel, args.target_recall)
    sel_eval = eval_at_threshold(y_sel.values, y_proba_sel, thr)

    out: Dict[str, Any] = {
        "selected_on": args.select_split,
        "target_recall": float(args.target_recall),
        "threshold": sel_eval["threshold"],
        "selection_metrics": sel_eval,
    }

    # Optional eval on another split
    if args.eval_split:
        eval_paths = splits[args.eval_split]
        X_ev, y_ev = load_Xy(eval_paths, features, target)
        y_proba_ev = get_proba(artifact, X_ev)
        ev_metrics = eval_at_threshold(y_ev.values, y_proba_ev, thr)
        out["evaluated_on"] = args.eval_split
        out["evaluation_metrics"] = ev_metrics

    # Save or print
    if args.save:
        save_path = Path(args.save)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        print(f"Saved threshold to {save_path}")
    else:
        print(json.dumps(out, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

