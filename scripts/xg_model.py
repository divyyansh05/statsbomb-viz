"""
xG Model — Train a logistic regression expected goals model on StatsBomb shot data.

Theory:
    xG is a binary classification problem: given shot features, predict P(goal).
    We use logistic regression because:
    - Output is naturally a probability [0, 1]
    - Interpretable coefficients
    - Good baseline before trying complex models

Features engineered:
    - distance_to_goal: Euclidean distance from shot location to goal centre
    - angle_to_goal: angle in radians subtended by the goal from shot location
    - is_header: body part == Head
    - is_penalty: shot type == Penalty
    - is_open_play: shot type == Open Play
    - is_first_time: first-time shot boolean
    - under_pressure: shot taken under pressure boolean

Evaluation:
    - Log-loss (lower = better probability calibration)
    - Brier score (lower = better)
    - Correlation with StatsBomb xG
    - Goals predicted vs actual at various thresholds
"""

import sys
import os
import numpy as np
import pandas as pd
from pathlib import Path

# ── Path setup ─────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.utils import get_db_connection

import warnings
warnings.filterwarnings("ignore")

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import (
    log_loss, brier_score_loss, roc_auc_score,
    classification_report
)
from sklearn.calibration import calibration_curve
import joblib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ── Constants ──────────────────────────────────────────────────────────────────
GOAL_X = 120.0          # StatsBomb pitch: goal centre x
GOAL_Y = 40.0           # StatsBomb pitch: goal centre y
GOAL_WIDTH = 7.32       # metres
MODELS_DIR = Path(__file__).parent.parent / "models"
MODELS_DIR.mkdir(exist_ok=True)


# ── Feature engineering ────────────────────────────────────────────────────────

def compute_distance(x: pd.Series, y: pd.Series) -> pd.Series:
    """Euclidean distance from shot location to goal centre."""
    return np.sqrt((GOAL_X - x) ** 2 + (GOAL_Y - y) ** 2)


def compute_angle(x: pd.Series, y: pd.Series) -> pd.Series:
    """
    Angle subtended by the goal mouth at the shot location (radians).
    
    Uses the cross-product / dot-product formula for angle between two vectors:
        - Vector A: shot location → left post  (120, 36.34)
        - Vector B: shot location → right post (120, 43.66)
    
    Wider angle = more of the goal is visible = higher xG.
    """
    post_left_y  = 36.34
    post_right_y = 43.66

    # Vectors from shot location to each post
    ax = GOAL_X - x
    ay = post_left_y - y
    bx = GOAL_X - x
    by = post_right_y - y

    # Angle = arctan2(|cross|, dot)
    cross = ax * by - ay * bx
    dot   = ax * bx + ay * by
    return np.arctan2(np.abs(cross), dot)


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Engineer all features from raw fact_shots columns."""
    features = pd.DataFrame(index=df.index)

    features["distance"]       = compute_distance(df["location_x"], df["location_y"])
    features["angle"]          = compute_angle(df["location_x"], df["location_y"])
    features["distance_sq"]    = features["distance"] ** 2   # non-linear distance term
    features["is_header"]      = (df["body_part"] == "Head").astype(int)
    features["is_penalty"]     = (df["shot_type"] == "Penalty").astype(int)
    features["is_open_play"]   = (df["shot_type"] == "Open Play").astype(int)
    features["is_first_time"]  = df["is_first_time"].fillna(False).astype(int)
    features["under_pressure"] = df["under_pressure"].fillna(False).astype(int)

    return features


# ── Load data ──────────────────────────────────────────────────────────────────

def load_shots() -> pd.DataFrame:
    print("Loading fact_shots from DuckDB...")
    con = get_db_connection(read_only=True)
    df = con.execute("""
        SELECT
            event_id, match_id, location_x, location_y,
            body_part, shot_type, is_first_time, under_pressure,
            xg AS statsbomb_xg,
            is_goal::INTEGER AS goal
        FROM fact_shots
        WHERE location_x IS NOT NULL
          AND location_y IS NOT NULL
          AND shot_type != 'Penalty'   -- exclude penalties from model (separate rate)
    """).df()
    con.close()
    print(f"  Loaded {len(df):,} shots ({df['goal'].sum()} goals, {df['goal'].mean()*100:.1f}% conversion)")
    return df


# ── Train ──────────────────────────────────────────────────────────────────────

def train(df: pd.DataFrame):
    print("\nEngineering features...")
    X = build_features(df)
    y = df["goal"]

    print(f"  Features: {list(X.columns)}")
    print(f"  Shape: {X.shape}")

    # Pipeline: scale → logistic regression
    model = Pipeline([
        ("scaler", StandardScaler()),
        ("lr", LogisticRegression(
            C=1.0,
            max_iter=1000,
            random_state=42,
            # removed class_weight="balanced"   # handles goal/no-goal imbalance
        ))
    ])

    # Cross-validation (stratified to preserve goal ratio in each fold)
    print("\nCross-validating (5-fold stratified)...")
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    
    auc_scores = cross_val_score(model, X, y, cv=cv, scoring="roc_auc")
    ll_scores  = cross_val_score(model, X, y, cv=cv, scoring="neg_log_loss")

    print(f"  ROC-AUC:  {auc_scores.mean():.3f} ± {auc_scores.std():.3f}")
    print(f"  Log-loss: {(-ll_scores).mean():.3f} ± {(-ll_scores).std():.3f}")

    # Train final model on all data
    print("\nTraining final model on full dataset...")
    model.fit(X, y)

    return model, X, y


# ── Evaluate ───────────────────────────────────────────────────────────────────

def evaluate(model, X: pd.DataFrame, y: pd.Series, df: pd.DataFrame):
    print("\n── Evaluation ──────────────────────────────────────────────────")

    xg_model = model.predict_proba(X)[:, 1]
    xg_sb    = df["statsbomb_xg"].values

    # Core metrics — our model
    ll_ours  = log_loss(y, xg_model)
    bs_ours  = brier_score_loss(y, xg_model)
    auc_ours = roc_auc_score(y, xg_model)

    # Core metrics — StatsBomb model
    ll_sb    = log_loss(y, xg_sb)
    bs_sb    = brier_score_loss(y, xg_sb)
    auc_sb   = roc_auc_score(y, xg_sb)

    print(f"\n{'Metric':<20} {'Our Model':>12} {'StatsBomb':>12}")
    print("-" * 46)
    print(f"{'Log-loss':<20} {ll_ours:>12.4f} {ll_sb:>12.4f}  (lower = better)")
    print(f"{'Brier score':<20} {bs_ours:>12.4f} {bs_sb:>12.4f}  (lower = better)")
    print(f"{'ROC-AUC':<20} {auc_ours:>12.4f} {auc_sb:>12.4f}  (higher = better)")

    # Correlation between our xG and StatsBomb xG
    corr = np.corrcoef(xg_model, xg_sb)[0, 1]
    print(f"\n  Correlation (ours vs StatsBomb xG): {corr:.3f}")

    # Total xG comparison
    print(f"\n  Total xG (our model):   {xg_model.sum():.1f}")
    print(f"  Total xG (StatsBomb):   {xg_sb.sum():.1f}")
    print(f"  Actual goals:           {y.sum()}")

    # Coefficients (interpretability)
    lr = model.named_steps["lr"]
    scaler = model.named_steps["scaler"]
    coef_df = pd.DataFrame({
        "feature":     X.columns,
        "coefficient": lr.coef_[0]
    }).sort_values("coefficient", ascending=False)
    print(f"\n── Feature Coefficients (logistic regression) ──────────────────")
    print(coef_df.to_string(index=False))
    print("\n  Positive = increases goal probability")
    print("  Negative = decreases goal probability")

    return xg_model


# ── Calibration plot ───────────────────────────────────────────────────────────

def plot_calibration(y, xg_model, xg_sb):
    fig, ax = plt.subplots(figsize=(7, 5))

    frac_pos_ours, mean_pred_ours = calibration_curve(y, xg_model, n_bins=10)
    frac_pos_sb,   mean_pred_sb   = calibration_curve(y, xg_sb,    n_bins=10)

    ax.plot([0, 1], [0, 1], "k--", label="Perfect calibration")
    ax.plot(mean_pred_ours, frac_pos_ours, "o-", label="Our xG model", color="#e74c3c")
    ax.plot(mean_pred_sb,   frac_pos_sb,   "s-", label="StatsBomb xG",  color="#2ecc71")

    ax.set_xlabel("Mean predicted probability")
    ax.set_ylabel("Fraction of positives (actual goals)")
    ax.set_title("xG Model Calibration\n(closer to dashed line = better calibrated)")
    ax.legend()
    ax.grid(alpha=0.3)

    out_path = MODELS_DIR / "xg_calibration.png"
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    print(f"\n  Calibration plot saved → {out_path}")
    plt.close()


# ── Save model ─────────────────────────────────────────────────────────────────

def save_model(model):
    path = MODELS_DIR / "xg_model.pkl"
    joblib.dump(model, path)
    print(f"\n  Model saved → {path}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  StatsBomb xG Model — Logistic Regression")
    print("=" * 60)

    df = load_shots()
    model, X, y = train(df)
    xg_model = evaluate(model, X, y, df)
    plot_calibration(y, xg_model, df["statsbomb_xg"].values)
    save_model(model)

    print("\n✅ Done. Run this to add xg_model column to fact_shots:")
    print("   python scripts/apply_xg_model.py")


if __name__ == "__main__":
    main()