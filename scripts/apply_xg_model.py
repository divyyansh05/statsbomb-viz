"""
Apply trained xG model to fact_shots and write xg_model column to DuckDB.
Run after xg_model.py has been trained and saved to models/xg_model.pkl.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import joblib
import pandas as pd
from pipeline.utils import get_db_connection
from scripts.xg_model import build_features

def main():
    print("Loading model...")
    model = joblib.load(Path(__file__).parent.parent / "models" / "xg_model.pkl")

    print("Loading fact_shots...")
    con = get_db_connection(read_only=False)
    df = con.execute("SELECT * FROM fact_shots").df()

    print(f"  {len(df):,} shots loaded")

    # Build features — fill nulls for rows missing location
    df_valid = df[df["location_x"].notna() & df["location_y"].notna()].copy()
    X = build_features(df_valid)
    df_valid["xg_model"] = model.predict_proba(X)[:, 1]

    # Penalties get fixed xG of 0.76 (historical average)
    df.loc[df["shot_type"] == "Penalty", "xg_model"] = 0.76
    df.update(df_valid[["xg_model"]])
    df["xg_model"] = df["xg_model"].fillna(0.0)

    print("Writing xg_model column to DuckDB...")
    con.execute("ALTER TABLE fact_shots DROP COLUMN IF EXISTS xg_model")
    con.execute("ALTER TABLE fact_shots ADD COLUMN xg_model DOUBLE")

    # Update row by row is slow — replace table instead
    con.execute("DROP TABLE fact_shots")
    con.execute("CREATE TABLE fact_shots AS SELECT * FROM df")

    count = con.execute("SELECT COUNT(*) FROM fact_shots WHERE xg_model > 0").fetchone()[0]
    print(f"  {count:,} shots now have xg_model values")

    sample = con.execute("""
        SELECT outcome, 
               ROUND(AVG(xg),3) as avg_statsbomb_xg, 
               ROUND(AVG(xg_model),3) as avg_our_xg,
               COUNT(*) as shots
        FROM fact_shots 
        GROUP BY outcome 
        ORDER BY avg_statsbomb_xg DESC
    """).df()
    print("\nxG by outcome:")
    print(sample.to_string(index=False))

    con.close()
    print("\n✅ fact_shots updated with xg_model column")

if __name__ == "__main__":
    main()