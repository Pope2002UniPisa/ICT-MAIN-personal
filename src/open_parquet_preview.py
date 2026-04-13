from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python src/open_parquet_preview.py <parquet_path>")
        return

    path = Path(sys.argv[1])

    if not path.exists():
        print(f"File not found: {path}")
        return

    df = pd.read_parquet(path)

# Preview,
    out_csv = path.with_suffix(".preview.csv")
    df.head(5000).to_csv(out_csv, index=False)
    print(f"Preview CSV saved to: {out_csv}")

# Controllo range temporale
    print("\n=== TIME RANGE ===")
    print("min:", df["ts_event"].min())
    print("max:", df["ts_event"].max())