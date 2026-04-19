# Script di utilità per ispezionare qualsiasi file .parquet del progetto.

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

    out_csv = path.with_suffix(".preview.csv")
    if "market_key" in df.columns:
        preview = (
            df.groupby("market_key", group_keys=False)
            .apply(lambda g: g.head(1000))
            .sort_values("ts_event")
            .reset_index(drop=True)
        )
    else:
        preview = df.head(5000)
    preview.to_csv(out_csv, index=False)
    print(f"Preview CSV saved to: {out_csv}")

    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")

    # Stampa il range temporale degli eventi nel file
    if "ts_event" in df.columns:
        print("\n=== TIME RANGE ===")
        print("min:", df["ts_event"].dropna().min())
        print("max:", df["ts_event"].dropna().max())

    # python src/open_parquet_preview.py data/processed/UL_mbp1.parquet
    # python src/open_parquet_preview.py data/processed/SHELL_mbp1.parquet
    # python src/open_parquet_preview.py data/processed/HSBC_mbp1.parquet


if __name__ == "__main__":
    main()