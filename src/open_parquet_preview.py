from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python src/open_parquet_csv_export.py <parquet_path>")
        return

    path = Path(sys.argv[1])
    if not path.exists():
        print(f"File not found: {path}")
        return

    df = pd.read_parquet(path)

    out_csv = path.with_suffix(".preview.csv")
    df.head(5000).to_csv(out_csv, index=False)
    # Le prime 5000 righe

    print(f"Preview CSV saved to: {out_csv}")


if __name__ == "__main__":
    main()