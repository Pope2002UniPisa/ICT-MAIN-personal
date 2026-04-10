from __future__ import annotations

from pathlib import Path
import databento as db
import pandas as pd


RAW_DIR = Path("data/raw/databento/UL/mbp1")
PROCESSED_DIR = Path("data/processed")
OUTPUT_FILE = PROCESSED_DIR / "UL_mbp1.parquet"


def load_single_dbn(file_path: Path) -> pd.DataFrame:
    """
    Load one Databento DBN file into a pandas DataFrame.
    """
    store = db.DBNStore.from_file(str(file_path))
    df = store.to_df().copy()

    # If ts_event is in the index, bring it back as a normal column
    if df.index.name == "ts_event":
        df = df.reset_index()

    return df


def load_all_mbp1_files(raw_dir: Path) -> pd.DataFrame:
    """
    Load and concatenate all MBP-1 DBN files in a folder.
    """
    files = sorted(raw_dir.glob("*.mbp-1.dbn.zst"))
    if not files:
        raise FileNotFoundError(f"No MBP-1 files found in {raw_dir}")

    frames = []
    for file_path in files:
        print(f"Loading: {file_path.name}")
        df = load_single_dbn(file_path)
        df["source_file"] = file_path.name
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    return combined


def clean_mbp1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic cleaning and standardization for MBP-1 data.
    """
    out = df.copy()

    # Ensure timestamps are proper datetimes
    if "ts_event" in out.columns:
        out["ts_event"] = pd.to_datetime(out["ts_event"], utc=True, errors="coerce")
    if "ts_recv" in out.columns:
        out["ts_recv"] = pd.to_datetime(out["ts_recv"], utc=True, errors="coerce")

    # Remove rows without event timestamp
    out = out.dropna(subset=["ts_event"])

    # Sort by event time
    out = out.sort_values("ts_event").reset_index(drop=True)

    # Convenience columns
    out["mid_price"] = (out["bid_px_00"] + out["ask_px_00"]) / 2
    out["spread"] = out["ask_px_00"] - out["bid_px_00"]
    out["event_date"] = out["ts_event"].dt.date

    # Millisecond timestamp if needed later
    out["ts_event_ms"] = out["ts_event"].astype("int64") // 10**6

    return out


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    df = load_all_mbp1_files(RAW_DIR)
    print(f"\nRaw shape: {df.shape}")

    df_clean = clean_mbp1(df)
    print(f"Clean shape: {df_clean.shape}")

    print("\nColumns:")
    print(df_clean.columns.tolist())

    print("\nHead:")
    print(df_clean.head())

    df_clean.to_parquet(OUTPUT_FILE, index=False)
    print(f"\nSaved processed file to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()