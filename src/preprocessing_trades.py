from __future__ import annotations

from pathlib import Path
import databento as db
import pandas as pd


RAW_DIR = Path("data/raw/databento/UL/trades")
PROCESSED_DIR = Path("data/processed")
OUTPUT_FILE = PROCESSED_DIR / "UL_trades.parquet"


def load_single_dbn(file_path: Path) -> pd.DataFrame:
    store = db.DBNStore.from_file(str(file_path))
    df = store.to_df().copy()

    if df.index.name == "ts_event":
        df = df.reset_index()

    return df


def load_all_trade_files(raw_dir: Path) -> pd.DataFrame:
    files = sorted(raw_dir.glob("*.trades.dbn.zst"))
    if not files:
        raise FileNotFoundError(f"No trades files found in {raw_dir}")

    frames = []
    for file_path in files:
        print(f"Loading: {file_path.name}")
        df = load_single_dbn(file_path)
        df["source_file"] = file_path.name
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    return combined


def clean_trades(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "ts_event" in out.columns:
        out["ts_event"] = pd.to_datetime(out["ts_event"], utc=True, errors="coerce")
    if "ts_recv" in out.columns:
        out["ts_recv"] = pd.to_datetime(out["ts_recv"], utc=True, errors="coerce")

    out = out.dropna(subset=["ts_event"])
    out = out.sort_values("ts_event").reset_index(drop=True)

    out["event_date"] = out["ts_event"].dt.date
    out["ts_event_ms"] = out["ts_event"].astype("int64") // 10**6

    return out


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    df = load_all_trade_files(RAW_DIR)
    print(f"\nRaw shape: {df.shape}")

    df_clean = clean_trades(df)
    print(f"Clean shape: {df_clean.shape}")

    print("\nColumns:")
    print(df_clean.columns.tolist())

    print("\nHead:")
    print(df_clean.head())

    df_clean.to_parquet(OUTPUT_FILE, index=False)
    print(f"\nSaved processed file to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()