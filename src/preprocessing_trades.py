from __future__ import annotations

from pathlib import Path
import databento as db
import pandas as pd


PROCESSED_DIR = Path("data/processed")


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
        frames.append(df)

    return pd.concat(frames, ignore_index=True)


def clean_trades(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    out = df.copy()

    if "ts_event" not in out.columns:
        raise ValueError(f"{symbol}: missing 'ts_event' column")
    if "price" not in out.columns:
        raise ValueError(f"{symbol}: missing 'price' column")
    if "size" not in out.columns:
        raise ValueError(f"{symbol}: missing 'size' column")

    out["ts_event"] = pd.to_datetime(out["ts_event"], utc=True, errors="coerce")
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out["size"] = pd.to_numeric(out["size"], errors="coerce")

    out = out.dropna(subset=["ts_event", "price", "size"]).copy()
    out = out.sort_values("ts_event").reset_index(drop=True)

    out["symbol"] = symbol
    out["event_date"] = out["ts_event"].dt.date
    out["ts_event_ms"] = out["ts_event"].astype("int64") // 10**6

    final_cols = [
        "ts_event",
        "symbol",
        "price",
        "size",
        "event_date",
        "ts_event_ms",
    ]

    if "side" in out.columns:
        final_cols.insert(4, "side")

    out = out[final_cols].copy()
    return out


def process_symbol(symbol: str, folder_name: str | None = None) -> Path:
    folder = folder_name or symbol
    raw_dir = Path(f"data/raw/databento/{folder}/trades")
    output_file = PROCESSED_DIR / f"{symbol}_trades.parquet"

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    df = load_all_trade_files(raw_dir)
    print(f"\n[{symbol}] Raw shape: {df.shape}")

    df_clean = clean_trades(df, symbol=symbol)
    print(f"[{symbol}] Clean shape: {df_clean.shape}")

    print(f"\n[{symbol}] Columns:")
    print(df_clean.columns.tolist())

    print(f"\n[{symbol}] Head:")
    print(df_clean.head())

    df_clean.to_parquet(output_file, index=False)
    print(f"\n[{symbol}] Saved processed file to: {output_file}")

    return output_file


def main() -> None:
    process_symbol("UL", "UL")
    process_symbol("SHELL", "Shell")
    process_symbol("HSBC", "HSBC")


if __name__ == "__main__":
    main()