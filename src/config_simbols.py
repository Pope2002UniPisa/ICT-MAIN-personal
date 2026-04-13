from pathlib import Path

RAW_BASE = Path("data/raw/databento")
PROCESSED_BASE = Path("data/processed")

SYMBOLS = {
    "UL": {
        "raw_dir": RAW_BASE / "UL",
        "processed_trades": PROCESSED_BASE / "UL_trades.parquet",
        "processed_mbp1": PROCESSED_BASE / "UL_mbp1.parquet",
    },
    "SHELL": {
        "raw_dir": RAW_BASE / "Shell",
        "processed_trades": PROCESSED_BASE / "SHELL_trades.parquet",
        "processed_mbp1": PROCESSED_BASE / "SHELL_mbp1.parquet",
    },
    "HSBC": {
        "raw_dir": RAW_BASE / "HSBC",
        "processed_trades": PROCESSED_BASE / "HSBC_trades.parquet",
        "processed_mbp1": PROCESSED_BASE / "HSBC_mbp1.parquet",
    },
}