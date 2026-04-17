from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

from .config_simbols import BASE_DIR, SIM_DIR, SYMBOL_CONFIG


OUT_DIR = BASE_DIR / "simulation_validation"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _load_base_mbp(symbol: str) -> pd.DataFrame:
    path = BASE_DIR / f"{symbol}_mbp1.parquet"
    df = pd.read_parquet(path)
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts_event"]).copy()
    return df.sort_values("ts_event").reset_index(drop=True)


def _load_sim_mbp(symbol: str, venue: str) -> pd.DataFrame:
    path = SIM_DIR / f"{symbol}_{venue}_mbp1_sim.parquet"
    df = pd.read_parquet(path)
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts_event"]).copy()
    return df.sort_values("ts_event").reset_index(drop=True)


def _summarize_mbp(df: pd.DataFrame, label: str) -> dict:
    mid = 0.5 * (df["bid_px_00"] + df["ask_px_00"])
    spread = df["ask_px_00"] - df["bid_px_00"]
    ret = mid.pct_change()

    return {
        "label": label,
        "rows": len(df),
        "mid_mean": float(mid.mean()),
        "mid_std": float(mid.std()),
        "ret_std": float(ret.std()),
        "spread_mean": float(spread.mean()),
        "spread_std": float(spread.std()),
        "bid_sz_mean": float(df["bid_sz_00"].mean()),
        "ask_sz_mean": float(df["ask_sz_00"].mean()),
    }


def validate_simulation() -> None:
    rows = []

    for symbol, meta in SYMBOL_CONFIG.items():
        base = _load_base_mbp(symbol)
        rows.append({"symbol": symbol, **_summarize_mbp(base, "BASE_REAL")})

        for venue in meta.get("simulate_venues", {}):
            sim = _load_sim_mbp(symbol, venue)
            rows.append({"symbol": symbol, **_summarize_mbp(sim, f"SIM_{venue}")})

    out = pd.DataFrame(rows)
    out_path = OUT_DIR / "simulation_validation_summary.csv"
    out.to_csv(out_path, index=False)

    print(f"[VALIDATION] Saved: {out_path}")
    print(out)


if __name__ == "__main__":
    validate_simulation()