from __future__ import annotations

from pathlib import Path
import pandas as pd

from .config_simbols import SYMBOL_CONFIG, BASE_DIR, SIM_DIR, SIMULATION_CONFIG

OUT_DIR = BASE_DIR / "cross_market"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _load_real_mbp(symbol: str) -> pd.DataFrame:
    path = BASE_DIR / f"{symbol}_mbp1.parquet"
    df = pd.read_parquet(path)
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["venue"] = "BASE_REAL"
    df["simulated"] = False
    return df.sort_values("ts_event").reset_index(drop=True)


def _load_sim_mbp(symbol: str, venue: str) -> pd.DataFrame:
    path = SIM_DIR / f"{symbol}_{venue}_mbp1_sim.parquet"
    df = pd.read_parquet(path)
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    return df.sort_values("ts_event").reset_index(drop=True)


def prepare_cross_market_inputs() -> None:
    tolerance = pd.Timedelta(milliseconds=SIMULATION_CONFIG["sync_tolerance_ms"])

    for symbol, meta in SYMBOL_CONFIG.items():
        real_df = _load_real_mbp(symbol)

        venue_frames = [real_df[["ts_event", "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00", "venue", "simulated"]].copy()]
        venue_frames[0]["market_key"] = "BASE_REAL"

        for venue in meta.get("simulate_venues", {}):
            sim_df = _load_sim_mbp(symbol, venue)
            sim_df = sim_df[["ts_event", "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00", "venue", "simulated", "bid_px_eur", "ask_px_eur"]].copy()
            sim_df["market_key"] = venue
            venue_frames.append(sim_df)

        merged = pd.concat(venue_frames, axis=0, ignore_index=True)
        merged = merged.sort_values(["ts_event", "market_key"]).reset_index(drop=True)

        out_path = OUT_DIR / f"{symbol}_cross_market_mbp1.parquet"
        merged.to_parquet(out_path, index=False)

        if meta.get("use_hk_close_sentiment_proxy", False):
            hk_path = SIM_DIR / f"{symbol}_hk_close_sentiment_proxy.parquet"
            if hk_path.exists():
                hk = pd.read_parquet(hk_path)
                hk_out = OUT_DIR / f"{symbol}_hk_close_sentiment_proxy.parquet"
                hk.to_parquet(hk_out, index=False)

        print(f"[XMARKET] Saved {out_path}")


if __name__ == "__main__":
    prepare_cross_market_inputs()