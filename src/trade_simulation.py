from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

from .config_simbols import BASE_DIR, SYMBOL_CONFIG, SIMULATION_CONFIG


SAMPLES_DIR = BASE_DIR / "opportunity_samples"
OUT_DIR = BASE_DIR / "simulated_trades"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _load_samples(symbol: str) -> pd.DataFrame:
    path = SAMPLES_DIR / f"{symbol}_opportunity_samples.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Opportunity samples file not found: {path}")

    df = pd.read_parquet(path)
    if df.empty:
        return df

    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts_event"]).copy()
    return df.sort_values(["window_id", "ts_event"]).reset_index(drop=True)


def _resample_window_every_10ms(window_df: pd.DataFrame, step_ms: int) -> pd.DataFrame:
    if window_df.empty:
        return pd.DataFrame()

    start = window_df["ts_event"].min()
    end = window_df["ts_event"].max()

    if pd.isna(start) or pd.isna(end) or end < start:
        return pd.DataFrame()

    sample_times = pd.date_range(start=start, end=end, freq=f"{step_ms}ms", tz="UTC")
    if len(sample_times) == 0:
        sample_times = pd.DatetimeIndex([start])

    target = pd.DataFrame({"ts_event": sample_times})

    res = pd.merge_asof(
        target.sort_values("ts_event"),
        window_df.sort_values("ts_event"),
        on="ts_event",
        direction="backward",
        tolerance=pd.Timedelta(milliseconds=step_ms),
    )

    res = res.dropna(subset=["gap_eur", "threshold_eur"]).copy()
    return res.reset_index(drop=True)


def _risk_buffer(sigma: pd.Series) -> pd.Series:
    risk_lambda = SIMULATION_CONFIG["risk_lambda"]
    tau_ms = SIMULATION_CONFIG["tau_ms_for_risk"]
    tau_sec = tau_ms / 1000.0
    sigma_safe = sigma.fillna(0.0)
    return risk_lambda * sigma_safe * np.sqrt(tau_sec)


def simulate_trading() -> None:
    trade_block_size = SIMULATION_CONFIG["trade_block_size"]
    trade_step_ms = SIMULATION_CONFIG["trade_step_ms"]
    cost_unit_eur = SIMULATION_CONFIG["cost_unit_eur"]
    fixed_buffer_eur = SIMULATION_CONFIG["fixed_buffer_eur"]

    for symbol in SYMBOL_CONFIG.keys():
        print(f"[TRADESIM] Processing {symbol}...")

        samples = _load_samples(symbol)

        trades_out = OUT_DIR / f"{symbol}_trade_blocks.parquet"
        summary_out = OUT_DIR / f"{symbol}_trade_summary.parquet"

        if samples.empty:
            print(f"[TRADESIM] No sample points for {symbol}. Saving empty outputs.")
            pd.DataFrame().to_parquet(trades_out, index=False)
            pd.DataFrame().to_parquet(summary_out, index=False)
            continue

        blocks = []

        for window_id, wdf in samples.groupby("window_id", sort=True):
            wdf = wdf.sort_values("ts_event").reset_index(drop=True)

            sampled = _resample_window_every_10ms(wdf, trade_step_ms)
            if sampled.empty:
                continue

            sampled["trade_block_size"] = trade_block_size
            sampled["risk_buffer_unit_eur"] = _risk_buffer(sampled["sigma_local"])
            sampled["cost_unit_eur"] = cost_unit_eur
            sampled["fixed_buffer_eur"] = fixed_buffer_eur

            sampled["gross_profit_eur"] = trade_block_size * sampled["gap_eur"]
            sampled["net_profit_eur"] = (
                trade_block_size * sampled["gap_eur"]
                - trade_block_size * sampled["cost_unit_eur"]
                - trade_block_size * sampled["risk_buffer_unit_eur"]
                - sampled["fixed_buffer_eur"]
            )

            sampled["window_id"] = window_id
            sampled["symbol"] = wdf["symbol"].iloc[0]
            sampled["pair"] = wdf["pair"].iloc[0]
            sampled["direction"] = wdf["direction"].iloc[0]

            keep_cols = [
                "window_id",
                "symbol",
                "pair",
                "direction",
                "ts_event",
                "trade_block_size",
                "gap_eur",
                "threshold_eur",
                "sigma_local",
                "book_qty",
                "cost_unit_eur",
                "fixed_buffer_eur",
                "risk_buffer_unit_eur",
                "gross_profit_eur",
                "net_profit_eur",
            ]
            sampled = sampled[keep_cols].copy()
            blocks.append(sampled)

        if not blocks:
            print(f"[TRADESIM] No trade blocks generated for {symbol}.")
            pd.DataFrame().to_parquet(trades_out, index=False)
            pd.DataFrame().to_parquet(summary_out, index=False)
            continue

        trades = pd.concat(blocks, ignore_index=True)
        trades = trades.sort_values(["window_id", "ts_event"]).reset_index(drop=True)

        summary = trades.groupby(["window_id", "symbol", "pair", "direction"], as_index=False).agg(
            n_blocks=("ts_event", "size"),
            total_qty=("trade_block_size", "sum"),
            gross_profit_total_eur=("gross_profit_eur", "sum"),
            net_profit_total_eur=("net_profit_eur", "sum"),
            avg_gap_eur=("gap_eur", "mean"),
            max_gap_eur=("gap_eur", "max"),
            avg_sigma_local=("sigma_local", "mean"),
        )

        trades.to_parquet(trades_out, index=False)
        summary.to_parquet(summary_out, index=False)

        print(f"[TRADESIM] Saved: {trades_out}")
        print(f"[TRADESIM] Saved: {summary_out}")
        print(
            f"[TRADESIM] {symbol}: "
            f"{len(summary):,} windows traded | "
            f"{len(trades):,} trade blocks | "
            f"gross total = {summary['gross_profit_total_eur'].sum():.2f} EUR | "
            f"net total = {summary['net_profit_total_eur'].sum():.2f} EUR"
        )


if __name__ == "__main__":
    simulate_trading()