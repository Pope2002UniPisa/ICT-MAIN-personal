from __future__ import annotations

import numpy as np
import pandas as pd

from .config_simbols import BASE_DIR, SYMBOL_CONFIG, SIMULATION_CONFIG


CROSS_DIR = BASE_DIR / "cross_market"
OUT_DIR = BASE_DIR / "opportunities"
SAMPLES_DIR = BASE_DIR / "opportunity_samples"
OVERLAP_PATH = BASE_DIR / "market_overlaps.parquet"

OUT_DIR.mkdir(parents=True, exist_ok=True)
SAMPLES_DIR.mkdir(parents=True, exist_ok=True)


def _load_cross_market(symbol: str) -> pd.DataFrame:
    path = CROSS_DIR / f"{symbol}_cross_market_mbp1.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Cross-market file not found: {path}")

    df = pd.read_parquet(path)
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts_event"]).copy()
    df = df.sort_values(["market_key", "ts_event"]).reset_index(drop=True)
    return df


def _load_overlaps(symbol: str) -> pd.DataFrame:
    if not OVERLAP_PATH.exists():
        raise FileNotFoundError(f"Overlap file not found: {OVERLAP_PATH}")

    ov = pd.read_parquet(OVERLAP_PATH)

    ov["start_rome"] = pd.to_datetime(ov["start_rome"], errors="coerce")
    ov["end_rome"] = pd.to_datetime(ov["end_rome"], errors="coerce")

    if ov["start_rome"].dt.tz is None:
        ov["start_rome"] = ov["start_rome"].dt.tz_localize("Europe/Rome").dt.tz_convert("UTC")
    else:
        ov["start_rome"] = ov["start_rome"].dt.tz_convert("UTC")

    if ov["end_rome"].dt.tz is None:
        ov["end_rome"] = ov["end_rome"].dt.tz_localize("Europe/Rome").dt.tz_convert("UTC")
    else:
        ov["end_rome"] = ov["end_rome"].dt.tz_convert("UTC")

    overlap_type = "pairwise" if symbol == "HSBC" else "triple"
    ov = ov[(ov["symbol"] == symbol) & (ov["overlap_type"] == overlap_type)].copy()
    ov["date_rome"] = pd.to_datetime(ov["date_rome"]).dt.date
    ov = ov.sort_values("date_rome").reset_index(drop=True)

    if not ov.empty:
        hours = (ov["end_rome"] - ov["start_rome"]).dt.total_seconds() / 3600.0
        print(
            f"[ENGINE] {symbol}: {overlap_type} overlap | "
            f"days={len(ov)} | avg_hours={hours.mean():.3f} | "
            f"min_hours={hours.min():.3f} | max_hours={hours.max():.3f}"
        )

    return ov


def _apply_overlap_filter(df: pd.DataFrame, overlaps: pd.DataFrame) -> pd.DataFrame:
    if df.empty or overlaps.empty:
        return pd.DataFrame(columns=df.columns)

    out = df.copy()
    out["date_rome"] = out["ts_event"].dt.tz_convert("Europe/Rome").dt.date

    merged = out.merge(
        overlaps[["date_rome", "start_rome", "end_rome"]],
        on="date_rome",
        how="inner",
    )

    mask = (
        (merged["ts_event"] >= merged["start_rome"]) &
        (merged["ts_event"] <= merged["end_rome"])
    )

    merged = merged[mask].copy()
    merged = merged.drop(columns=["date_rome", "start_rome", "end_rome"])
    merged = merged.sort_values(["market_key", "ts_event"]).reset_index(drop=True)
    return merged


def _ensure_eur_quotes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "bid_px_eur" not in out.columns:
        out["bid_px_eur"] = np.nan
    if "ask_px_eur" not in out.columns:
        out["ask_px_eur"] = np.nan

    missing_bid = out["bid_px_eur"].isna()
    missing_ask = out["ask_px_eur"].isna()

    usd_to_eur = SIMULATION_CONFIG["base_fx_to_eur"]["USD"]

    out.loc[missing_bid, "bid_px_eur"] = out.loc[missing_bid, "bid_px_00"] * usd_to_eur
    out.loc[missing_ask, "ask_px_eur"] = out.loc[missing_ask, "ask_px_00"] * usd_to_eur

    out["mid_px_eur"] = 0.5 * (out["bid_px_eur"] + out["ask_px_eur"])
    out["spread_eur"] = out["ask_px_eur"] - out["bid_px_eur"]

    return out


def _get_pairs_for_symbol(symbol: str, available_markets) -> list[tuple[str, str]]:
    markets = list(sorted(set(available_markets)))

    if symbol == "HSBC":
        desired = [("BASE_REAL", "LSE")]
    else:
        desired = [("BASE_REAL", "LSE"), ("BASE_REAL", "AMS"), ("LSE", "AMS")]

    return [pair for pair in desired if pair[0] in markets and pair[1] in markets]


def _rename_for_merge(df: pd.DataFrame, market_key: str) -> pd.DataFrame:
    cols = [
        "ts_event",
        "bid_px_00",
        "ask_px_00",
        "bid_sz_00",
        "ask_sz_00",
        "bid_px_eur",
        "ask_px_eur",
        "mid_px_eur",
        "spread_eur",
    ]
    out = df[cols].copy()
    rename_map = {c: f"{market_key.lower()}__{c}" for c in cols if c != "ts_event"}
    return out.rename(columns=rename_map)


def _synchronize_pair(df: pd.DataFrame, market_a: str, market_b: str, tolerance_ms: int) -> pd.DataFrame:
    a = df[df["market_key"] == market_a].copy().sort_values("ts_event")
    b = df[df["market_key"] == market_b].copy().sort_values("ts_event")

    if a.empty or b.empty:
        return pd.DataFrame()

    a = _rename_for_merge(a, market_a)
    b = _rename_for_merge(b, market_b)

    tol = pd.Timedelta(milliseconds=tolerance_ms)

    merged = pd.merge_asof(
        a.sort_values("ts_event"),
        b.sort_values("ts_event"),
        on="ts_event",
        direction="backward",
        tolerance=tol,
    )

    merged["pair"] = f"{market_a}__{market_b}"
    merged["market_a"] = market_a
    merged["market_b"] = market_b

    needed_cols = [
        f"{market_a.lower()}__bid_px_eur",
        f"{market_a.lower()}__ask_px_eur",
        f"{market_b.lower()}__bid_px_eur",
        f"{market_b.lower()}__ask_px_eur",
    ]
    merged = merged.dropna(subset=needed_cols).copy()
    return merged.reset_index(drop=True)


def _compute_pair_metrics(sync_df: pd.DataFrame, market_a: str, market_b: str) -> pd.DataFrame:
    out = sync_df.copy()

    a = market_a.lower()
    b = market_b.lower()

    out["gap_a_sell_b_buy_eur"] = out[f"{a}__bid_px_eur"] - out[f"{b}__ask_px_eur"]
    out["gap_b_sell_a_buy_eur"] = out[f"{b}__bid_px_eur"] - out[f"{a}__ask_px_eur"]

    out["qty_a_sell_b_buy"] = np.minimum(out[f"{a}__bid_sz_00"], out[f"{b}__ask_sz_00"])
    out["qty_b_sell_a_buy"] = np.minimum(out[f"{b}__bid_sz_00"], out[f"{a}__ask_sz_00"])

    out["gap_a_sell_b_buy_diff"] = out["gap_a_sell_b_buy_eur"].diff()
    out["gap_b_sell_a_buy_diff"] = out["gap_b_sell_a_buy_eur"].diff()

    roll_window = 200
    out["sigma_a_sell_b_buy"] = out["gap_a_sell_b_buy_diff"].rolling(roll_window, min_periods=20).std()
    out["sigma_b_sell_a_buy"] = out["gap_b_sell_a_buy_diff"].rolling(roll_window, min_periods=20).std()

    return out


def _dynamic_threshold(sigma_series: pd.Series) -> pd.Series:
    c_unit = SIMULATION_CONFIG["cost_unit_eur"]
    risk_lambda = SIMULATION_CONFIG["risk_lambda"]
    tau_ms = SIMULATION_CONFIG["tau_ms_for_risk"]
    fixed_buffer = SIMULATION_CONFIG["fixed_buffer_eur"]

    tau_sec = tau_ms / 1000.0
    sigma_safe = sigma_series.fillna(0.0)

    thr = c_unit + fixed_buffer + risk_lambda * sigma_safe * np.sqrt(tau_sec)
    return pd.Series(thr, index=sigma_series.index)


def _extract_windows_and_samples(df, symbol, pair, direction_label, gap_col, qty_col, sigma_col):
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    tmp = df[["ts_event", gap_col, qty_col, sigma_col, "market_a", "market_b", "pair"]].copy()
    tmp = tmp.rename(columns={gap_col: "gap_eur", qty_col: "book_qty", sigma_col: "sigma_local"})
    tmp["threshold_eur"] = _dynamic_threshold(tmp["sigma_local"])
    tmp["is_open"] = tmp["gap_eur"] > tmp["threshold_eur"]

    if tmp["is_open"].sum() == 0:
        return pd.DataFrame(), pd.DataFrame()

    tmp["block_start"] = tmp["is_open"] & (~tmp["is_open"].shift(1, fill_value=False))
    tmp["window_id"] = tmp["block_start"].cumsum()
    tmp = tmp[tmp["is_open"]].copy()

    tmp["symbol"] = symbol
    tmp["pair"] = pair
    tmp["direction"] = direction_label

    grouped = tmp.groupby("window_id", as_index=False).agg(
        t_open=("ts_event", "min"),
        t_close=("ts_event", "max"),
        n_obs=("ts_event", "size"),
        max_gap_eur=("gap_eur", "max"),
        mean_gap_eur=("gap_eur", "mean"),
        min_gap_eur=("gap_eur", "min"),
        threshold_open=("threshold_eur", "first"),
        threshold_mean=("threshold_eur", "mean"),
        book_qty_open=("book_qty", "first"),
        book_qty_mean=("book_qty", "mean"),
        sigma_open=("sigma_local", "first"),
        sigma_mean=("sigma_local", "mean"),
    )

    grouped["duration_ms"] = (grouped["t_close"] - grouped["t_open"]).dt.total_seconds() * 1000.0
    grouped["symbol"] = symbol
    grouped["pair"] = pair
    grouped["direction"] = direction_label

    grouped = grouped[
        (grouped["duration_ms"] >= SIMULATION_CONFIG["min_duration_ms"]) &
        (grouped["n_obs"] >= SIMULATION_CONFIG["min_obs_per_window"])
    ].copy()

    valid_ids = set(grouped["window_id"].tolist())
    samples = tmp[tmp["window_id"].isin(valid_ids)].copy()

    cols = [
        "window_id", "symbol", "pair", "direction", "t_open", "t_close", "duration_ms",
        "n_obs", "threshold_open", "threshold_mean", "min_gap_eur", "mean_gap_eur",
        "max_gap_eur", "book_qty_open", "book_qty_mean", "sigma_open", "sigma_mean",
    ]
    return grouped[cols].copy(), samples.reset_index(drop=True)


def _process_symbol(symbol: str):
    df = _load_cross_market(symbol)
    overlaps = _load_overlaps(symbol)

    rows_before = len(df)
    df = _apply_overlap_filter(df, overlaps)
    rows_after = len(df)

    print(f"[ENGINE] {symbol}: rows before overlap = {rows_before:,}, after overlap = {rows_after:,}")

    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = _ensure_eur_quotes(df)

    pairs = _get_pairs_for_symbol(symbol, df["market_key"].unique())
    all_windows = []
    all_samples = []

    tolerance_ms = SIMULATION_CONFIG["sync_tolerance_ms"]

    for market_a, market_b in pairs:
        sync_df = _synchronize_pair(df, market_a, market_b, tolerance_ms)
        if sync_df.empty:
            continue

        sync_df = _compute_pair_metrics(sync_df, market_a, market_b)
        pair = f"{market_a}__{market_b}"

        w1, s1 = _extract_windows_and_samples(
            sync_df, symbol, pair, f"SELL_{market_a}_BUY_{market_b}",
            "gap_a_sell_b_buy_eur", "qty_a_sell_b_buy", "sigma_a_sell_b_buy"
        )

        w2, s2 = _extract_windows_and_samples(
            sync_df, symbol, pair, f"SELL_{market_b}_BUY_{market_a}",
            "gap_b_sell_a_buy_eur", "qty_b_sell_a_buy", "sigma_b_sell_a_buy"
        )

        if not w1.empty:
            all_windows.append(w1)
            all_samples.append(s1)
        if not w2.empty:
            all_windows.append(w2)
            all_samples.append(s2)

    if not all_windows:
        return pd.DataFrame(), pd.DataFrame()

    windows = pd.concat(all_windows, ignore_index=True)
    windows = windows.sort_values(["t_open", "pair", "direction"]).reset_index(drop=True)

    samples = pd.concat(all_samples, ignore_index=True)
    samples = samples.sort_values(["ts_event", "pair", "direction", "window_id"]).reset_index(drop=True)

    return windows, samples


def build_opportunity_tables() -> None:
    for symbol in SYMBOL_CONFIG.keys():
        print(f"[ENGINE] Processing {symbol}...")
        opp, samples = _process_symbol(symbol)

        out_path = OUT_DIR / f"{symbol}_opportunities.parquet"
        sample_path = SAMPLES_DIR / f"{symbol}_opportunity_samples.parquet"

        if opp.empty:
            print(f"[ENGINE] No opportunities found for {symbol}. Saving empty files.")
            pd.DataFrame().to_parquet(out_path, index=False)
            pd.DataFrame().to_parquet(sample_path, index=False)
            continue

        opp.to_parquet(out_path, index=False)
        samples.to_parquet(sample_path, index=False)

        print(f"[ENGINE] Saved: {out_path}")
        print(f"[ENGINE] Saved: {sample_path}")
        print(
            f"[ENGINE] {symbol}: {len(opp):,} windows | "
            f"avg duration = {opp['duration_ms'].mean():.3f} ms | "
            f"avg max gap = {opp['max_gap_eur'].mean():.6f} EUR"
        )


if __name__ == "__main__":
    build_opportunity_tables()