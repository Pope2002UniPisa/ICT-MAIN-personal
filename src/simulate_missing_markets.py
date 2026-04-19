from __future__ import annotations

from pathlib import Path
import numpy as np # NumPy è la libreria fondamentale per il calcolo numerico in Python, usata per array, operazioni matematiche, generazione di numeri casuali e molto altro.
import pandas as pd

from .config_simbols import SYMBOL_CONFIG, SIMULATION_CONFIG, BASE_DIR, SIM_DIR

def _load_base(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Priorità:
    1. usa i file *_overlap.parquet se esistono
    2. altrimenti usa i file completi standard

    In questo modo i mercati simulati nascono già all'interno
    dell'overlap corretto che hai costruito tu.
    """
    mbp_overlap_path = BASE_DIR / f"{symbol}_mbp1_overlap.parquet"
    trades_overlap_path = BASE_DIR / f"{symbol}_trades_overlap.parquet"

    mbp_full_path = BASE_DIR / f"{symbol}_mbp1.parquet"
    trades_full_path = BASE_DIR / f"{symbol}_trades.parquet"

    if mbp_overlap_path.exists() and trades_overlap_path.exists():
        mbp_path = mbp_overlap_path
        trades_path = trades_overlap_path
        print(f"[SIM] Using overlap-filtered base files for {symbol}.")
    else:
        mbp_path = mbp_full_path
        trades_path = trades_full_path
        print(f"[SIM] Using full base files for {symbol}.")

    if not mbp_path.exists():
        raise FileNotFoundError(f"Missing base MBP1 file: {mbp_path}")
    if not trades_path.exists():
        raise FileNotFoundError(f"Missing base trades file: {trades_path}")

    mbp = pd.read_parquet(mbp_path)
    trades = pd.read_parquet(trades_path)

    mbp["ts_event"] = pd.to_datetime(mbp["ts_event"], utc=True)
    trades["ts_event"] = pd.to_datetime(trades["ts_event"], utc=True)

    mbp = mbp.sort_values("ts_event").reset_index(drop=True)
    trades = trades.sort_values("ts_event").reset_index(drop=True)

    return mbp, trades


def _ensure_columns_mbp(mbp: pd.DataFrame) -> pd.DataFrame:
    required = ["ts_event", "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00"]
    missing = [c for c in required if c not in mbp.columns]
    if missing:
        raise ValueError(f"Missing MBP1 columns: {missing}")
    return mbp.copy()


def _ensure_columns_trades(trades: pd.DataFrame) -> pd.DataFrame:
    required = ["ts_event", "price", "size"]
    missing = [c for c in required if c not in trades.columns]
    if missing:
        raise ValueError(f"Missing trades columns: {missing}")
    return trades.copy()


def _base_to_eur(series: pd.Series, ccy: str) -> pd.Series:
    fx = SIMULATION_CONFIG["base_fx_to_eur"][ccy]
    return series * fx


def _eur_to_local(series: pd.Series, fx_to_eur: float) -> pd.Series:
    return series / fx_to_eur


def _generate_persistent_windows(
    ts: pd.Series,
    prob: float,
    bps_min: float,
    bps_max: float,
    ms_min: int,
    ms_max: int,
    rng: np.random.Generator,
) -> pd.Series:
    n = len(ts)
    effect = np.zeros(n, dtype=float)

    # Pre-campiona i seed: O(n) una sola volta, poi solo ~prob*n iterazioni.
    # Per ogni seed usa searchsorted → O(log n) invece di O(n) per window.
    ts_arr = ts.to_numpy(dtype="datetime64[ns]")
    seed_indices = np.where(rng.random(n) < prob)[0]

    for idx in seed_indices:
        shock_bps = rng.uniform(bps_min, bps_max) * rng.choice([-1.0, 1.0])
        duration_ms = int(rng.integers(ms_min, ms_max + 1))
        t1 = ts_arr[idx] + np.timedelta64(duration_ms, "ms")
        end_idx = int(np.searchsorted(ts_arr, t1, side="right"))
        effect[idx:end_idx] += shock_bps / 10_000.0

    return pd.Series(effect, index=ts.index)


def _simulate_mbp_for_venue(
    base_mbp: pd.DataFrame,
    base_ccy: str,
    venue: str,
    venue_ccy: str,
    venue_fx_to_eur: float,
    rng: np.random.Generator,
) -> pd.DataFrame:
    mbp = _ensure_columns_mbp(base_mbp)

    base_bid_eur = _base_to_eur(mbp["bid_px_00"], base_ccy)
    base_ask_eur = _base_to_eur(mbp["ask_px_00"], base_ccy)

    base_mid_eur = 0.5 * (base_bid_eur + base_ask_eur)
    base_spread_eur = (base_ask_eur - base_bid_eur).clip(lower=1e-8)

    noise_bps_std = SIMULATION_CONFIG["price_noise_bps_std"]
    spread_mult_mean = SIMULATION_CONFIG["spread_multiplier_mean"]
    spread_mult_std = SIMULATION_CONFIG["spread_multiplier_std"]
    size_mult_mean = SIMULATION_CONFIG["size_multiplier_mean"]
    size_mult_std = SIMULATION_CONFIG["size_multiplier_std"]

    micro_noise = rng.normal(0.0, noise_bps_std / 10000.0, len(mbp))
    spread_mult = np.clip(
        rng.normal(spread_mult_mean, spread_mult_std, len(mbp)),
        0.7,
        1.6,
    )
    size_mult_bid = np.clip(
        rng.normal(size_mult_mean, size_mult_std, len(mbp)),
        0.2,
        2.5,
    )
    size_mult_ask = np.clip(
        rng.normal(size_mult_mean, size_mult_std, len(mbp)),
        0.2,
        2.5,
    )

    arb_effect = _generate_persistent_windows(
        ts=mbp["ts_event"],
        prob=SIMULATION_CONFIG["arb_window_prob"],
        bps_min=SIMULATION_CONFIG["arb_window_bps_min"],
        bps_max=SIMULATION_CONFIG["arb_window_bps_max"],
        ms_min=SIMULATION_CONFIG["arb_window_ms_min"],
        ms_max=SIMULATION_CONFIG["arb_window_ms_max"],
        rng=rng,
    )

    syn_mid_eur = base_mid_eur * (1.0 + micro_noise + arb_effect)
    syn_spread_eur = base_spread_eur * spread_mult

    syn_bid_eur = syn_mid_eur - 0.5 * syn_spread_eur
    syn_ask_eur = syn_mid_eur + 0.5 * syn_spread_eur

    out = mbp.copy()
    out["venue"] = venue
    out["ccy"] = venue_ccy
    out["fx_to_eur"] = venue_fx_to_eur

    out["bid_px_eur"] = syn_bid_eur
    out["ask_px_eur"] = syn_ask_eur

    out["bid_px_00"] = _eur_to_local(syn_bid_eur, venue_fx_to_eur)
    out["ask_px_00"] = _eur_to_local(syn_ask_eur, venue_fx_to_eur)

    out["bid_sz_00"] = np.maximum(1, np.round(out["bid_sz_00"] * size_mult_bid)).astype(int)
    out["ask_sz_00"] = np.maximum(1, np.round(out["ask_sz_00"] * size_mult_ask)).astype(int)

    # Ricalcola mid e spread coerenti con bid/ask simulati
    out["spread"] = (out["ask_px_00"] - out["bid_px_00"]).clip(lower=0.0)
    out["mid"] = 0.5 * (out["bid_px_00"] + out["ask_px_00"])

    out["simulated"] = True
    return out


def _simulate_trades_for_venue(
    base_trades: pd.DataFrame,
    base_ccy: str,
    venue: str,
    venue_ccy: str,
    venue_fx_to_eur: float,
    rng: np.random.Generator,
) -> pd.DataFrame:
    trades = _ensure_columns_trades(base_trades)

    base_price_eur = _base_to_eur(trades["price"], base_ccy)
    micro_noise = rng.normal(0.0, SIMULATION_CONFIG["price_noise_bps_std"] / 10000.0, len(trades))

    syn_price_eur = base_price_eur * (1.0 + micro_noise)

    out = trades.copy()
    out["venue"] = venue
    out["ccy"] = venue_ccy
    out["fx_to_eur"] = venue_fx_to_eur
    out["price_eur"] = syn_price_eur
    out["price"] = _eur_to_local(syn_price_eur, venue_fx_to_eur)

    if "size" in out.columns:
        size_mult = np.clip(
            rng.normal(
                SIMULATION_CONFIG["size_multiplier_mean"],
                SIMULATION_CONFIG["size_multiplier_std"],
                len(out),
            ),
            0.2,
            2.5,
        )
        out["size"] = np.maximum(1, np.round(out["size"] * size_mult)).astype(int)

    out["simulated"] = True
    return out


def _build_hk_close_sentiment_proxy(base_mbp: pd.DataFrame, symbol: str) -> pd.DataFrame:
    mbp = _ensure_columns_mbp(base_mbp).copy()

    mbp["mid"] = 0.5 * (mbp["bid_px_00"] + mbp["ask_px_00"])
    daily = (
        mbp.set_index("ts_event")["mid"]
        .resample("1D")
        .last()
        .dropna()
        .to_frame("close_proxy")
    )

    daily["ret"] = daily["close_proxy"].pct_change()
    daily["hk_close_sentiment_proxy"] = (
        daily["ret"].ewm(span=5, adjust=False).mean().fillna(0.0)
    )

    daily["symbol"] = symbol
    daily = daily.reset_index()
    return daily


def simulate_missing_markets(seed: int = 42) -> None:
    rng = np.random.default_rng(seed)

    for symbol, meta in SYMBOL_CONFIG.items():
        base_symbol = meta["base_symbol"]
        base_ccy = meta["base_ccy"]

        print(f"[SIM] Loading base data for {symbol}...")
        base_mbp, base_trades = _load_base(base_symbol)

        for venue, vmeta in meta.get("simulate_venues", {}).items():
            print(f"[SIM] Creating synthetic venue {venue} for {symbol}...")

            mbp_syn = _simulate_mbp_for_venue(
                base_mbp=base_mbp,
                base_ccy=base_ccy,
                venue=venue,
                venue_ccy=vmeta["ccy"],
                venue_fx_to_eur=vmeta["fx_to_eur"],
                rng=rng,
            )

            trades_syn = _simulate_trades_for_venue(
                base_trades=base_trades,
                base_ccy=base_ccy,
                venue=venue,
                venue_ccy=vmeta["ccy"],
                venue_fx_to_eur=vmeta["fx_to_eur"],
                rng=rng,
            )

            mbp_out = SIM_DIR / f"{symbol}_{venue}_mbp1_sim.parquet"
            trades_out = SIM_DIR / f"{symbol}_{venue}_trades_sim.parquet"

            mbp_syn.to_parquet(mbp_out, index=False)
            trades_syn.to_parquet(trades_out, index=False)

        if meta.get("use_hk_close_sentiment_proxy", False):
            print(f"[SIM] Creating HK close sentiment proxy for {symbol}...")
            hk_sent = _build_hk_close_sentiment_proxy(base_mbp, symbol)
            hk_out = SIM_DIR / f"{symbol}_hk_close_sentiment_proxy.parquet"
            hk_sent.to_parquet(hk_out, index=False)

    print("[SIM] Done.")

# python -m src.simulate_missing_markets

# python src/open_parquet_preview.py data/processed/simulated/UL_AMS_mbp1_sim.parquet
# python src/open_parquet_preview.py data/processed/simulated/UL_AMS_trades_sim.parquet
# python src/open_parquet_preview.py data/processed/simulated/UL_LSE_mbp1_sim.parquet
# python src/open_parquet_preview.py data/processed/simulated/UL_LSE_trades_sim.parquet

# python src/open_parquet_preview.py data/processed/simulated/SHELL_AMS_mbp1_sim.parquet
# python src/open_parquet_preview.py data/processed/simulated/SHELL_AMS_trades_sim.parquet
# python src/open_parquet_preview.py data/processed/simulated/SHELL_LSE_mbp1_sim.parquet
# python src/open_parquet_preview.py data/processed/simulated/SHELL_LSE_trades_sim.parquet

# python src/open_parquet_preview.py data/processed/simulated/HSBC_LSE_mbp1_sim.parquet
# python src/open_parquet_preview.py data/processed/simulated/HSBC_LSE_trades_sim.parquet