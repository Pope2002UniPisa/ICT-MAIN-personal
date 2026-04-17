from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
BASE_DIR = DATA_DIR / "processed"
SIM_DIR = BASE_DIR / "simulated"

SIM_DIR.mkdir(parents=True, exist_ok=True)

SYMBOL_CONFIG = {
    "UL": {
        "base_symbol": "UL",
        "base_ccy": "USD",
        "base_venue": "US",
        "simulate_venues": {
            "LSE": {"ccy": "GBP", "fx_to_eur": 1.17},
            "AMS": {"ccy": "EUR", "fx_to_eur": 1.00},
        },
    },
    "SHELL": {
        "base_symbol": "SHELL",
        "base_ccy": "USD",
        "base_venue": "US",
        "simulate_venues": {
            "LSE": {"ccy": "GBP", "fx_to_eur": 1.17},
            "AMS": {"ccy": "EUR", "fx_to_eur": 1.00},
        },
    },
    "HSBC": {
        "base_symbol": "HSBC",
        "base_ccy": "USD",
        "base_venue": "US_ADR",
        "simulate_venues": {
            "LSE": {"ccy": "GBP", "fx_to_eur": 1.17},
        },
        "use_hk_close_sentiment_proxy": True,
    },
}

SIMULATION_CONFIG = {
    "sync_tolerance_ms": 10,
    "max_hold_ms": 10,
    "base_fx_to_eur": {
        "USD": 0.92,
        "GBP": 1.17,
        "EUR": 1.00,
        "HKD": 0.118,
    },

    "price_noise_bps_std": 1.5,
    "spread_multiplier_mean": 1.10,
    "spread_multiplier_std": 0.08,
    "size_multiplier_mean": 0.95,
    "size_multiplier_std": 0.20,
    "arb_window_prob": 0.0008,
    "arb_window_bps_min": 3.0,
    "arb_window_bps_max": 12.0,
    "arb_window_ms_min": 1,
    "arb_window_ms_max": 10,
    "sentiment_strength": 0.15,

    "risk_lambda": 4.0,
    "tau_ms_for_risk": 10,
    "cost_unit_eur": 0.0015,
    "fixed_buffer_eur": 0.0010,
    "min_duration_ms": 1.0,
    "min_obs_per_window": 2,

    "trade_block_size": 1000,
    "trade_step_ms": 10,
}