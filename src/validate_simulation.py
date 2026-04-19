# Verifica che i dati simulati siano statisticamente coerenti con i dati reali.
# Per ogni simbolo confronta le proprietà del book (mid, spread, dimensioni) tra:
#   - BASE_REAL: dati reali del mercato base (US)
#   - SIM_{venue}: dati simulati per ogni venue (LSE, AMS)
# Se mid_mean, spread_mean e ret_std dei simulati sono vicini al reale,
# la simulazione è considerata valida per l'analisi cross-market.
# Output: data/processed/simulation_validation/simulation_validation_summary.csv

from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

from .config_simbols import BASE_DIR, SIM_DIR, SYMBOL_CONFIG


OUT_DIR = BASE_DIR / "simulation_validation"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ===== FUNZIONE: CARICA I DATI REALI MBP1 PER UN SIMBOLO =====

def _load_base_mbp(symbol: str) -> pd.DataFrame:
    path = BASE_DIR / f"{symbol}_mbp1.parquet"
    df = pd.read_parquet(path)
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts_event"]).copy()
    return df.sort_values("ts_event").reset_index(drop=True)


# ===== FUNZIONE: CARICA I DATI SIMULATI MBP1 PER UN SIMBOLO E UNA VENUE =====

def _load_sim_mbp(symbol: str, venue: str) -> pd.DataFrame:
    path = SIM_DIR / f"{symbol}_{venue}_mbp1_sim.parquet"
    df = pd.read_parquet(path)
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts_event"]).copy()
    return df.sort_values("ts_event").reset_index(drop=True)


# ===== FUNZIONE: CALCOLA LE STATISTICHE RIASSUNTIVE DEL BOOK =====

def _summarize_mbp(df: pd.DataFrame, label: str) -> dict:
    # mid = punto medio tra bid e ask → stima del prezzo "equo" in ogni istante
    mid = 0.5 * (df["bid_px_00"] + df["ask_px_00"])
    # spread = differenza tra ask e bid → costo implicito di eseguire un trade immediato
    spread = df["ask_px_00"] - df["bid_px_00"]
    # variazione percentuale del mid tra un tick e il successivo → proxy della volatilità tick-by-tick
    ret = mid.pct_change()

    return {
        "label": label,
        "rows": len(df),                        # numero totale di aggiornamenti del book
        "mid_mean": float(mid.mean()),          # prezzo medio: deve essere coerente tra reale e simulato
        "mid_std": float(mid.std()),            # dispersione del prezzo: misura l'ampiezza delle oscillazioni
        "ret_std": float(ret.std()),            # volatilità dei rendimenti tick-by-tick
        "spread_mean": float(spread.mean()),    # spread medio: nei simulati dovrebbe essere ~5% più largo (spread_multiplier_mean=1.05)
        "spread_std": float(spread.std()),      # variabilità dello spread
        "bid_sz_mean": float(df["bid_sz_00"].mean()),   # dimensione media del best bid
        "ask_sz_mean": float(df["ask_sz_00"].mean()),   # dimensione media del best ask
    }


# ===== FUNZIONE PRINCIPALE — CONFRONTA REALE VS SIMULATO PER TUTTI I SIMBOLI =====

def validate_simulation() -> None:
    rows = []

    for symbol, meta in SYMBOL_CONFIG.items():
        # carica e riassume il mercato reale (riferimento)
        base = _load_base_mbp(symbol)
        rows.append({"symbol": symbol, **_summarize_mbp(base, "BASE_REAL")})

        # carica e riassume ogni venue simulata → affianca le statistiche al reale
        for venue in meta.get("simulate_venues", {}):
            sim = _load_sim_mbp(symbol, venue)
            rows.append({"symbol": symbol, **_summarize_mbp(sim, f"SIM_{venue}")})

    # una riga per ogni combinazione simbolo × venue: facilita il confronto visivo
    out = pd.DataFrame(rows)
    out_path = OUT_DIR / "simulation_validation_summary.csv"
    out.to_csv(out_path, index=False)

    print(f"[VALIDATION] Saved: {out_path}")
    print(out)


if __name__ == "__main__":
    validate_simulation()


# python -m src.validate_simulation
# si crea il file csv in data/processed/simulation_validation/...