# Prepara gli input per il motore di analisi cross-market.
# Per ogni simbolo in SYMBOL_CONFIG:
#   - carica i dati reali MBP1 dal mercato base
#   - carica i dati simulati MBP1 per ogni venue simulata
#   - li unisce in un unico parquet ordinato per timestamp
# Output: data/cross_market/{symbol}_cross_market_mbp1.parquet

# per il momento è sufficiente un analisi di mbp1 (best bid/offer) per identificare le opportunità 
# di arbitraggio cross-market, ma in futuro si potrebbe estendere a livelli più profondi o ad altri 
# tipi di dati (es. trades). 

# per il momento come si vede nel Quantifying the high-frequency trading “arms race” 
# - Matteo Aquilina, Eric Budish and Peter O’Neill, i dati di quote standard, mbp1 sono insufficienti
# per misurare correttamente il latency arbitrage, in quanto non mostrano le informazioni di 
# chi abbia effettivamente "perso" le gare quindi chi ha fallito gli IOC o chi ha cancellato l'ordine.

# Per uno studio più approfondito sono necessari i message data dell'exchange.
# quindi per ora individuo solo delle opportunità potenziali di arbitraggio, ma definire 
# la race con vincitori e perdenti non è possibile perché non ho i messaggi falliti
# Quindi LIMITE DEL DATASET 

from __future__ import annotations
from pathlib import Path
import pandas as pd
from .config_simbols import SYMBOL_CONFIG, BASE_DIR, SIM_DIR, SIMULATION_CONFIG

OUT_DIR = BASE_DIR / "cross_market"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ===== FUNZIONE: CARICA I DATI REALI MBP1 PER UN SIMBOLO =====

def _load_real_mbp(symbol: str) -> pd.DataFrame:
    path = BASE_DIR / f"{symbol}_mbp1.parquet"
    df = pd.read_parquet(path)
    # utc=True garantisce che i timestamp siano timezone-aware → confrontabili con i simulati
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    # "BASE_REAL" identifica il mercato di riferimento nel merge successivo
    df["venue"] = "BASE_REAL"
    df["simulated"] = False
    return df.sort_values("ts_event").reset_index(drop=True)


# ===== FUNZIONE: CARICA I DATI SIMULATI MBP1 PER UN SIMBOLO E UNA VENUE =====

def _load_sim_mbp(symbol: str, venue: str) -> pd.DataFrame:
    path = SIM_DIR / f"{symbol}_{venue}_mbp1_sim.parquet"
    df = pd.read_parquet(path)
    # stesso trattamento dei timestamp reali per garantire coerenza nel merge
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    return df.sort_values("ts_event").reset_index(drop=True)


# ===== FUNZIONE PRINCIPALE — COSTRUISCE I PARQUET CROSS-MARKET =====

def prepare_cross_market_inputs() -> None:
    # soglia di tolleranza temporale usata poi dal motore di sincronizzazione
    tolerance = pd.Timedelta(milliseconds=SIMULATION_CONFIG["sync_tolerance_ms"])

    for symbol, meta in SYMBOL_CONFIG.items():
        real_df = _load_real_mbp(symbol)

        # inizializza la lista con il frame reale; market_key identifica la sorgente
        venue_frames = [real_df[["ts_event", "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00", "venue", "simulated"]].copy()]
        venue_frames[0]["market_key"] = "BASE_REAL"

        for venue in meta.get("simulate_venues", {}):
            sim_df = _load_sim_mbp(symbol, venue)
            # bid_px_eur / ask_px_eur sono presenti solo nei simulati (prezzi convertiti in EUR)
            sim_df = sim_df[["ts_event", "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00", "venue", "simulated", "bid_px_eur", "ask_px_eur"]].copy()
            sim_df["market_key"] = venue
            venue_frames.append(sim_df)

        # concat verticale: tutte le venue affiancate su un unico asse temporale
        merged = pd.concat(venue_frames, axis=0, ignore_index=True)
        # ordine doppio: prima per tempo, poi per venue → determinismo in caso di timestamp identici
        merged = merged.sort_values(["ts_event", "market_key"]).reset_index(drop=True)

        out_path = OUT_DIR / f"{symbol}_cross_market_mbp1.parquet"
        merged.to_parquet(out_path, index=False)

        print(f"[XMARKET] Saved {out_path}")


if __name__ == "__main__":
    prepare_cross_market_inputs()

# python src/open_parquet_preview.py data/processed/cross_market/UL_cross_market_mbp1.parquet
# python src/open_parquet_preview.py data/processed/cross_market/SHELL_cross_market_mbp1.parquet
# python src/open_parquet_preview.py data/processed/cross_market/HSBC_cross_market_mbp1.parquet