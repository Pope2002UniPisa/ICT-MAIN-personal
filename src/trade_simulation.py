# Simula l'esecuzione di trade sulle finestre di arbitraggio identificate dal motore.
# Per ogni simbolo in SYMBOL_CONFIG:
#   1. carica i campioni grezzi delle finestre (output di cross_market_engine.py)
#   2. per ogni finestra, ricampiona i tick ogni 10 ms (step_ms) → simula decisioni periodiche
#   3. per ogni step calcola profitto lordo e netto tenendo conto di costi e rischio
#   4. aggrega per finestra in un riepilogo (summary)
# Output:
#   data/processed/simulated_trades/{symbol}_trade_blocks.parquet  → un blocco per ogni step da 10ms
#   data/processed/simulated_trades/{symbol}_trade_summary.parquet → un riepilogo per finestra

from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd
from .config_simbols import BASE_DIR, SYMBOL_CONFIG, SIMULATION_CONFIG


SAMPLES_DIR = BASE_DIR / "opportunity_samples"
OUT_DIR = BASE_DIR / "simulated_trades"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ===== FUNZIONE: CARICA I CAMPIONI DI OPPORTUNITÀ PER UN SIMBOLO =====

def _load_samples(symbol: str) -> pd.DataFrame:
    # i campioni sono i tick grezzi dentro le finestre di arbitraggio (prodotti dal motore)
    path = SAMPLES_DIR / f"{symbol}_opportunity_samples.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Opportunity samples file not found: {path}")

    df = pd.read_parquet(path)
    if df.empty:
        return df

    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts_event"]).copy()
    # ordina per finestra e poi per tempo → garantisce coerenza nel ricampionamento successivo
    return df.sort_values(["window_id", "ts_event"]).reset_index(drop=True)


# ===== FUNZIONE: RICAMPIONA UNA FINESTRA A PASSI FISSI DI N MILLISECONDI =====

def _resample_window_every_10ms(window_df: pd.DataFrame, step_ms: int) -> pd.DataFrame:
    # i tick reali dentro una finestra non sono a intervalli regolari (arrivano quando cambia il book)
    # questa funzione crea una griglia temporale a passi fissi (es. ogni 10ms) e per ogni punto
    # cerca l'ultimo tick disponibile entro step_ms prima → simula una decisione periodica
    if window_df.empty:
        return pd.DataFrame()

    start = window_df["ts_event"].min()
    end = window_df["ts_event"].max()

    if pd.isna(start) or pd.isna(end) or end < start:
        return pd.DataFrame()

    # costruisce la griglia temporale da start a end con passo step_ms
    sample_times = pd.date_range(start=start, end=end, freq=f"{step_ms}ms", tz="UTC")
    if len(sample_times) == 0:
        # finestra più corta di step_ms → un solo punto al momento di apertura
        sample_times = pd.DatetimeIndex([start])

    target = pd.DataFrame({"ts_event": sample_times})

    # merge_asof backward: per ogni punto della griglia prende l'ultimo tick reale
    # avvenuto entro step_ms prima → se non c'è nessun tick entro la tolleranza, NaN
    res = pd.merge_asof(
        target.sort_values("ts_event"),
        window_df.sort_values("ts_event"),
        on="ts_event",
        direction="backward",
        tolerance=pd.Timedelta(milliseconds=step_ms),
    )

    # scarta i punti griglia privi di un tick valido (nessuna informazione disponibile)
    res = res.dropna(subset=["gap_eur", "threshold_eur"]).copy()
    return res.reset_index(drop=True)


# ===== FUNZIONE: CALCOLA IL BUFFER DI RISCHIO PER UNITÀ DI AZIONE =====

def _risk_buffer(sigma: pd.Series) -> pd.Series:
    # stima il costo del rischio di tenere una posizione aperta per tau millisecondi:
    # risk_lambda × sigma × sqrt(tau_sec)
    # sigma misura quanto può muoversi il gap in un tick → sqrt(tau) lo scala all'orizzonte tau
    risk_lambda = SIMULATION_CONFIG["risk_lambda"]
    tau_ms = SIMULATION_CONFIG["tau_ms_for_risk"]
    tau_sec = tau_ms / 1000.0
    sigma_safe = sigma.fillna(0.0)
    return risk_lambda * sigma_safe * np.sqrt(tau_sec)


# ===== FUNZIONE PRINCIPALE — SIMULA I TRADE PER TUTTI I SIMBOLI =====

def simulate_trading() -> None:
    trade_block_size = SIMULATION_CONFIG["trade_block_size"]   # numero di azioni per blocco (es. 1000)
    trade_step_ms = SIMULATION_CONFIG["trade_step_ms"]         # cadenza delle decisioni in ms (es. 10)
    cost_unit_eur = SIMULATION_CONFIG["cost_unit_eur"]         # costo per azione eseguita
    fixed_buffer_eur = SIMULATION_CONFIG["fixed_buffer_eur"]   # margine fisso minimo richiesto

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

        # itera su ogni finestra di arbitraggio separatamente
        for window_id, wdf in samples.groupby("window_id", sort=True):
            wdf = wdf.sort_values("ts_event").reset_index(drop=True)

            # step 1: ricampiona la finestra a intervalli fissi di trade_step_ms
            sampled = _resample_window_every_10ms(wdf, trade_step_ms)
            if sampled.empty:
                continue

            # step 2: aggiunge le variabili di simulazione a ogni punto della griglia
            sampled["trade_block_size"] = trade_block_size
            sampled["risk_buffer_unit_eur"] = _risk_buffer(sampled["sigma_local"])
            sampled["cost_unit_eur"] = cost_unit_eur
            sampled["fixed_buffer_eur"] = fixed_buffer_eur

            # step 3: calcola profitto lordo e netto per ogni blocco da 1000 azioni
            # profitto lordo = gap × quantità (quante unità guadagno se compro su B e vendo su A)
            sampled["gross_profit_eur"] = trade_block_size * sampled["gap_eur"]
            # profitto netto = lordo − costi di transazione − buffer di rischio − buffer fisso
            sampled["net_profit_eur"] = (
                trade_block_size * sampled["gap_eur"]
                - trade_block_size * sampled["cost_unit_eur"]
                - trade_block_size * sampled["risk_buffer_unit_eur"]
                - sampled["fixed_buffer_eur"]
            )

            # step 4: aggiunge le chiavi identificative della finestra
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

        # unisce tutti i blocchi di tutte le finestre in un unico DataFrame
        trades = pd.concat(blocks, ignore_index=True)
        trades = trades.sort_values(["window_id", "ts_event"]).reset_index(drop=True)

        # aggrega per finestra: somma profitti, conta blocchi, calcola statistiche sul gap
        summary = trades.groupby(["window_id", "symbol", "pair", "direction"], as_index=False).agg(
            n_blocks=("ts_event", "size"),                          # quanti step da 10ms dentro la finestra
            total_qty=("trade_block_size", "sum"),                  # azioni totali trattate
            gross_profit_total_eur=("gross_profit_eur", "sum"),     # profitto lordo cumulato
            net_profit_total_eur=("net_profit_eur", "sum"),         # profitto netto cumulato
            avg_gap_eur=("gap_eur", "mean"),                        # gap medio nella finestra
            max_gap_eur=("gap_eur", "max"),                         # gap massimo nella finestra
            avg_sigma_local=("sigma_local", "mean"),                # volatilità media locale
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

# python src/open_parquet_preview.py data/processed/simulated_trades/UL_trade_blocks.parquet
# python src/open_parquet_preview.py data/processed/simulated_trades/UL_trade_summary.parquet
# python src/open_parquet_preview.py data/processed/simulated_trades/SHELL_trade_blocks.parquet
# python src/open_parquet_preview.py data/processed/simulated_trades/SHELL_trade_summary.parquet
# python src/open_parquet_preview.py data/processed/simulated_trades/HSBC_trade_blocks.parquet
# python src/open_parquet_preview.py data/processed/simulated_trades/HSBC_trade_summary.parquet