from __future__ import annotations

from pathlib import Path
import pandas as pd

# Esistenza file: 

FILES = {
    "UL_mbp1": Path("data/processed/UL_mbp1.parquet"),
    "UL_trades": Path("data/processed/UL_trades.parquet"),
    "SHELL_mbp1": Path("data/processed/SHELL_mbp1.parquet"),
    "SHELL_trades": Path("data/processed/SHELL_trades.parquet"),
    "HSBC_mbp1": Path("data/processed/HSBC_mbp1.parquet"),
    "HSBC_trades": Path("data/processed/HSBC_trades.parquet"),
}


def summarize_file(label: str, path: Path) -> None:
    print("\n" + "=" * 80)
    print(f"{label}")
    print(f"Path: {path}")

    if not path.exists():
        print("STATUS: MISSING FILE")
        return

    df = pd.read_parquet(path)
# Shape e colonne: cos'è lo shape? Lo shape è una tupla che indica il numero di righe e colonne del DataFrame. Ad esempio, se df.shape restituisce (1000, 10), significa che il DataFrame ha 1000 righe e 10 colonne. Le colonne sono i nomi delle variabili o campi presenti nel DataFrame, che possono essere 
    print(f"Shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")

    if len(df) == 0:
        print("STATUS: EMPTY DATAFRAME")
        return
# Primi 3 record:
    print("\nHead:")
    print(df.head(3))
# Range e nulls di ts_event:
    if "ts_event" in df.columns:
        ts = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
        print("\nts_event summary:")
        print(f"  min: {ts.min()}")
        print(f"  max: {ts.max()}")
        print(f"  nulls: {ts.isna().sum():,}")
# Righe per event_date:
    if "event_date" in df.columns:
        print("\nRows per event_date:")
        print(df.groupby("event_date").size())
# Statistiche di bid/ask/spread/mid_price per mbp1 (quindi bid, ask, spread e mid_price):
    if label.endswith("mbp1"):
        for col in ["bid_px_00", "ask_px_00", "spread", "mid"]:
            if col in df.columns:
                print(f"\n{col} summary:")
                print(df[col].describe())

        if "spread" in df.columns:
            neg_spread = (df["spread"] < 0).sum()
            zero_spread = (df["spread"] == 0).sum()
            print(f"\nNegative spread rows: {neg_spread:,}")
            print(f"Zero spread rows: {zero_spread:,}")
# Statistiche di price/size per trades:
    if label.endswith("trades"):
        for col in ["price", "size"]:
            if col in df.columns:
                print(f"\n{col} summary:")
                print(df[col].describe())
# Presenza e coerenza della colonna symbol, con conteggio per simbolo:
    if "symbol" in df.columns:
        print("\nSymbol counts:")
        print(df["symbol"].value_counts(dropna=False))


def main() -> None:
    for label, path in FILES.items():
        summarize_file(label, path)


if __name__ == "__main__":
    main()

# Analisi risposte terminale:
# - file parquet presenti e corretti (no file vuoti)
# - conversione corretta in datetime di ts_event in UTC (no nulls dopo conversione)
# range 10 marzo - 10 aprile
# - presenza di event_date con distribuzione coerente (10 giorni di trading, 20-30k righe al giorno)
# - per mbp1: bid/ask/spread/mid_price con valori ragione
# - spread positivo, mid_price tra bid e ask

# N.B. ZERO SPREAD NEGATIVI
# è importante che lo spread sia non negativo perché altrimenti vorrebbe dire che
# il prezzo di vendita (ask) è inferiore al prezzo di acquisto (bid), il che è controintuitivo e potrebbe indicare un errore nei dati.
# Dato che spread = ask-bid

# - mid = (ask+bid)/2
# - per trades colonne corrette con valori ragionevoli (price > 0, size > 0)
# - presenza colonna symbol con valori coerenti (UL, SHELL, HSBC)
