# ===== FILTRO TEMPORALE — TAGLIA I DATI AGLI ORARI DI OVERLAP =====

# Questo script prende i dati grezzi processati (MBP e trades) e li
# riduce alla sola finestra temporale in cui i mercati rilevanti sono
# aperti contemporaneamente (overlap), calcolata da overlap_calendar.py.
#
# Perché serve: confrontare prezzi tra borse ha senso solo nelle ore in cui
# tutte le piazze coinvolte sono attive. Fuori dall'overlap i prezzi non sono
# comparabili e includere quei dati sporcherebbe l'analisi cross-market.
#
# Output per ogni titolo:
#   data/processed/{symbol}_mbp1_overlap.parquet    ← book filtrato
#   data/processed/{symbol}_trades_overlap.parquet  ← trades filtrate

import pandas as pd
from pathlib import Path


PROCESSED_DIR = Path("data/processed")


# ===== FUNZIONE: CARICA I DATI GREZZI E IL CALENDARIO DI OVERLAP =====

def load_data(symbol: str):
    # carica il book (MBP) e le trades già processate per il titolo
    mbp    = pd.read_parquet(PROCESSED_DIR / f"{symbol}_mbp1.parquet")
    trades = pd.read_parquet(PROCESSED_DIR / f"{symbol}_trades.parquet")

    # carica il calendario degli overlap prodotto da overlap_calendar.py:
    # contiene per ogni giorno e ogni titolo la finestra start_rome → end_rome
    overlaps = pd.read_parquet(PROCESSED_DIR / "market_overlaps.parquet")

    return mbp, trades, overlaps


# ===== FUNZIONE: APPLICA IL FILTRO DI OVERLAP A UN DATAFRAME =====

def filter_by_overlap(df: pd.DataFrame, overlap_df: pd.DataFrame) -> pd.DataFrame:
    # lavora su una copia per non modificare il DataFrame originale
    df = df.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"])

    # converte il timestamp in ora di Roma ed estrae solo la data (senza ora)
    # serve come chiave di join: accoppia ogni riga al suo overlap del giorno
    df["date_rome"] = df["ts_event"].dt.tz_convert("Europe/Rome").dt.date

    # merge "inner" tra i dati e il calendario di overlap sulla colonna date_rome:
    # ogni riga del DataFrame riceve i valori start_rome e end_rome del suo giorno
    # "inner" significa che vengono tenute solo le righe per cui esiste un overlap
    # (scarta automaticamente giorni festivi o fuori campione)
    merged = df.merge(
        overlap_df,
        on="date_rome",
        how="inner"
    )

    # filtro temporale: tieni solo le righe il cui timestamp cade
    # dentro la finestra di overlap del giorno
    # >= start_rome e <= end_rome → dentro la finestra
    # tutto il resto (prima dell'apertura o dopo la chiusura) viene scartato
    mask = (
        (merged["ts_event"] >= merged["start_rome"]) &
        (merged["ts_event"] <= merged["end_rome"])
    )

    return merged[mask].copy()


# ===== FUNZIONE: PIPELINE COMPLETA PER UN SINGOLO TITOLO =====

def process_symbol(symbol: str, overlap_type: str):
    # overlap_type distingue tra:
    #   "triple"   → UL e SHELL (AMS + LSE + US)
    #   "pairwise" → HSBC (LSE + US)
    # serve per selezionare la riga giusta dal calendario degli overlap
    print(f"\nProcessing {symbol}...")

    mbp, trades, overlaps = load_data(symbol)

    # filtra il calendario tenendo solo le righe del titolo e del tipo di overlap corretto
    overlaps_symbol = overlaps[
        (overlaps["symbol"] == symbol) &
        (overlaps["overlap_type"] == overlap_type)
    ]

    # applica il filtro temporale a MBP e trades
    mbp_f    = filter_by_overlap(mbp, overlaps_symbol)
    trades_f = filter_by_overlap(trades, overlaps_symbol)

    # stampa quante righe rimangono dopo il filtro (utile per verifica)
    print(f"{symbol} MBP rows: {len(mbp_f):,}")
    print(f"{symbol} Trades rows: {len(trades_f):,}")

    # salva i dati filtrati in nuovi file parquet separati da quelli grezzi
    # il suffisso "_overlap" indica che sono già tagliati alla finestra di overlap
    mbp_f.to_parquet(PROCESSED_DIR / f"{symbol}_mbp1_overlap.parquet", index=False)
    trades_f.to_parquet(PROCESSED_DIR / f"{symbol}_trades_overlap.parquet", index=False)


# ===== FUNZIONE PRINCIPALE =====

def main():
    # UL e SHELL → overlap triplo (AMS + LSE + US)
    # HSBC       → overlap pairwise (LSE + US), Hong Kong non si sovrappone con NY
    process_symbol("UL",    "triple")
    process_symbol("SHELL", "triple")
    process_symbol("HSBC",  "pairwise")


# vedi spiegazione di if __name__ == "__main__" in preprocessing_mbp1.py
if __name__ == "__main__":
    main()
