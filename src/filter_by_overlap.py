

# Abbiamo definito il filtro temporale e garantito che i dati di mbp1 e trades siano allineati agli overlap di mercato.
# Il passo successivo è applicare questo filtro ai dati e salvare i risultati filtrati in nuovi file parquet.
# Questi file filtrati saranno poi usati per l'analisi della performance, assicurandoci di confrontare solo i periodi in cui i mercati erano effettivamente aperti e sovrapposti.
# Il codice qui sotto implementa questa logica, caricando i dati, applicando il filtro di overlap e salvando i risultati filtrati.


import pandas as pd
from pathlib import Path


PROCESSED_DIR = Path("data/processed")


def load_data(symbol: str):
    mbp = pd.read_parquet(PROCESSED_DIR / f"{symbol}_mbp1.parquet")
    trades = pd.read_parquet(PROCESSED_DIR / f"{symbol}_trades.parquet")

    overlaps = pd.read_parquet(PROCESSED_DIR / "market_overlaps.parquet")

    return mbp, trades, overlaps


def filter_by_overlap(df: pd.DataFrame, overlap_df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"])

    # merge per data
    df["date_rome"] = df["ts_event"].dt.tz_convert("Europe/Rome").dt.date

    merged = df.merge(
        overlap_df,
        on="date_rome",
        how="inner"
    )

    # filtro temporale
    mask = (
        (merged["ts_event"] >= merged["start_rome"]) &
        (merged["ts_event"] <= merged["end_rome"])
    )

    return merged[mask].copy()


def process_symbol(symbol: str, overlap_type: str):
    print(f"\nProcessing {symbol}...")

    mbp, trades, overlaps = load_data(symbol)

    overlaps_symbol = overlaps[
        (overlaps["symbol"] == symbol) &
        (overlaps["overlap_type"] == overlap_type)
    ]

    mbp_f = filter_by_overlap(mbp, overlaps_symbol)
    trades_f = filter_by_overlap(trades, overlaps_symbol)

    print(f"{symbol} MBP rows: {len(mbp_f):,}")
    print(f"{symbol} Trades rows: {len(trades_f):,}")

    mbp_f.to_parquet(PROCESSED_DIR / f"{symbol}_mbp1_overlap.parquet", index=False)
    trades_f.to_parquet(PROCESSED_DIR / f"{symbol}_trades_overlap.parquet", index=False)


def main():
    process_symbol("UL", "triple")
    process_symbol("SHELL", "triple")
    process_symbol("HSBC", "pairwise")


if __name__ == "__main__":
    main()