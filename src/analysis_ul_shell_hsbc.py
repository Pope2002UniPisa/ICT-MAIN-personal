# Questo script è un controllo diagnostico da eseguire dopo il preprocessing
# e prima di lanciare le analisi cross-market.
# Verifica per ciascuno dei tre titoli (UL, SHELL, HSBC) che:
#   1. i file processati esistano e abbiano le dimensioni attese
#   2. le colonne siano quelle giuste
#   3. lo spread bid-ask sia in un range sensato (niente valori negativi o assurdi)
#   4. i prezzi delle trade siano distribuiti in modo ragionevole
#   5. la copertura temporale sia uniforme (nessun giorno con troppo pochi dati)
#
# Eseguilo ogni volta che rigeneri i dati dal preprocessing per avere
# una conferma rapida che tutto sia andato a buon fine.

import pandas as pd
from pathlib import Path

PROCESSED_DIR = Path("data/processed")
SYMBOLS = ["UL", "SHELL", "HSBC"]


# ===== FUNZIONE: ANALISI DI UN SINGOLO TITOLO =====

def analyze_symbol(symbol: str) -> None:
    # costruisce i percorsi ai file processati per il titolo
    mbp_path = PROCESSED_DIR / f"{symbol}_mbp1.parquet"
    trades_path = PROCESSED_DIR / f"{symbol}_trades.parquet"

    print(f"\n{'='*60}") # separatore visivo tra i titoli
    print(f"  {symbol}")
    print(f"{'='*60}")

    if not mbp_path.exists():
        print(f"  [ERRORE] File MBP non trovato: {mbp_path}")
        return
    if not trades_path.exists():
        print(f"  [ERRORE] File trades non trovato: {trades_path}")
        return

    mbp = pd.read_parquet(mbp_path)
    trades = pd.read_parquet(trades_path)

    # shape restituisce (numero_righe, numero_colonne)
    print(f"\n[Dimensioni]")
    print(f"  MBP:    {mbp.shape[0]:>10,} righe  x  {mbp.shape[1]} colonne")
    print(f"  Trades: {trades.shape[0]:>10,} righe  x  {trades.shape[1]} colonne")

    # stampa i nomi delle colonne per verificare che il preprocessing
    print(f"\n[Colonne MBP]")
    print(f"  {mbp.columns()}")
    print(f"[Colonne Trades]")
    print(f"  {trades.columns.tolist()}")

    print(f"\n[Spread bid-ask (MBP)]")
    print(mbp["spread"].describe().to_string())

    n_bad_spread = (mbp["spread"] <= 0).sum()
    if n_bad_spread > 0:
        print(f"  *** ATTENZIONE: {n_bad_spread} righe con spread <= 0 ***")

    # describe() mostra min, max, media e percentili
    # controlla che il range di prezzi sia plausibile per il titolo
    print(f"\n[Prezzi trade]")
    if "price" in trades.columns:
        print(trades["price"].describe().to_string())
    else:
        print("  Colonna 'price' non trovata.")

    # conta quanti eventi MBP e quante trade ci sono per ogni giorno
    print(f"\n[Eventi MBP per giorno]")
    mbp_per_day = mbp.groupby("event_date").size().rename("n_eventi")
    print(mbp_per_day.to_string())

    print(f"\n[Trade per giorno]")
    trades_per_day = trades.groupby("event_date").size().rename("n_trade")
    print(trades_per_day.to_string())

    # date mancanti in uno dei due dataset possono causare problemi nel join cross-market
    mbp_dates = set(mbp["event_date"].unique())
    trades_dates = set(trades["event_date"].unique())
    only_in_mbp = mbp_dates - trades_dates # differenza tra insiemi: ho gli elementi che stanno in mbp, ma non in trades
    only_in_trades = trades_dates - mbp_dates
    if only_in_mbp:
        print(f"  *** Date in MBP ma non in trades: {sorted(only_in_mbp)} ***")
    if only_in_trades:
        print(f"  *** Date in trades ma non in MBP: {sorted(only_in_trades)} ***")
        # sorted() ordine le date in ordine cronologico per una lettura più facile

# ===== FUNZIONE PRINCIPALE =====

def main() -> None:
    # gira l'analisi per ogni titolo in sequenza
    for symbol in SYMBOLS:
        analyze_symbol(symbol)

    print(f"\n{'='*60}")
    print("  Sanity check completato.")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
