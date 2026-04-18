#   UL    → triple overlap: Amsterdam (AMS) + Londra (LSE) + New York (US)
#   SHELL → triple overlap: Amsterdam (AMS) + Londra (LSE) + New York (US)
#   HSBC  → pairwise overlap: Londra (LSE) + New York (US)
#           (HSBC Hong Kong chiude quando apre NY, quindi non c'è sovrapposizione tripla utile)
#
# Output: data/processed/market_overlaps.parquet

from __future__ import annotations
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import datetime, time
import pandas as pd

PROCESSED_DIR = Path("data/processed")
OUTPUT_FILE = PROCESSED_DIR / "market_overlaps.parquet"

ROME_TZ = ZoneInfo("Europe/Rome")
LONDON_TZ = ZoneInfo("Europe/London")
NEW_YORK_TZ = ZoneInfo("America/New_York")
AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")

# ===== FUNZIONE: CARICA LE DATE DI RIFERIMENTO DEL PROGETTO =====

def load_reference_dates() -> pd.DatetimeIndex:
    # usa il file delle trades di UL come calendario di riferimento:
    # ci dà esattamente le giornate in cui abbiamo dati reali
    ref_path = PROCESSED_DIR / "UL_trades.parquet"
    if not ref_path.exists():
        raise FileNotFoundError(f"Reference file not found: {ref_path}")

    df = pd.read_parquet(ref_path)

    if "event_date" not in df.columns:
        raise ValueError("Reference parquet must contain 'event_date'.")

    # normalize() azzera la componente oraria → ottiene solo la data
    # dropna() rimuove eventuali valori mancanti
    # unique() elimina i duplicati (una riga per giorno)
    dates = pd.to_datetime(df["event_date"]).dt.normalize().dropna().sort_values().unique()
    return pd.DatetimeIndex(dates)


# ===== FUNZIONE: CONVERTE UN ORARIO DI MERCATO IN ORA DI ROMA =====

def to_rome_timestamp(d: pd.Timestamp, market_tz: ZoneInfo, hh: int, mm: int) -> pd.Timestamp:
    # prende una data (d), un fuso orario di mercato e un orario (hh:mm)
    # e restituisce il momento corrispondente espresso in ora di Roma
    # Esempio: mercato LSE apre alle 08:00 London time
    #          → in estate (BST = UTC+1) corrisponde alle 09:00 di Roma (CEST = UTC+2)
    #          → in inverno (GMT = UTC+0) corrisponde alle 09:00 di Roma (CET = UTC+1)
    # datetime.combine() fonde la data con l'orario in un unico oggetto datetime
    local_dt = datetime.combine(d.date(), time(hh, mm), tzinfo=market_tz)
    # astimezone(ROME_TZ) converte automaticamente tenendo conto dell'ora legale
    return pd.Timestamp(local_dt.astimezone(ROME_TZ))


# ===== FUNZIONE: CALCOLA APERTURA E CHIUSURA DI OGNI MERCATO PER UN GIORNO =====

def build_market_session_bounds(d: pd.Timestamp) -> dict[str, pd.Timestamp]:
    # restituisce un dizionario con i timestamp di apertura e chiusura
    # di ogni mercato, tutti espressi in ora di Roma

    # LSE (London Stock Exchange): apre 08:00, chiude 16:30 ora di Londra
    lse_open = to_rome_timestamp(d, LONDON_TZ, 8, 0)
    lse_close = to_rome_timestamp(d, LONDON_TZ, 16, 30)

    # Euronext Amsterdam: apre 09:00, chiude 17:30 ora di Amsterdam
    ams_open = to_rome_timestamp(d, AMSTERDAM_TZ, 9, 0)
    ams_close = to_rome_timestamp(d, AMSTERDAM_TZ, 17, 30)
    # Anche per Amsterdam si considera 17:30 anche se alcuni provider dicono 17:40

    # NYSE/NASDAQ (US): apre 09:30, chiude 16:00 ora di New York
    us_open = to_rome_timestamp(d, NEW_YORK_TZ, 9, 30)
    us_close = to_rome_timestamp(d, NEW_YORK_TZ, 16, 0)

    return { # mi vengono già dati con l'orario di Roma quindi sono apposto
        "lse_open_rome": lse_open,
        "lse_close_rome": lse_close,
        "ams_open_rome": ams_open,
        "ams_close_rome": ams_close,
        "us_open_rome": us_open,
        "us_close_rome": us_close,
    }

# ===== FUNZIONE: CALCOLA LA FINESTRA DI OVERLAP TRA PIÙ MERCATI =====

def compute_overlap(start_times: list[pd.Timestamp], end_times: list[pd.Timestamp]) -> tuple[pd.Timestamp, pd.Timestamp, float]:
    # logica matematica dell'overlap:
    # la finestra in cui TUTTI i mercati sono aperti contemporaneamente inizia
    # quando l'ULTIMO ad aprire ha aperto e finisce quando il PRIMO a chiudere ha chiuso
    start = max(start_times)
    end = min(end_times)
    overlap_seconds = max(0.0, (end - start).total_seconds())
    return start, end, overlap_seconds


# ===== FUNZIONE: COSTRUISCE UNA RIGA DI OVERLAP PER OGNI GIORNO E TITOLO =====

def build_overlap_rows(dates: pd.DatetimeIndex) -> pd.DataFrame:
    rows: list[dict] = []

    for d in dates:
        # scarta sabato (dayofweek=5) e domenica (dayofweek=6):
        if d.dayofweek >= 5:
            continue
        # calcola apertura/chiusura di tutti i mercati per questa data
        bounds = build_market_session_bounds(d)
        ul_start, ul_end, ul_sec = compute_overlap(
            [
                bounds["ams_open_rome"],
                bounds["lse_open_rome"],
                bounds["us_open_rome"],
            ],
            [
                bounds["ams_close_rome"],
                bounds["lse_close_rome"],
                bounds["us_close_rome"],
            ],
        )
        rows.append(
            {
                "symbol": "UL",
                "overlap_type": "triple",
                "date_rome": d.date(),
                "start_rome": ul_start,        # inizio finestra di overlap in ora di Roma
                "end_rome": ul_end,            # fine finestra di overlap in ora di Roma
                "overlap_seconds": ul_sec,     # durata in secondi
                "overlap_hours": ul_sec / 3600.0,  # durata in ore (più leggibile)
                "markets": "AMS_LSE_US",
            }
        )
        shell_start, shell_end, shell_sec = compute_overlap(
            [
                bounds["ams_open_rome"],
                bounds["lse_open_rome"],
                bounds["us_open_rome"],
            ],
            [
                bounds["ams_close_rome"],
                bounds["lse_close_rome"],
                bounds["us_close_rome"],
            ],
        )
        rows.append(
            {
                "symbol": "SHELL",
                "overlap_type": "triple",
                "date_rome": d.date(),
                "start_rome": shell_start,
                "end_rome": shell_end,
                "overlap_seconds": shell_sec,
                "overlap_hours": shell_sec / 3600.0,
                "markets": "AMS_LSE_US",
            }
        )

        hsbc_start, hsbc_end, hsbc_sec = compute_overlap(
            [
                bounds["lse_open_rome"],
                bounds["us_open_rome"],
            ],
            [
                bounds["lse_close_rome"],
                bounds["us_close_rome"],
            ],
        )
        rows.append(
            {
                "symbol": "HSBC",
                "overlap_type": "pairwise",   # "pairwise" = solo due mercati, non tre
                "date_rome": d.date(),
                "start_rome": hsbc_start,
                "end_rome": hsbc_end,
                "overlap_seconds": hsbc_sec,
                "overlap_hours": hsbc_sec / 3600.0,
                "markets": "LSE_US",
            }
        )

    out = pd.DataFrame(rows)

    # tieni solo le righe con overlap reale (> 0 secondi):
    out = out[out["overlap_seconds"] > 0].copy()
    # ordina per titolo e data, e azzera la numerazione delle righe
    return out.sort_values(["symbol", "date_rome"]).reset_index(drop=True)


# ===== FUNZIONE: STAMPA UN RIEPILOGO DELLE FINESTRE DI OVERLAP =====

def summarize_overlap(df: pd.DataFrame) -> None:
    print("\n=== OVERLAP WINDOWS ===")
    # stampa le prime 10 righe per ogni titolo separatamente,
    for symbol in ["UL", "SHELL", "HSBC"]:
        sub = df[df["symbol"] == symbol]
        print(f"\n--- {symbol} ---")
        print(
            sub[[
                "date_rome",
                "start_rome",
                "end_rome",
                "overlap_hours",
                "overlap_type"
            ]].head(10)
        )

# ===== FUNZIONE PRINCIPALE — ESEGUE LA PIPELINE COMPLETA =====

def main() -> None:
    # crea la cartella di output se non esiste già
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    # step 1: recupera le date reali del progetto dai dati già processati
    dates = load_reference_dates()
    # step 2: calcola le finestre di overlap per ogni giorno e ogni titolo
    overlaps = build_overlap_rows(dates)
    # step 3: stampa un riepilogo a video per verifica visiva
    summarize_overlap(overlaps)
    # step 4: salva il risultato in parquet (usato poi da filter_by_overlap.py)
    overlaps.to_parquet(OUTPUT_FILE, index=False)
    print(f"\nSaved: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()