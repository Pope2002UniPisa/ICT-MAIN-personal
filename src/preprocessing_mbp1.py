# ===== ORDER BOOK LEVEL 1 — CARICAMENTO E PULIZIA DEI DATI GREZZI =====

# abilita la sintassi moderna per i "suggerimenti di tipo" (es. str | None)
# anche su versioni di Python più vecchie
from __future__ import annotations 
from pathlib import Path
import databento as db
import pandas as pd

# percorso di dove vengono salvati i dati 
PROCESSED_DIR = Path("data/processed")

# funzione di LETTURA di un singolo file DBN e conversione in DataFrame
def load_single_dbn(file_path: Path) -> pd.DataFrame:
    # apre il file DBN (formato proprietario di Databento) e lo carica in memoria
    store = db.DBNStore.from_file(str(file_path))
    # converte i dati in un DataFrame e ne fa una copia indipendente
    # (.copy() evita che modifiche successive alterino i dati originali)
    df = store.to_df().copy()
    # i dati a volte hanno il timestamp come "indice" della tabella invece che
    # come colonna normale — in quel caso lo spostiamo a colonna
    if df.index.name == "ts_event":
        df = df.reset_index()

    return df


# ===== FUNZIONE: CARICA TUTTI I FILE MBP-1 DA UNA CARTELLA =====
def load_all_mbp1_files(raw_dir: Path) -> pd.DataFrame:
    # cerca tutti i file con estensione .mbp-1.dbn.zst nella cartella
    # sorted() li ordina alfabeticamente per garantire un ordine consistente
    files = sorted(raw_dir.glob("*.mbp-1.dbn.zst"))
    if not files:
        raise FileNotFoundError(f"No MBP-1 files found in {raw_dir}")
    # lista vuota che raccoglierà le tabelle caricate da ogni file
    frames = []
    for file_path in files:
        # stampa il nome del file che sta caricando, con f"" indica una f-stringa dove 
        # {...} permette di inserire il valore di una variabile direttamente nel testo
        print(f"Loading: {file_path.name}")
        df = load_single_dbn(file_path) # si richiama la funzione di caricamento per ogni file
        # aggiunge la tabella appena caricata alla lista
        frames.append(df)
    # unisce tutte le tabelle in una sola, resettando la numerazione delle righe
    return pd.concat(frames, ignore_index=True)


# ===== FUNZIONE: PULIZIA DEI DATI =====

# funzione: pulisce i dati grezzi di un simbolo, aggiunge colonne calcolate
# e restituisce solo le colonne necessarie all'analisi
# symbol: str → parametro stringa con il nome del titolo (es. "UL")
def clean_mbp1(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    # lavora su una copia per non modificare la tabella originale
    out = df.copy()

    # lista delle colonne che devono essere presenti obbligatoriamente
    # ts_event = timestamp dell'evento
    # bid_px_00 = prezzo migliore di acquisto (bid)
    # ask_px_00 = prezzo migliore di vendita (ask)
    # bid_sz_00 = quantità disponibile al prezzo bid
    # ask_sz_00 = quantità disponibile al prezzo ask
    required_cols = ["ts_event", "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00"]
    # controlla se manca qualche colonna obbligatoria
    missing = [col for col in required_cols if col not in out.columns]
    if missing:
        raise ValueError(f"{symbol}: missing MBP columns: {missing}")

    # converte il timestamp in formato data/ora standard con fuso UTC
    # errors="coerce" → se un valore non è convertibile, lo trasforma in NaN
    # (valore vuoto) invece di dare errore
    out["ts_event"] = pd.to_datetime(out["ts_event"], utc=True, errors="coerce")
    out["bid_px_00"] = pd.to_numeric(out["bid_px_00"], errors="coerce")
    out["ask_px_00"] = pd.to_numeric(out["ask_px_00"], errors="coerce")
    out["bid_sz_00"] = pd.to_numeric(out["bid_sz_00"], errors="coerce")
    out["ask_sz_00"] = pd.to_numeric(out["ask_sz_00"], errors="coerce")

    # grazie a coerce le righe con valori NaN posso eliminarle tutte insieme con dropna()
    # dropna() è un metodo del DataFrame di pandas, con "drop" che significa "elimina" 
    # e "na" che sta per "not available" (valori mancanti)
    out = out.dropna(subset=required_cols).copy()
    # ordina le righe per timestamp crescente e azzera la numerazione delle righe
    out = out.sort_values("ts_event").reset_index(drop=True)
    out["symbol"] = symbol
    out["spread"] = out["ask_px_00"] - out["bid_px_00"]
    out["mid"] = (out["bid_px_00"] + out["ask_px_00"]) / 2
    out["event_date"] = out["ts_event"].dt.date
    # converte il timestamp da nanosecondi a millisecondi (divide per 10^6)
    out["ts_event_ms"] = out["ts_event"].astype("int64") // 10**6

    # lista delle colonne da tenere nel risultato finale (scarta tutto il resto)
    final_cols = [
        "ts_event",
        "symbol",
        "bid_px_00",
        "ask_px_00",
        "bid_sz_00",
        "ask_sz_00",
        "spread",
        "mid",
        "event_date",
        "ts_event_ms",
    ]
    out = out[final_cols].copy()
    return out


# ===== FUNZIONE: PIPELINE COMPLETA PER UN SINGOLO SIMBOLO =====

# funzione: esegue l'intera pipeline (carica, pulisce, salva) per un titolo
# -> Path → restituisce il percorso del file salvato
def process_symbol(symbol: str, folder_name: str | None = None) -> Path:
    # se folder_name non è specificato, usa il simbolo come nome cartella
    folder = folder_name or symbol
    # costruisce il percorso alla cartella dei dati grezzi
    raw_dir = Path(f"data/raw/databento/{folder}/mbp1")
    # costruisce il percorso del file di output in formato parquet
    # (formato colonnare efficiente per grandi tabelle di dati)
    output_file = PROCESSED_DIR / f"{symbol}_mbp1.parquet"

    # crea la cartella di output se non esiste già
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    # carica tutti i file grezzi dalla cartella
    df = load_all_mbp1_files(raw_dir)
    # stampa le dimensioni della tabella grezza (numero righe, numero colonne)
    print(f"\n[{symbol}] Raw shape: {df.shape}")
    # pulisce e arricchisce i dati
    df_clean = clean_mbp1(df, symbol=symbol)
    # stampa le dimensioni dopo la pulizia
    print(f"[{symbol}] Clean shape: {df_clean.shape}")
    # stampa i nomi di tutte le colonne presenti nella tabella finale nn
    print(f"\n[{symbol}] Columns:")
    print(df_clean.columns.tolist())
    # stampa le prime 5 righe della tabella per verifica visiva
    print(f"\n[{symbol}] Head:")
    print(df_clean.head())
    # salva la tabella pulita su disco in formato parquet (index=False = non salva la numerazione delle righe)
    df_clean.to_parquet(output_file, index=False)
    print(f"\n[{symbol}] Saved processed file to: {output_file}")
    return output_file

# ===== FUNZIONE PRINCIPALE — ESEGUE LA PIPELINE PER TUTTI I SIMBOLI =====

def main() -> None: # questa funzione non restituisce nulla, per questo c'è "-> None"
    process_symbol("UL", "UL")
    process_symbol("SHELL", "Shell")
    process_symbol("HSBC", "HSBC")

if __name__ == "__main__":
    main()

# Immagina di avere due file:
# File A (preprocessing_mbp1.py) — contiene le funzioni e il main()
# File B (altro_file.py) — vuole usare solo la funzione clean_mbp1 di File A
# Senza if __name__ == "__main__":, File A sarebbe così:
# def clean_mbp1(...): ...
# def main(): ...
# main()  # parte sempre, senza condizioni
# Quando File B scrive import preprocessing_mbp1 per prendere clean_mbp1, Python 
# legge tutto File A dall'inizio alla fine — e quando arriva a main() lo esegue 
# automaticamente, caricando tutti i dati anche se File B non lo voleva.

# con if __name__ == "__main__":, invece:
# Python prima di eseguire main() fa una domanda:
# "Questo file è stato avviato direttamente oppure importato?"
# Se lo hai avviato tu direttamente dal terminale, Python risponde "__main__" — 
# la condizione è vera e main() parte.
# Se invece è stato importato da un altro file, Python risponde con il nome del 
# file ("preprocessing_mbp1") — la condizione è falsa e main() non parte.