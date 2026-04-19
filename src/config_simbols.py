# importa la libreria "pathlib": serve per costruire e gestire percorsi di file
# e cartelle in modo compatibile con qualsiasi sistema operativo
from pathlib import Path
# li gestisce come path (percorso) e non come stringhe

# Path(__file__) → prende il percorso di questo file (config_simbols.py)
# .resolve() → lo trasforma in un percorso completo e assoluto (es. /Users/leonardo/Desktop/.../src/config_simbols.py)
# .parents[1] → risale di 2 livelli nella struttura delle cartelle:
# parents[0] = cartella src/ (quella che contiene questo file)
# parents[1] = cartella sopra src/, cioè la cartella principale del progetto
# Il risultato è che PROJECT_ROOT contiene il percorso della cartella radice del progetto, senza doverlo scrivere a mano. 
# In questo modo il codice funziona su qualsiasi computer, indipendentemente da dove il progetto è salvato.
PROJECT_ROOT = Path(__file__).resolve().parents[1] 
# in MAIUSCOLO per indicare che è una costante (valore che non cambia durante l'esecuzione)

# variabile: percorso alla cartella "data" dentro la cartella principale
DATA_DIR = PROJECT_ROOT / "data"

# variabile: percorso alla sottocartella "processed" dentro "data",
# dove vengono salvati i dati già elaborati
BASE_DIR = DATA_DIR / "processed"

# variabile: percorso alla sottocartella "simulated" dentro "processed",
# dove vengono salvati i dati della simulazione
SIM_DIR = BASE_DIR / "simulated"

# funzione: mkdir è un metodo dell'oggetto Path che significa "make directory" — crea fisicamente la cartella sul disco. 
# parents=True → se anche le cartelle intermedie (data/, processed/) non esistono, le crea tutte;
# exist_ok=True → se la cartella esiste già, non dà errore e va avanti)
SIM_DIR.mkdir(parents=True, exist_ok=True) 
 # comando per scaricare il progetto su una macchina nuova dove quelle cartelle non ci sono


# ===== CONFIGURAZIONE DEI SIMBOLI AZIONARI =====

# SYMBOL_CONFIG è una costante, il cui valore è un dizionario che contiene tutte le informazioni
# il dizionario descrive che forma ha il valore che contiene: è un dizionario annidato, cioè un dizionario che contiene altri dizionari al suo interno
# la chiave più esterna è il simbolo del titolo (es. "UL"), e il valore associato è un altro dizionario che contiene tutte le informazioni specifiche per quel titolo
SYMBOL_CONFIG = {
    # chiave del dizionario: identifica il blocco di configurazione
    "UL": {
        "base_symbol": "UL", # stringa (testo): il ticker ufficiale del titolo sul suo mercato principale
        "base_ccy": "USD",  
        "base_venue": "US",
        "simulate_venues": {
            "LSE": {"ccy": "GBP", "fx_to_eur": 1.17},   # Borsa di Londra — valuta GBP, 1 GBP = 1.17 EUR
            "AMS": {"ccy": "EUR", "fx_to_eur": 1.00},   # Borsa di Amsterdam — valuta EUR, già in Euro
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
    # (quotato come ADR negli USA)
    "HSBC": {
        "base_symbol": "HSBC",
        "base_ccy": "USD",
        "base_venue": "US_ADR",
        "simulate_venues": {
            "LSE": {"ccy": "GBP", "fx_to_eur": 1.17},
        },
    },
}

# ===== PARAMETRI DELLA SIMULAZIONE =====

# dizionario che raccoglie tutti i parametri numerici che controllano
# il comportamento di simulazione
SIMULATION_CONFIG = {
    # Qui parte la logica del BACKWARD-ASOF:
    "sync_tolerance_ms": 10,
    "max_hold_ms": 10,
    # dizionario: associa ogni valuta al suo tasso di conversione verso l'Euro
    "base_fx_to_eur": {
        "USD": 0.92,   # decimale (numero con virgola): 1 USD = 0.92 EUR
        "GBP": 1.17,   # 1 GBP = 1.17 EUR
        "EUR": 1.00,
        "HKD": 0.118,  # 1 HKD = 0.118 EUR
    },
    "price_noise_bps_std": 5.0,
    # decimale: valore medio del moltiplicatore applicato allo spread bid-ask simulato
    # (1.05 significa spread 5% più largo rispetto al mercato base)
    "spread_multiplier_mean": 1.8,
    "spread_multiplier_std": 0.20,
    # quantità di azioni disponibile in ogni ordine non è identica a quella del mercato base
    # in media gli ordini sui mercati simulati hanno il 95% della quantità del mercato base. 
    # in linea con le aspettattive reali dato che i mercati secondari rispetto all'US
    # tendono ad avere meno liquidità (quantità più basse)
    "size_multiplier_mean": 0.95,
    "size_multiplier_std": 0.10,
# --- Parametri delle finestre di arbitraggio ---
    # opportunità di profitto dovuta a disallineamento di prezzo tra mercati)
    "arb_window_prob": 0.00015,
    # disallineamento minimo di prezzo (in bps) affinché
    # una finestra di arbitraggio venga considerata valida
    "arb_window_bps_min": 1.5,
    # disallineamento massimo di prezzo (in bps)
    "arb_window_bps_max": 5.0,
    "arb_window_ms_min": 1,   # durata minima in millisecondi di una finestra di arbitraggio
    "arb_window_ms_max": 6,   # durata massima in millisecondi di una finestra di arbitraggio
# --- Parametri di rischio e costi di transazione ---
    # coefficiente di avversione al rischio nel modello di ottimizzazione
    # (valore più alto = il modello è più prudente e rinuncia a operazioni rischiose)
    "risk_lambda": 6.0,
    # orizzonte temporale in millisecondi usato per calcolare
    # il rischio associato al mantenere una posizione aperta
    "tau_ms_for_risk": 10,
    # rappresenta le commissioni di transazione
    "cost_unit_eur": 0.0020,
    # soglia fissa minima in Euro che il profitto atteso
    # deve superare prima di eseguire un'operazione
    "fixed_buffer_eur": 0.0015,
# --- Filtri di qualità sui dati ---
    # durata minima in millisecondi che un evento deve avere
    # per essere incluso nell'analisi (filtra eventi troppo brevi)
    "min_duration_ms": 1.0,
    # almeno 2 osservazioni (bid-ask) devono essere presenti in una finestra di arbitraggio
    "min_obs_per_window": 2,
# --- Dimensione e cadenza delle operazioni simulate ---
    "trade_block_size": 1000,
    "trade_step_ms": 10,   
}