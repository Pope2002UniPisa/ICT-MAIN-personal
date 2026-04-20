from __future__ import annotations

# sys: libreria standard Python per leggere gli argomenti passati da riga di comando (sys.argv)
import sys
from pathlib import Path

import pandas as pd
import pm4py


# ===== COSTANTI DI PERCORSO =====

# Tutti i dati processati vivono sotto data/processed/
BASE_DIR = Path("data/processed")

# Sorgenti: output delle fasi precedenti della pipeline
OPP_DIR     = BASE_DIR / "opportunities"        # finestre di arbitraggio (una riga per finestra)
SAMPLES_DIR = BASE_DIR / "opportunity_samples"  # tick grezzi dentro le finestre
TRADES_DIR  = BASE_DIR / "simulated_trades"     # blocchi e summary dell'esecuzione simulata

# Output: event log e artefatti pm4py
OUT_DIR = BASE_DIR / "event_logs"

# Simboli analizzati — allineati a SYMBOL_CONFIG in config_simbols.py
SYMBOLS = ["UL", "SHELL", "HSBC"]


# ===== CARICAMENTO DEI DATI SORGENTE =====

def load_all_opportunities() -> pd.DataFrame:
    # Carica e concatena le finestre di arbitraggio per tutti i simboli.
    # Ogni riga = una finestra identificata da (symbol, window_id) con t_open, t_close e metriche di gap.
    frames = []
    for symbol in SYMBOLS:
        path = OPP_DIR / f"{symbol}_opportunities.parquet"
        if not path.exists():
            print(f"[WARN] Missing opportunities file: {path}")
            continue
        df = pd.read_parquet(path)
        if df.empty:
            continue
        # t_open e t_close sono i bordi temporali della finestra (gap sopra soglia)
        df["t_open"]  = pd.to_datetime(df["t_open"],  utc=True, errors="coerce")
        df["t_close"] = pd.to_datetime(df["t_close"], utc=True, errors="coerce")
        df = df.dropna(subset=["t_open", "t_close"]).copy()
        frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_all_trade_summaries() -> pd.DataFrame:
    # Carica e concatena i summary dell'esecuzione simulata per tutti i simboli.
    # Ogni riga = una finestra con profitto netto totale → usato per etichettare l'esito del trade.
    frames = []
    for symbol in SYMBOLS:
        path = TRADES_DIR / f"{symbol}_trade_summary.parquet"
        if not path.exists():
            print(f"[WARN] Missing trade summary file: {path}")
            continue
        df = pd.read_parquet(path)
        if df.empty:
            continue
        frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_first_last_trade_blocks() -> pd.DataFrame:
    # Carica i trade_blocks e ne estrae solo il primo e l'ultimo timestamp per ogni finestra.
    # Il primo ts = momento di ingresso nel trade; l'ultimo ts = momento di uscita.
    frames = []
    for symbol in SYMBOLS:
        path = TRADES_DIR / f"{symbol}_trade_blocks.parquet"
        if not path.exists():
            print(f"[WARN] Missing trade blocks file: {path}")
            continue
        df = pd.read_parquet(path)
        if df.empty:
            continue
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
        df = df.dropna(subset=["ts_event"]).copy()

        # Aggrega per finestra: mantiene solo il primo e l'ultimo step eseguito
        agg = df.groupby(["symbol", "window_id"], as_index=False).agg(
            trade_enter_ts=("ts_event", "min"),
            trade_exit_ts=("ts_event", "max"),
        )
        frames.append(agg)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ===== COSTRUZIONE DELL'EVENT LOG ECONOMICO =====

def build_event_log(
    opp: pd.DataFrame,
    summary: pd.DataFrame,
    trade_times: pd.DataFrame,
) -> pd.DataFrame:
    # Costruisce l'event log in cui ogni case = una finestra di arbitraggio,
    # con eventi economici ordinati cronologicamente che descrivono il processo di arbitraggio.
    #
    # Processo modellato (ispirato al notebook del prof con eventi economici reali):
    #   gap_open → trade_enter → trade_exit → profit_realized / loss_realized
    #                                       ↘ execution_skipped (se nessun block eseguito)
    #
    # case_id = f"{symbol}_{window_id}" → univoco su tutti i simboli

    if opp.empty:
        return pd.DataFrame(columns=["case_id", "activity", "timestamp", "symbol", "pair", "direction"])

    # --- Join opportunities ↔ trade_summary su (symbol, window_id) ---
    # left join: mantiene anche le finestre senza trade (execution_skipped)
    merged = opp.merge(
        summary[["symbol", "window_id", "net_profit_total_eur", "n_blocks"]],
        on=["symbol", "window_id"],
        how="left",
    )

    # --- Join con i timestamp di ingresso/uscita dai trade_blocks ---
    if not trade_times.empty:
        merged = merged.merge(
            trade_times[["symbol", "window_id", "trade_enter_ts", "trade_exit_ts"]],
            on=["symbol", "window_id"],
            how="left",
        )
    else:
        # se non ci sono trade_blocks, le colonne esistono ma sono NaT
        merged["trade_enter_ts"] = pd.NaT
        merged["trade_exit_ts"]  = pd.NaT

    # --- Costruzione degli eventi per ogni finestra ---
    rows: list[dict] = []

    for _, win in merged.iterrows():
        # case_id univoco tra tutti i simboli
        cid = f"{win['symbol']}_{int(win['window_id'])}"
        sym  = win["symbol"]
        pair = win.get("pair", "")
        dirn = win.get("direction", "")

        # EVENTO 1 — gap_open: il gap tra i due mercati supera la soglia dinamica per la prima volta.
        # Corrisponde all'apertura della finestra di arbitraggio (t_open).
        rows.append({
            "case_id":   cid,
            "activity":  "gap_open",
            "timestamp": win["t_open"],
            "symbol":    sym,
            "pair":      pair,
            "direction": dirn,
        })

        has_blocks = pd.notna(win.get("n_blocks")) and win["n_blocks"] > 0
        enter_ts = win.get("trade_enter_ts")
        exit_ts  = win.get("trade_exit_ts")

        if has_blocks and pd.notna(enter_ts):
            # EVENTO 2 — trade_enter: il sistema inizia l'esecuzione simulata del trade.
            # Timestamp = primo step da 10ms eseguito dentro la finestra.
            rows.append({
                "case_id":   cid,
                "activity":  "trade_enter",
                "timestamp": enter_ts,
                "symbol":    sym,
                "pair":      pair,
                "direction": dirn,
            })

            # EVENTO 3 — trade_exit: il sistema chiude la posizione.
            # Timestamp = ultimo step eseguito (può coincidere con trade_enter se finestra brevissima).
            rows.append({
                "case_id":   cid,
                "activity":  "trade_exit",
                "timestamp": exit_ts if pd.notna(exit_ts) else win["t_close"],
                "symbol":    sym,
                "pair":      pair,
                "direction": dirn,
            })

            # EVENTO 4a/4b — profit_realized o loss_realized: esito economico del trade.
            # net_profit_total_eur > 0 → profitto netto positivo dopo costi e rischio.
            # Questo è il "punto di arrivo" del processo di arbitraggio andato a buon fine.
            net = win.get("net_profit_total_eur", 0.0)
            outcome = "profit_realized" if (pd.notna(net) and net > 0) else "loss_realized"
            rows.append({
                "case_id":   cid,
                "activity":  outcome,
                "timestamp": win["t_close"],
                "symbol":    sym,
                "pair":      pair,
                "direction": dirn,
            })

        else:
            # EVENTO 2 (percorso alternativo) — execution_skipped: la finestra si è aperta ma si è
            # chiusa prima che il motore di simulazione riuscisse a eseguire almeno uno step da 10ms.
            # Questo path evidenzia la "race condition": il gap sparisce troppo in fretta.
            # È il punto debole del dataset — in un sistema reale sarebbero i trade mancati per latenza.
            rows.append({
                "case_id":   cid,
                "activity":  "execution_skipped",
                "timestamp": win["t_close"],
                "symbol":    sym,
                "pair":      pair,
                "direction": dirn,
            })

    if not rows:
        return pd.DataFrame(columns=["case_id", "activity", "timestamp", "symbol", "pair", "direction"])

    event_log = pd.DataFrame(rows)
    # Ordina cronologicamente: necessario per pm4py e per la leggibilità del log
    event_log = event_log.sort_values(["case_id", "timestamp"]).reset_index(drop=True)
    return event_log


# ===== STATISTICHE DESCRITTIVE =====

def summarize_event_log(event_log: pd.DataFrame) -> None:
    # Stampa statistiche descrittive sull'event log costruito: distribuzione attività e dimensione case
    if event_log.empty:
        print("Event log vuoto.")
        return

    case_sizes = event_log.groupby("case_id").size()

    print(f"\n=== EVENT LOG — RIEPILOGO ===")
    print(f"Totale eventi:   {len(event_log):,}")
    print(f"Totale case:     {event_log['case_id'].nunique():,}")
    print(f"Simboli presenti: {sorted(event_log['symbol'].unique().tolist())}")
    print(f"Avg eventi/case: {case_sizes.mean():.2f}")
    print(f"Mediana:         {case_sizes.median():.2f}")
    print(f"95° percentile:  {case_sizes.quantile(0.95):.2f}")
    print("\nDistribuzione attività:")
    print(event_log["activity"].value_counts().to_string())
    print("\nDistribuzione per simbolo:")
    print(event_log.groupby(["symbol", "activity"]).size().to_string())


# ===== EXPORT =====

def export_event_log(event_log: pd.DataFrame, out_path: Path) -> None:
    # Salva l'event log come CSV con le colonne standard pm4py: case_id, activity, timestamp.
    # Le colonne extra (symbol, pair, direction) sono incluse come attributi aggiuntivi del log.
    export_df = event_log[["case_id", "activity", "timestamp", "symbol", "pair", "direction"]].copy()
    export_df.to_csv(out_path, index=False)


# ===== FLUSSO PM4PY =====

def run_process_mining(event_log: pd.DataFrame, out_dir: Path) -> None:
    # Esegue l'intero flusso pm4py sul log economico dell'arbitraggio:
    # format → start/end activities → DFG → Inductive Miner → BPMN

    if event_log.empty:
        print("Nessun evento da processare.")
        return

    # format_dataframe: obbligatorio in pm4py per dichiarare quali colonne svolgono i ruoli
    # di case identifier, activity label e timestamp prima di passare ai discovery algorithm.
    # Usa solo le tre colonne fondamentali; le colonne extra (symbol, pair) non vengono passate
    # perché pm4py standard non gestisce attributi multi-livello senza XES.
    log = pm4py.format_dataframe(
        event_log[["case_id", "activity", "timestamp"]].copy(),
        case_id="case_id",
        activity_key="activity",
        timestamp_key="timestamp",
    )

    # Attività iniziali e finali: ci aspettiamo gap_open come start e
    # profit_realized / loss_realized / execution_skipped come end
    start_activities = pm4py.get_start_activities(log)
    end_activities   = pm4py.get_end_activities(log)
    print(f"\nStart activities: {start_activities}")
    print(f"End activities:   {end_activities}")

    # discover_dfg: Directly-Follows Graph — per ogni coppia (A, B) conta quante volte
    # B è stata eseguita immediatamente dopo A nello stesso case.
    # Sul nostro log l'arco più frequente deve essere gap_open → trade_enter.
    dfg, dfg_start, dfg_end = pm4py.discover_dfg(log)
    print("\nDFG edges (activity_a -> activity_b: count):")
    # Stampa ordinato per frequenza decrescente per leggere subito i path dominanti
    for (a, b), count in sorted(dfg.items(), key=lambda x: -x[1]):
        print(f"  {a} -> {b}: {count}")

    # Salva il DFG come PNG nella cartella di output
    dfg_path = str(out_dir / "all_symbols_dfg.png")
    pm4py.save_vis_dfg(dfg, dfg_start, dfg_end, dfg_path)
    print(f"DFG saved: {dfg_path}")

    # discover_process_tree_inductive: Inductive Miner — garantisce un process tree
    # sempre sound (no deadlock, no livelock). Scopre sia il path principale
    # (gap_open → trade_enter → trade_exit → esito) sia il path alternativo (execution_skipped).
    process_tree = pm4py.discover_process_tree_inductive(log)

    # Salva la visualizzazione del process tree come PNG
    tree_path = str(out_dir / "all_symbols_process_tree.png")
    pm4py.save_vis_process_tree(process_tree, tree_path)
    print(f"Process tree saved: {tree_path}")

    # Converte il process tree in BPMN: rappresentazione standard leggibile da tool industriali
    bpmn_model = pm4py.convert_to_bpmn(process_tree)

    # Salva la visualizzazione grafica del BPMN come PNG
    bpmn_png_path = str(out_dir / "all_symbols_bpmn.png")
    pm4py.save_vis_bpmn(bpmn_model, bpmn_png_path)
    print(f"BPMN visualization saved: {bpmn_png_path}")

    # Salva il BPMN in formato XML standard: importabile in Disco, Camunda, Signavio ecc.
    bpmn_xml_path = str(out_dir / "all_symbols_bpmn.bpmn")
    pm4py.write_bpmn(bpmn_model, bpmn_xml_path)
    print(f"BPMN XML saved: {bpmn_xml_path}")


# ===== ENTRY POINT =====

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Caricamento dati sorgente...")

    # Carica le finestre di arbitraggio (una riga per finestra, tutti i simboli)
    opp = load_all_opportunities()
    if opp.empty:
        print("[ERRORE] Nessuna finestra di arbitraggio trovata. Esegui prima cross_market_engine.py.")
        sys.exit(1)
    print(f"Opportunità caricate: {len(opp):,} finestre su {opp['symbol'].nunique()} simboli")

    # Carica i summary dell'esecuzione simulata (profitto netto per finestra)
    summary = load_all_trade_summaries()
    print(f"Trade summary caricato: {len(summary):,} righe")

    # Carica i timestamp di primo e ultimo trade block per ogni finestra
    trade_times = load_first_last_trade_blocks()
    print(f"Trade block times caricati: {len(trade_times):,} finestre con esecuzione")

    # Costruisce l'event log economico con gli eventi del processo di arbitraggio
    print("\nCostruzione event log economico...")
    event_log = build_event_log(opp, summary, trade_times)

    # Stampa le statistiche descrittive per validare il log prima del mining
    summarize_event_log(event_log)

    # Salva il CSV (formato standard pm4py / Disco)
    out_file = OUT_DIR / "all_symbols_arbitrage_event_log.csv"
    export_event_log(event_log, out_file)
    print(f"\nEvent log salvato: {out_file}")

    # Esegue l'intero flusso di process mining pm4py: DFG, Inductive Miner, BPMN
    print("\nEsecuzione process mining (pm4py)...")
    run_process_mining(event_log, OUT_DIR)


if __name__ == "__main__":
    main()
