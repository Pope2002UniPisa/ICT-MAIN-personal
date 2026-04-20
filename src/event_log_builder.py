from __future__ import annotations

# sys: libreria standard Python per leggere gli argomenti passati da riga di comando (sys.argv)
import sys
from pathlib import Path

import pandas as pd
import pm4py


# Cartella di output per tutti gli event log generati
OUT_DIR = Path("data/processed/event_logs")


def get_paths(symbol: str) -> tuple[Path, Path]:
    # Restituisce i percorsi attesi dei file parquet MBP e trades per il simbolo dato
    symbol = symbol.upper()
    mbp_path = Path(f"data/processed/{symbol}_mbp1.parquet")
    trades_path = Path(f"data/processed/{symbol}_trades.parquet")
    return mbp_path, trades_path


def load_data(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Carica i dati MBP (Market By Price) e trades dal disco, validando la presenza di ts_event
    mbp_path, trades_path = get_paths(symbol)

    if not mbp_path.exists():
        raise FileNotFoundError(f"MBP file not found: {mbp_path}")
    if not trades_path.exists():
        raise FileNotFoundError(f"Trades file not found: {trades_path}")

    mbp = pd.read_parquet(mbp_path)
    trades = pd.read_parquet(trades_path)

    if "ts_event" not in mbp.columns:
        raise ValueError(f"'ts_event' column missing in {mbp_path}")
    if "ts_event" not in trades.columns:
        raise ValueError(f"'ts_event' column missing in {trades_path}")

    # Conversione a datetime UTC e rimozione di righe con timestamp non valido
    mbp["ts_event"] = pd.to_datetime(mbp["ts_event"], utc=True, errors="coerce")
    trades["ts_event"] = pd.to_datetime(trades["ts_event"], utc=True, errors="coerce")

    mbp = mbp.dropna(subset=["ts_event"]).copy()
    trades = trades.dropna(subset=["ts_event"]).copy()

    return mbp, trades


def filter_regular_market_hours(df: pd.DataFrame) -> pd.DataFrame:
    # Mantiene solo gli eventi che cadono durante la sessione regolare NYSE: lun-ven, 09:30–16:00 ET
    out = df.copy()

    ts_ny = out["ts_event"].dt.tz_convert("America/New_York")
    minutes = ts_ny.dt.hour * 60 + ts_ny.dt.minute

    open_min = 9 * 60 + 30   # 09:30
    close_min = 16 * 60       # 16:00

    is_weekday = ts_ny.dt.dayofweek < 5
    in_session = (minutes >= open_min) & (minutes < close_min)

    out = out[is_weekday & in_session].copy()
    return out


def build_mbp_events(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    # Trasforma le righe MBP in eventi di processo: bid_change, ask_change, spread_widen, spread_narrow
    required_cols = ["ts_event", "bid_px_00", "ask_px_00"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"{symbol}: missing MBP columns: {missing}")

    df = df.sort_values("ts_event").copy()

    events: list[dict] = []
    prev_bid = None
    prev_ask = None

    for _, row in df.iterrows():
        bid = row["bid_px_00"]
        ask = row["ask_px_00"]
        ts = row["ts_event"]

        # Emette un evento se il best bid è cambiato rispetto al tick precedente
        if pd.notna(bid) and prev_bid is not None and bid != prev_bid:
            events.append(
                {
                    "timestamp": ts,
                    "activity": "bid_change",
                    "symbol": symbol,
                }
            )

        # Emette un evento se il best ask è cambiato rispetto al tick precedente
        if pd.notna(ask) and prev_ask is not None and ask != prev_ask:
            events.append(
                {
                    "timestamp": ts,
                    "activity": "ask_change",
                    "symbol": symbol,
                }
            )

        # Emette un evento di variazione dello spread (allargamento o restringimento)
        if (
            prev_bid is not None
            and prev_ask is not None
            and pd.notna(bid)
            and pd.notna(ask)
        ):
            prev_spread = prev_ask - prev_bid
            curr_spread = ask - bid

            if curr_spread > prev_spread:
                events.append(
                    {
                        "timestamp": ts,
                        "activity": "spread_widen",
                        "symbol": symbol,
                    }
                )
            elif curr_spread < prev_spread:
                events.append(
                    {
                        "timestamp": ts,
                        "activity": "spread_narrow",
                        "symbol": symbol,
                    }
                )

        prev_bid = bid
        prev_ask = ask

    if not events:
        return pd.DataFrame(columns=["timestamp", "activity", "symbol"])

    return pd.DataFrame(events)


def build_trade_events(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    # Ogni riga del dataset trades diventa un evento "trade" nell'event log
    if "ts_event" not in df.columns:
        raise ValueError(f"{symbol}: 'ts_event' missing in trades data")

    df = df.sort_values("ts_event").copy()

    events = pd.DataFrame(
        {
            "timestamp": df["ts_event"],
            "activity": "trade",
            "symbol": symbol,
        }
    )
    return events


def assign_cases_by_event(event_log: pd.DataFrame) -> pd.DataFrame:
    """
    Build cases using market events, not fixed time windows.

    Logic:
    - a new case starts at the first event
    - after each trade, the next event starts a new case
    """
    # Ordina per timestamp e inizia il primo case al primo evento
    out = event_log.sort_values("timestamp").reset_index(drop=True).copy()

    out["new_case"] = False
    if len(out) > 0:
        out.loc[0, "new_case"] = True

    # Ogni evento immediatamente successivo a un trade apre un nuovo case
    prev_activity = out["activity"].shift(1)
    out.loc[prev_activity == "trade", "new_case"] = True

    # case_id incrementale: ogni True in new_case fa scattare un nuovo identificatore
    out["case_id"] = out["new_case"].cumsum().astype(int)
    return out


def export_discolog(df: pd.DataFrame, out_path: Path) -> None:
    # Salva l'event log in formato CSV con le sole colonne richieste da pm4py: case_id, activity, timestamp
    export_df = df[["case_id", "activity", "timestamp"]].copy()
    export_df.to_csv(out_path, index=False)


def summarize_cases(df: pd.DataFrame, label: str) -> None:
    # Stampa statistiche descrittive sui case: numero di eventi, numero di case, distribuzione eventi per case
    if df.empty:
        print(f"\n=== {label} ===")
        print("No events found.")
        return

    case_sizes = df.groupby("case_id").size()

    print(f"\n=== {label} ===")
    print(f"Events: {len(df):,}")
    print(f"Cases: {df['case_id'].nunique():,}")
    print(f"Avg events per case: {case_sizes.mean():.2f}")
    print(f"Median events per case: {case_sizes.median():.2f}")
    print(f"95th pct events per case: {case_sizes.quantile(0.95):.2f}")
    print("\nActivities:")
    print(df["activity"].value_counts())


def run_process_mining(df_cases: pd.DataFrame, symbol: str, out_dir: Path) -> None:
    # Esegue l'intero flusso pm4py sul DataFrame dei case: format → DFG → Inductive Miner → BPMN

    if df_cases.empty:
        print("No cases to process.")
        return

    # format_dataframe: obbligatorio in pm4py per dichiarare quali colonne svolgono i ruoli
    # di case identifier, activity label e timestamp prima di passare ai discovery algorithm
    event_log = pm4py.format_dataframe(
        df_cases[["case_id", "activity", "timestamp"]].copy(),
        case_id="case_id",
        activity_key="activity",
        timestamp_key="timestamp",
    )

    # Recupera le attività con cui iniziano e finiscono i case (utile per validare la struttura del log)
    start_activities = pm4py.get_start_activities(event_log)
    end_activities = pm4py.get_end_activities(event_log)
    print(f"\nStart activities: {start_activities}")
    print(f"End activities:   {end_activities}")

    # discover_dfg: costruisce il Directly-Follows Graph — per ogni coppia (A, B) conta
    # quante volte B è stata eseguita immediatamente dopo A all'interno dello stesso case
    dfg, dfg_start, dfg_end = pm4py.discover_dfg(event_log)
    print("\nDFG edges (activity_a -> activity_b: count):")
    # Stampa gli archi ordinati per frequenza decrescente
    for (a, b), count in sorted(dfg.items(), key=lambda x: -x[1]):
        print(f"  {a} -> {b}: {count}")

    # Salva il DFG come immagine PNG nella cartella di output
    dfg_path = str(out_dir / f"{symbol}_dfg.png")
    pm4py.save_vis_dfg(dfg, dfg_start, dfg_end, dfg_path)
    print(f"DFG saved: {dfg_path}")

    # discover_process_tree_inductive: applica l'Inductive Miner, che garantisce
    # un process tree sempre sound (nessun deadlock, nessun livelock)
    process_tree = pm4py.discover_process_tree_inductive(event_log)

    # Salva la visualizzazione del process tree come PNG
    tree_path = str(out_dir / f"{symbol}_process_tree.png")
    pm4py.save_vis_process_tree(process_tree, tree_path)
    print(f"Process tree saved: {tree_path}")

    # Converte il process tree in BPMN: rappresentazione standard di processo leggibile da tool industriali
    bpmn_model = pm4py.convert_to_bpmn(process_tree)

    # Salva la visualizzazione grafica del BPMN come PNG
    bpmn_png_path = str(out_dir / f"{symbol}_bpmn.png")
    pm4py.save_vis_bpmn(bpmn_model, bpmn_png_path)
    print(f"BPMN visualization saved: {bpmn_png_path}")

    # Salva il BPMN anche in formato XML standard, importabile in Disco, Camunda, Signavio ecc.
    bpmn_xml_path = str(out_dir / f"{symbol}_bpmn.bpmn")
    pm4py.write_bpmn(bpmn_model, bpmn_xml_path)
    print(f"BPMN XML saved: {bpmn_xml_path}")


def main() -> None:
    # Simbolo di default "UL"; può essere sovrascritto passando un argomento da riga di comando
    symbol = "UL"
    if len(sys.argv) > 1:
        symbol = sys.argv[1].upper()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\nProcessing symbol: {symbol}")
    mbp, trades = load_data(symbol)

    # Filtraggio alle sole ore di mercato regolare prima di costruire gli eventi
    mbp = filter_regular_market_hours(mbp)
    trades = filter_regular_market_hours(trades)

    print(f"Filtered MBP rows: {len(mbp):,}")
    print(f"Filtered Trades rows: {len(trades):,}")

    print("\nBuilding MBP events...")
    mbp_events = build_mbp_events(mbp, symbol)

    print("Building trade events...")
    trade_events = build_trade_events(trades, symbol)

    # Unione e ordinamento cronologico degli eventi MBP e trades in un unico log
    event_log = pd.concat([mbp_events, trade_events], ignore_index=True)
    event_log = event_log.sort_values("timestamp").reset_index(drop=True)

    print(f"\nBase event log size: {len(event_log):,}")

    # Assegnazione dei case_id e export CSV (formato disco-log)
    df_cases = assign_cases_by_event(event_log)
    summarize_cases(df_cases, f"{symbol}_event_driven_trade")

    out_file = OUT_DIR / f"{symbol}_event_log_event_driven_trade.csv"
    export_discolog(df_cases, out_file)
    print(f"Saved: {out_file}")

    # Esegue l'intero flusso di process mining pm4py: DFG, Inductive Miner, BPMN
    print("\nRunning process mining (pm4py)...")
    run_process_mining(df_cases, symbol, OUT_DIR)


if __name__ == "__main__":
    main()