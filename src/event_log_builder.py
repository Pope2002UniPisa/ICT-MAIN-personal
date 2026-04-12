import pandas as pd


# === LOAD DATA ===
mbp = pd.read_parquet("data/processed/UL_mbp1.parquet")
trades = pd.read_parquet("data/processed/UL_trades.parquet")


# === 1. EVENTI DA ORDER BOOK ===

def build_mbp_events(df):
    df = df.copy()

    # ordinamento
    df = df.sort_values("ts_event")

    events = []

    prev_bid = None
    prev_ask = None

    for _, row in df.iterrows():

        bid = row["bid_px_00"]
        ask = row["ask_px_00"]

        # cambio bid
        if prev_bid is not None and bid != prev_bid:
            events.append({
                "timestamp": row["ts_event"],
                "activity": "bid_change"
            })

        # cambio ask
        if prev_ask is not None and ask != prev_ask:
            events.append({
                "timestamp": row["ts_event"],
                "activity": "ask_change"
            })

        # spread change
        if prev_bid is not None and prev_ask is not None:
            prev_spread = prev_ask - prev_bid
            curr_spread = ask - bid

            if curr_spread > prev_spread:
                events.append({
                    "timestamp": row["ts_event"],
                    "activity": "spread_widen"
                })
            elif curr_spread < prev_spread:
                events.append({
                    "timestamp": row["ts_event"],
                    "activity": "spread_narrow"
                })

        prev_bid = bid
        prev_ask = ask

    return pd.DataFrame(events)


# === 2. EVENTI DA TRADES ===

def build_trade_events(df):
    df = df.copy()

    events = []

    for _, row in df.iterrows():

        events.append({
            "timestamp": row["ts_event"],
            "activity": "trade"
        })

    return pd.DataFrame(events)


# === BUILD ===

print("Building MBP events...")
mbp_events = build_mbp_events(mbp)

print("Building Trade events...")
trade_events = build_trade_events(trades)


# === MERGE ===

event_log = pd.concat([mbp_events, trade_events])
event_log = event_log.sort_values("timestamp").reset_index(drop=True)


# === ADD CASE ID (temporaneo)

event_log["case_id"] = (
    event_log["timestamp"].astype("int64") // 10**9
) // 60  # ogni minuto = un caso


# === SAVE ===

event_log.to_csv("data/processed/event_log.csv", index=False)

print("\nEvent log created!")
print(event_log.head())
print(event_log["activity"].value_counts())

# Per il momento abbiamo definito:
# - "bid_change": quando cambia il prezzo di acquisto
# - "ask_change": quando cambia il prezzo di vendita
# - "spread_widen": quando lo spread si allarga
# - "spread_narrow": quando lo spread si restringe
# - "trade": quando avviene una transazione