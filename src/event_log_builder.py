from __future__ import annotations

import pandas as pd
from pathlib import Path


MBP_PATH = Path("data/processed/UL_mbp1.parquet")
TRADES_PATH = Path("data/processed/UL_trades.parquet")
OUT_DIR = Path("data/processed/event_logs")


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    mbp = pd.read_parquet(MBP_PATH)
    trades = pd.read_parquet(TRADES_PATH)

    mbp["ts_event"] = pd.to_datetime(mbp["ts_event"], utc=True)
    trades["ts_event"] = pd.to_datetime(trades["ts_event"], utc=True)

    return mbp, trades


def filter_regular_market_hours(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    ts_ny = out["ts_event"].dt.tz_convert("America/New_York")

    minutes = ts_ny.dt.hour * 60 + ts_ny.dt.minute
    open_min = 9 * 60 + 30
    close_min = 16 * 60

    is_weekday = ts_ny.dt.dayofweek < 5
    in_session = (minutes >= open_min) & (minutes < close_min)

    out = out[is_weekday & in_session].copy()
    return out


def build_mbp_events(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("ts_event").copy()

    events = []
    prev_bid = None
    prev_ask = None

    for _, row in df.iterrows():
        bid = row["bid_px_00"]
        ask = row["ask_px_00"]

        if pd.notna(bid) and prev_bid is not None and bid != prev_bid:
            events.append({
                "timestamp": row["ts_event"],
                "activity": "bid_change",
            })

        if pd.notna(ask) and prev_ask is not None and ask != prev_ask:
            events.append({
                "timestamp": row["ts_event"],
                "activity": "ask_change",
            })

        if (
            prev_bid is not None and prev_ask is not None
            and pd.notna(bid) and pd.notna(ask)
        ):
            prev_spread = prev_ask - prev_bid
            curr_spread = ask - bid

            if curr_spread > prev_spread:
                events.append({
                    "timestamp": row["ts_event"],
                    "activity": "spread_widen",
                })
            elif curr_spread < prev_spread:
                events.append({
                    "timestamp": row["ts_event"],
                    "activity": "spread_narrow",
                })

        prev_bid = bid
        prev_ask = ask

    return pd.DataFrame(events)


def build_trade_events(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("ts_event").copy()

    events = pd.DataFrame({
        "timestamp": df["ts_event"],
        "activity": "trade",
    })
    return events


def assign_cases_by_event(event_log: pd.DataFrame) -> pd.DataFrame:
    """
    Build cases using market events, not fixed time windows.

    Logic:
    - a new case starts at the first event
    - after each trade, the next event starts a new case
    """
    out = event_log.sort_values("timestamp").reset_index(drop=True).copy()

    # A new case starts at the first row
    out["new_case"] = False
    if len(out) > 0:
        out.loc[0, "new_case"] = True

    # If the PREVIOUS event was a trade, then the CURRENT event starts a new case
    prev_activity = out["activity"].shift(1)
    out.loc[prev_activity == "trade", "new_case"] = True

    out["case_id"] = out["new_case"].cumsum().astype(int)
    return out


def export_discolog(df: pd.DataFrame, out_path: Path) -> None:
    export_df = df[["case_id", "activity", "timestamp"]].copy()
    export_df.to_csv(out_path, index=False)


def summarize_cases(df: pd.DataFrame, label: str) -> None:
    case_sizes = df.groupby("case_id").size()

    print(f"\n=== {label} ===")
    print(f"Events: {len(df):,}")
    print(f"Cases: {df['case_id'].nunique():,}")
    print(f"Avg events per case: {case_sizes.mean():.2f}")
    print(f"Median events per case: {case_sizes.median():.2f}")
    print(f"95th pct events per case: {case_sizes.quantile(0.95):.2f}")
    print("\nActivities:")
    print(df["activity"].value_counts())


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    mbp, trades = load_data()

    mbp = filter_regular_market_hours(mbp)
    trades = filter_regular_market_hours(trades)

    print(f"Filtered MBP rows: {len(mbp):,}")
    print(f"Filtered Trades rows: {len(trades):,}")

    print("\nBuilding MBP events...")
    mbp_events = build_mbp_events(mbp)

    print("Building trade events...")
    trade_events = build_trade_events(trades)

    event_log = pd.concat([mbp_events, trade_events], ignore_index=True)
    event_log = event_log.sort_values("timestamp").reset_index(drop=True)

    print(f"\nBase event log size: {len(event_log):,}")

    df_cases = assign_cases_by_event(event_log)
    summarize_cases(df_cases, "event_driven_trade")

    out_file = OUT_DIR / "event_log_event_driven_trade.csv"
    export_discolog(df_cases, out_file)
    print(f"Saved: {out_file}")


if __name__ == "__main__":
    main()