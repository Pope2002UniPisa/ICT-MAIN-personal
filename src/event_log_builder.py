from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


OUT_DIR = Path("data/processed/event_logs")


def get_paths(symbol: str) -> tuple[Path, Path]:
    symbol = symbol.upper()
    mbp_path = Path(f"data/processed/{symbol}_mbp1.parquet")
    trades_path = Path(f"data/processed/{symbol}_trades.parquet")
    return mbp_path, trades_path


def load_data(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
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

    mbp["ts_event"] = pd.to_datetime(mbp["ts_event"], utc=True, errors="coerce")
    trades["ts_event"] = pd.to_datetime(trades["ts_event"], utc=True, errors="coerce")

    mbp = mbp.dropna(subset=["ts_event"]).copy()
    trades = trades.dropna(subset=["ts_event"]).copy()

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


def build_mbp_events(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
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

        if pd.notna(bid) and prev_bid is not None and bid != prev_bid:
            events.append(
                {
                    "timestamp": ts,
                    "activity": "bid_change",
                    "symbol": symbol,
                }
            )

        if pd.notna(ask) and prev_ask is not None and ask != prev_ask:
            events.append(
                {
                    "timestamp": ts,
                    "activity": "ask_change",
                    "symbol": symbol,
                }
            )

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
    out = event_log.sort_values("timestamp").reset_index(drop=True).copy()

    out["new_case"] = False
    if len(out) > 0:
        out.loc[0, "new_case"] = True

    prev_activity = out["activity"].shift(1)
    out.loc[prev_activity == "trade", "new_case"] = True

    out["case_id"] = out["new_case"].cumsum().astype(int)
    return out


def export_discolog(df: pd.DataFrame, out_path: Path) -> None:
    export_df = df[["case_id", "activity", "timestamp"]].copy()
    export_df.to_csv(out_path, index=False)


def summarize_cases(df: pd.DataFrame, label: str) -> None:
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


def main() -> None:
    symbol = "UL"
    if len(sys.argv) > 1:
        symbol = sys.argv[1].upper()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\nProcessing symbol: {symbol}")
    mbp, trades = load_data(symbol)

    mbp = filter_regular_market_hours(mbp)
    trades = filter_regular_market_hours(trades)

    print(f"Filtered MBP rows: {len(mbp):,}")
    print(f"Filtered Trades rows: {len(trades):,}")

    print("\nBuilding MBP events...")
    mbp_events = build_mbp_events(mbp, symbol)

    print("Building trade events...")
    trade_events = build_trade_events(trades, symbol)

    event_log = pd.concat([mbp_events, trade_events], ignore_index=True)
    event_log = event_log.sort_values("timestamp").reset_index(drop=True)

    print(f"\nBase event log size: {len(event_log):,}")

    df_cases = assign_cases_by_event(event_log)
    summarize_cases(df_cases, f"{symbol}_event_driven_trade")

    out_file = OUT_DIR / f"{symbol}_event_log_event_driven_trade.csv"
    export_discolog(df_cases, out_file)
    print(f"Saved: {out_file}")


if __name__ == "__main__":
    main()