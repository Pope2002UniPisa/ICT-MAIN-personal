# definisce per ogni mercato il suo calendario di open/close;
# converte tutto in UTC;
# costruisce per ogni data gli overlap:
# UL: triple
# SHELL: triple
# HSBC: pairwise (HK visto come analisi rispetto al giorno successivo, dato che chiude quando apre l'altra)
# salva un file tipo: data/processed/market_overlaps.parquet

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


def load_reference_dates() -> pd.DatetimeIndex:
    """
    Usa i dati già processati per estrarre il range di date del progetto.
    Per ora basta UL, ma puoi cambiare riferimento se vuoi.
    """
    ref_path = PROCESSED_DIR / "UL_trades.parquet"
    if not ref_path.exists():
        raise FileNotFoundError(f"Reference file not found: {ref_path}")

    df = pd.read_parquet(ref_path)

    if "event_date" in df.columns:
        dates = pd.to_datetime(df["event_date"]).dt.normalize().dropna().sort_values().unique()
        return pd.DatetimeIndex(dates)

    if "ts_event" not in df.columns:
        raise ValueError("Reference parquet must contain 'event_date' or 'ts_event'.")

    ts = pd.to_datetime(df["ts_event"], utc=True, errors="coerce").dropna()
    ts_rome = ts.dt.tz_convert(ROME_TZ)
    dates = ts_rome.dt.normalize().sort_values().unique()
    return pd.DatetimeIndex(dates)


def to_rome_timestamp(d: pd.Timestamp, market_tz: ZoneInfo, hh: int, mm: int) -> pd.Timestamp:
    """
    Costruisce un timestamp locale del mercato e lo converte in Europe/Rome.
    """
    local_dt = datetime.combine(d.date(), time(hh, mm), tzinfo=market_tz)
    return pd.Timestamp(local_dt.astimezone(ROME_TZ))


def build_market_session_bounds(d: pd.Timestamp) -> dict[str, pd.Timestamp]:
    """
    Restituisce open/close in ora di Roma per i tre mercati utili.
    """
    lse_open = to_rome_timestamp(d, LONDON_TZ, 8, 0)
    lse_close = to_rome_timestamp(d, LONDON_TZ, 16, 30)

    ams_open = to_rome_timestamp(d, AMSTERDAM_TZ, 9, 0)
    ams_close = to_rome_timestamp(d, AMSTERDAM_TZ, 17, 30)
# Anche per Amsterdam si considera 17:30 anche se alcuni provider dicono 17:40
    us_open = to_rome_timestamp(d, NEW_YORK_TZ, 9, 30)
    us_close = to_rome_timestamp(d, NEW_YORK_TZ, 16, 0)

    return {
        "lse_open_rome": lse_open,
        "lse_close_rome": lse_close,
        "ams_open_rome": ams_open,
        "ams_close_rome": ams_close,
        "us_open_rome": us_open,
        "us_close_rome": us_close,
    }


def compute_overlap(start_times: list[pd.Timestamp], end_times: list[pd.Timestamp]) -> tuple[pd.Timestamp, pd.Timestamp, float]:
    start = max(start_times)
    end = min(end_times)

    overlap_seconds = max(0.0, (end - start).total_seconds())
    return start, end, overlap_seconds


def build_overlap_rows(dates: pd.DatetimeIndex) -> pd.DataFrame:
    rows: list[dict] = []

    for d in dates:
        # scarta weekend
        if d.dayofweek >= 5:
            continue

        bounds = build_market_session_bounds(d)

        # UL triple overlap: AMS + LSE + US
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
                "start_rome": ul_start,
                "end_rome": ul_end,
                "overlap_seconds": ul_sec,
                "overlap_hours": ul_sec / 3600.0,
                "markets": "AMS_LSE_US",
            }
        )

        # SHELL triple overlap: AMS + LSE + US
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

        # HSBC pairwise overlap: LSE + US
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
                "overlap_type": "pairwise",
                "date_rome": d.date(),
                "start_rome": hsbc_start,
                "end_rome": hsbc_end,
                "overlap_seconds": hsbc_sec,
                "overlap_hours": hsbc_sec / 3600.0,
                "markets": "LSE_US",
            }
        )

    out = pd.DataFrame(rows)

    # tieni solo overlap reali
    out = out[out["overlap_seconds"] > 0].copy()

    return out.sort_values(["symbol", "date_rome"]).reset_index(drop=True)


def summarize_overlap(df: pd.DataFrame) -> None:
    print("\n=== OVERLAP WINDOWS ===")

    # stampa per ogni titolo separatamente
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

def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    dates = load_reference_dates()
    overlaps = build_overlap_rows(dates)
    summarize_overlap(overlaps)

    overlaps.to_parquet(OUTPUT_FILE, index=False)
    print(f"\nSaved: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()