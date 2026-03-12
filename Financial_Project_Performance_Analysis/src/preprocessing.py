from pathlib import Path
import pandas as pd


# -----------------------
# 1) CARICO DATASET
# -----------------------
df = pd.read_csv("data/raw/universe_40_assets.csv")


# -----------------------
# 2) ORDINO I DATI
# -----------------------
df = df.sort_values(["Ticker", "Date"]).reset_index(drop=True)


# -----------------------
# 3) CALCOLO RENDIMENTO GIORNALIERO
# -----------------------
df["Return"] = df.groupby("Ticker")["Close"].pct_change()


# -----------------------
# 4) CREO CARTELLA OUTPUT
# -----------------------
output_dir = Path("data/processed")
output_dir.mkdir(parents=True, exist_ok=True)


# -----------------------
# 5) SALVO DATASET CON I RENDIMENTI
# -----------------------
output_file_returns = output_dir / "universe_with_returns.csv"
df.to_csv(output_file_returns, index=False)


# -----------------------
# 6) CREO SUNTO FINALE PER TICKER
# -----------------------
summary_rows = []

for ticker in df["Ticker"].unique():
    df_ticker = df[df["Ticker"] == ticker].copy()

    # tolgo il primo NaN del rendimento
    returns = df_ticker["Return"].dropna()

    positive_days = (returns > 0).sum()
    negative_days = (returns < 0).sum()

    initial_close = df_ticker["Close"].iloc[0]
    final_close = df_ticker["Close"].iloc[-1]

    total_monetary_return = final_close - initial_close
    total_percentage_return = (final_close / initial_close) - 1

    asset_type = df_ticker["Type"].iloc[0]

    summary_rows.append({
        "Ticker": ticker,
        "Type": asset_type,
        "Initial Close": initial_close,
        "Final Close": final_close,
        "Positive Days": positive_days,
        "Negative Days": negative_days,
        "Total Monetary Return": total_monetary_return,
        "Total Percentage Return": total_percentage_return
    })


summary_df = pd.DataFrame(summary_rows)

# ordino per ticker
summary_df = summary_df.sort_values("Ticker").reset_index(drop=True)


# -----------------------
# 7) SALVO SUNTO FINALE
# -----------------------
output_file_summary = output_dir / "summary_by_ticker.csv"
summary_df.to_csv(output_file_summary, index=False)


# -----------------------
# 8) STAMPO RISULTATI
# -----------------------
print("\nRendimento giornaliero calcolato correttamente.")
print(f"\nFile salvato in: {output_file_returns}")

print("\nSunto finale creato correttamente.")
print(f"File salvato in: {output_file_summary}")

print("\nPrime righe del dataset con i rendimenti:")
print(df.head())

print("\nPrime righe del sunto finale:")
print(summary_df.head())