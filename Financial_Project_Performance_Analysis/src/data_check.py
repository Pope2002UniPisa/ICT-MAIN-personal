from pathlib import Path
import pandas as pd


# ---------------------------
# 1) CARICO IL FILE
# ---------------------------
file_path = Path("data/raw/universe_40_assets.csv")

df = pd.read_csv(file_path)

print("\nDataset caricato correttamente")


# ---------------------------
# 2) NUMERO TOTALE RIGHE
# ---------------------------
print("\nNumero totale righe:")
print(len(df))


# ---------------------------
# 3) NUMERO TICKER
# ---------------------------
tickers = df["Ticker"].unique()

print("\nNumero ticker presenti:")
print(len(tickers))

print("\nLista ticker:")
print(tickers)


# ---------------------------
# 4) RIGHE PER TICKER
# ---------------------------
print("\nNumero righe per ticker:")

rows_per_ticker = df.groupby("Ticker").size()

print(rows_per_ticker)


# ---------------------------
# 5) STRUMENTI PER CATEGORIA
# ---------------------------
print("\nNumero ticker per categoria:")

type_count = df.groupby("Type")["Ticker"].nunique()

print(type_count)


# ---------------------------
# 6) VALORI MANCANTI
# ---------------------------
print("\nValori mancanti per colonna:")

missing_values = df.isna().sum()

print(missing_values)


# ---------------------------
# 7) CONTROLLO DATE
# ---------------------------
print("\nPrima data nel dataset:")
print(df["Date"].min())

print("\nUltima data nel dataset:")
print(df["Date"].max())