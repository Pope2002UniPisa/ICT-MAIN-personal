from pathlib import Path
from datetime import date, timedelta
import pandas as pd
import yfinance as yf


# ----------------------------
# 1) LISTE TITOLI
# ----------------------------
stocks = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "NVDA", "TSLA", "JPM", "V", "WMT"
]

etfs = [
    "SPY", "QQQ", "VTI", "IVV", "VOO",
    "IWM", "EFA", "EEM", "XLK", "XLF"
]

# Per semplicità uso bond ETF
bonds = [
    "TLT", "IEF", "SHY", "BND", "AGG",
    "LQD", "HYG", "TIP", "BSV", "VGIT"
]

# Fondi / mutual funds
funds = [
    "VTSAX", "VFIAX", "VWELX", "VWINX", "VGTSX",
    "FCNTX", "AGTHX", "TRBCX", "SWPPX", "FXAIX"
]


# ----------------------------
# 2) DIZIONARIO TICKER -> TIPO
# ----------------------------
ticker_type = {}

for ticker in stocks:
    ticker_type[ticker] = "stock"

for ticker in etfs:
    ticker_type[ticker] = "etf"

for ticker in bonds:
    ticker_type[ticker] = "bond"

for ticker in funds:
    ticker_type[ticker] = "fund"


# ----------------------------
# 3) DATE
# ----------------------------
start_date = "2021-01-01"
end_date = (date.today() - timedelta(days=1)).isoformat()


# ----------------------------
# 4) FUNZIONE PER SCARICARE UN TITOLO
# ----------------------------
def download_one_ticker(ticker, asset_type):
    try:
        df = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            auto_adjust=False,
            progress=False
        )

        # Se non scarica nulla, restituiamo dataframe vuoto
        if df.empty:
            print(f"Nessun dato trovato per {ticker}")
            return pd.DataFrame()

        # reset index per avere Date come colonna
        df = df.reset_index()

        # se le colonne arrivano multi-index, le semplifichiamo
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        # teniamo solo le colonne utili, se esistono
        columns_to_keep = ["Date", "Open", "High", "Low", "Close", "Volume", "Dividends"]
        existing_columns = [col for col in columns_to_keep if col in df.columns]
        df = df[existing_columns]

        # aggiungiamo ticker e tipo
        df["Ticker"] = ticker
        df["Type"] = asset_type

        # riordiniamo le colonne
        final_order = ["Date", "Ticker", "Type", "Open", "High", "Low", "Close", "Volume", "Dividends"]
        existing_final_order = [col for col in final_order if col in df.columns]
        df = df[existing_final_order]

        print(f"Dati scaricati per {ticker}: {len(df)} righe")
        return df

    except Exception as e:
        print(f"Errore con {ticker}: {e}")
        return pd.DataFrame()


# ----------------------------
# 5) SCARICO TUTTI I TITOLI
# ----------------------------
all_data = []

for ticker, asset_type in ticker_type.items():
    df_ticker = download_one_ticker(ticker, asset_type)
    if not df_ticker.empty:
        all_data.append(df_ticker)


# ----------------------------
# 6) UNISCO TUTTO
# ----------------------------
if len(all_data) == 0:
    print("Nessun dato scaricato.")
else:
    universe_df = pd.concat(all_data, ignore_index=True)

    # ordiniamo per ticker e data
    universe_df = universe_df.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    # creiamo cartella output se non esiste
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    # salviamo csv
    output_file = output_dir / "universe_40_assets.csv"
    universe_df.to_csv(output_file, index=False)

    print("\nDownload completato.")
    print(f"Periodo dati: dal {start_date} al {end_date}")
    print(f"Numero totale righe: {len(universe_df)}")
    print(f"Numero totale ticker scaricati: {universe_df['Ticker'].nunique()}")
    print(f"File salvato in: {output_file.resolve()}")

    print("\nPrime 10 righe:")
    print(universe_df.head(10))