import pandas as pd

mbp = pd.read_parquet("data/processed/UL_mbp1.parquet")
trades = pd.read_parquet("data/processed/UL_trades.parquet")

print("MBP shape:", mbp.shape)
print("Trades shape:", trades.shape)

print("\nMBP columns:")
print(mbp.columns.tolist())

print("\nTrades columns:")
print(trades.columns.tolist())

print("\nSpread summary:")
print(mbp["spread"].describe())

print("\nTrades price summary:")
if "price" in trades.columns:
    print(trades["price"].describe())

print("\nEvents per day - MBP:")
print(mbp.groupby("event_date").size())

print("\nTrades per day:")
print(trades.groupby("event_date").size())