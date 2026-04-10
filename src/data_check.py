import pandas as pd

df = pd.read_parquet("data/processed/UL_mbp1.parquet")

print(df.head())
print(df.shape)
print(df.columns)