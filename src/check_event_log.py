import pandas as pd

df = pd.read_csv("data/processed/event_log.csv")

print(df.head())
print("\nActivities:")
print(df["activity"].value_counts())

print("\nCases:")
print(df["case_id"].nunique())