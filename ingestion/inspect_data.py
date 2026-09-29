import pandas as pd


df = pd.read_parquet(
    "data/bronze/season=2025/round=01/laps.parquet"
)


print("\nCOLUMNS:")
print(df.columns.tolist())


print("\nSHAPE:")
print(df.shape)


print("\nFIRST 10 ROWS:")
print(
    df[
        [
            "Driver",
            "LapNumber",
            "LapTime",
            "Compound",
            "TyreLife",
            "Position"
        ]
    ].head(10)
)


print("\nDATA TYPES:")

print(df.dtypes)