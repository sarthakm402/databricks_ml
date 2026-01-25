import pandas as pd
import numpy as np

# read pristine raw data
df_tran = pd.read_csv(
    r"/mnt/c/Users/sarthak mohapatra/Downloads/archive/fraudTrain.csv"
)

df_tran["event_ts"] = pd.to_datetime(df_tran["trans_date_trans_time"])

# working copy for corruption
df = df_tran.copy()

# generate duplicates
rows = 10000
sample = 5
np.random.seed(42)

syn_subset = [
    df.sample(n=rows, random_state=i)
    for i in range(sample)
]

syn_df = pd.concat(syn_subset, ignore_index=True)
df = pd.concat([df, syn_df], ignore_index=True)

# generate nulls
null_rows = np.random.sample(len(df)) < 0.2
null_df = df.loc[null_rows].copy()

null_df["merchant"] = None
null_df["category"] = None

df = pd.concat([df, null_df], ignore_index=True)

# generate skewed data
skew_rows = np.random.sample(len(df)) < 0.02
skew_df = df.loc[skew_rows].copy()

skew_data = pd.concat([skew_df] * 50, ignore_index=True)
df = pd.concat([skew_data, df], ignore_index=True)

# generate duplicates with different values
duplicate_rows = np.random.sample(len(df)) < 0.1
duplicate_df = df.loc[duplicate_rows].copy()

duplicate_df["amt"] = duplicate_df["amt"] * 50

duplicate_data = pd.concat([duplicate_df] * 10, ignore_index=True)
df = pd.concat([duplicate_data, df], ignore_index=True)

# generate late arriving events
late_rows = np.random.sample(len(df)) < 0.1
late_df = df.loc[late_rows].copy()

late_df["event_ts"] -= pd.to_timedelta(
    np.random.randint(1, 4, size=len(late_df)),
    unit="D"
)

df = pd.concat([df, late_df], ignore_index=True)

# write corrupted raw dataset
df.to_csv(
    r"/mnt/c/Users/sarthak mohapatra/Downloads/archive/fraudTrain_raw_corrupted_v1.csv",
    index=False
)
