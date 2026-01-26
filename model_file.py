import torch.nn as nn
import pandas as pd
import numpy as np
import torch
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

df = pd.read_csv("/mnt/c/Users/sarthak mohapatra/Downloads/archive/silver_file.csv")
# print(df.columns)
LABEL_COL = "is_fraud"
TIME_COL = "event_ts"

# numeric features only
NUM_FEATURES = [
    "amt", "lat", "long", "city_pop",
    "unix_time", "merch_lat", "merch_long"
]

BATCH_SIZE = 256
EPOCHS = 10
LR = 1e-3
df[TIME_COL] = pd.to_datetime(df[TIME_COL])
df = df[
    (df["amt"] > 0) &
    (df["lat"].between(-90, 90)) &
    (df["long"].between(-180, 180)) &
    (df["merch_lat"].between(-90, 90)) &
    (df["merch_long"].between(-180, 180)) &
    (df["unix_time"] > 0)
]
df["amt"] = np.log1p(df["amt"])
df["city_pop"] = np.log1p(df["city_pop"])
df = df.sort_values(TIME_COL)

X = df[NUM_FEATURES].values
y = df[LABEL_COL].values.astype(np.float32)

scaler = StandardScaler()
X = scaler.fit_transform(X)
split_idx = int(len(df) * 0.8)
X_train = X[:split_idx]
X_val = X[split_idx:]
y_train = y[:split_idx]
y_val = y[split_idx:]
pos_weight = (len(y_train) - y_train.sum()) / y_train.sum()
