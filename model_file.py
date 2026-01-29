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
class Frauddataset(Dataset):
    def __init__(self,X,y):
        self.X = torch.tensor(X, dtype=torch.float32)
        self.y = torch.tensor(y, dtype=torch.float32)
    def __len__(self):
        return len(self.X)
    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]
train_loader = DataLoader(
    Frauddataset(X_train, y_train),
    batch_size=BATCH_SIZE,
    shuffle=True
)
val_loader = DataLoader(
    Frauddataset(X_val, y_val),
    batch_size=BATCH_SIZE
)
class MLP(nn.Module):
    def __init__(self,input_dim):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.ReLU(),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 1)
        )
    def forward(self, x):
        return self.net(x).squeeze(1)
model = MLP(len(NUM_FEATURES))
criterion = nn.BCEWithLogitsLoss(
    pos_weight=torch.tensor(pos_weight)
)
optimizer = torch.optim.Adam(model.parameters(), lr=LR)

for epoch in range(EPOCHS):
    model.train()
    train_loss = 0.0
    for xb, yb in train_loader:
        optimizer.zero_grad()
        logits = model(xb)
        loss = criterion(logits, yb)
        loss.backward()
        optimizer.step()
        train_loss += loss.item()

    model.eval()
    val_loss = 0.0
    with torch.no_grad():
        for xb, yb in val_loader:
            logits = model(xb)
            loss = criterion(logits, yb)
            val_loss += loss.item()

    print(
        f"epoch={epoch+1} "
        f"train_loss={train_loss/len(train_loader):.4f} "
        f"val_loss={val_loss/len(val_loader):.4f}"
    )

torch.save(
    {
        "model_state": model.state_dict(),
        "scaler": scaler,
        "features": NUM_FEATURES
    },
    "fraud_model.pt"
)

print("done")