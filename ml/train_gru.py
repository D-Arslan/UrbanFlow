"""UrbanFlow — Sprint 3 : 3e modele — GRU (PyTorch), cible delta + perte L1 (voir §6.9).

Modele SEQUENTIEL : au lieu de lags fabriques a la main (XGBoost), on donne au reseau
la SEQUENCE brute des K derniers pas (60 min) et il apprend lui-meme la dynamique.
GRU (Gated Recurrent Unit) = RNN a portes, plus simple/rapide qu'un LSTM.

Choix coherents avec l'option 1 (§6.8) :
  - cible = DELTA bikes(t+h) - bikes(t) ; persistance = "delta 0" ;
  - perte L1 (nn.L1Loss) -> optimise le MAE, la metrique evaluee ;
  - reconstruction bornee clip(bikes(t) + delta_hat, 0, capacite).

Anti-leakage (§6.3-6.4) :
  - fenetres STRICTEMENT contigues sur la grille 5 min (pas de couture par-dessus un trou) ;
  - meme split temporel + embargo (end-ts de la fenetre : passe -> train, futur -> test) ;
  - standardisation ajustee sur le TRAIN seulement.

Echelle : sous-echantillon de stations (demo de methode ; GRU CPU sur 1,5 M sequences =
trop lourd). Le nombre de stations gardees/ecartees est LOGGE (pas de troncature muette).

Lancement :
  python ml/train_gru.py                                      # dataset reel (sous-echantillon)
  python ml/train_gru.py --data ml/data/dataset_synth.parquet   # smoke-test
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from torch import nn

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import FEATURE_COLS, GAP_MIN, HORIZONS, TEST_FRACTION, mae_rmse  # noqa: E402

DATA_DEFAULT = Path(__file__).resolve().parent / "data" / "dataset.parquet"
MODELS_DIR = Path(__file__).resolve().parent / "models"

# --- Hyperparametres (raisonnables, non optimises) ---------------------------
K = 12            # longueur de sequence : 12 pas de 5 min = 60 min d'historique
HIDDEN = 64       # taille de l'etat cache du GRU
EPOCHS = 6
BATCH = 4096
LR = 1e-3
N_STATIONS = 200  # sous-echantillon (demo) ; loggé
SEED = 0


def select_stations(df: pd.DataFrame, n: int):
    """Sous-echantillon DETERMINISTE de n stations, reparties sur l'ensemble trie."""
    ids = np.sort(df["station_id"].unique())
    if len(ids) <= n:
        return ids
    idx = np.linspace(0, len(ids) - 1, n).astype(int)   # repartition uniforme
    return ids[np.unique(idx)]


def build_sequences(df: pd.DataFrame, col: str):
    """Construit les fenetres (N, K, F) + cible delta, en n'acceptant que des fenetres
    STRICTEMENT contigues (pas de 5 min). Renvoie X, y_delta, bikes_t, cap_t, end_ts."""
    F = len(FEATURE_COLS)
    Xs, yd, bk, cp, ets = [], [], [], [], []
    for _sid, g in df.groupby("station_id"):
        g = g.sort_values("ts")
        feats = g[FEATURE_COLS].to_numpy(dtype=np.float32)
        ts = g["ts"].to_numpy()
        bikes = g["bikes"].to_numpy(dtype=np.float32)
        cap = g["capacity"].to_numpy(dtype=np.float32)
        target = g[col].to_numpy(dtype=np.float32)
        n = len(g)
        if n < K:
            continue
        # longueur du run contigu (pas de 5 min) finissant a chaque indice
        diffs_min = (np.diff(ts).astype("timedelta64[m]")).astype(int)
        run = np.ones(n, dtype=int)
        for j in range(1, n):
            run[j] = run[j - 1] + 1 if diffs_min[j - 1] == 5 else 1
        for i in range(K - 1, n):
            if run[i] < K or np.isnan(target[i]):
                continue
            Xs.append(feats[i - K + 1: i + 1])
            yd.append(target[i] - bikes[i])
            bk.append(bikes[i]); cp.append(cap[i]); ets.append(ts[i])
    if not Xs:
        return (np.empty((0, K, F), np.float32), np.empty(0, np.float32),
                np.empty(0, np.float32), np.empty(0, np.float32),
                np.empty(0, "datetime64[ns]"))
    return (np.stack(Xs), np.asarray(yd, np.float32), np.asarray(bk, np.float32),
            np.asarray(cp, np.float32), np.asarray(ets, "datetime64[ns]"))


class GRUReg(nn.Module):
    def __init__(self, n_features: int, hidden: int):
        super().__init__()
        self.gru = nn.GRU(n_features, hidden, batch_first=True)
        self.head = nn.Linear(hidden, 1)

    def forward(self, x):
        out, _ = self.gru(x)            # out: (B, K, hidden)
        return self.head(out[:, -1, :]).squeeze(-1)   # dernier pas -> scalaire (delta)


def run_horizon(df: pd.DataFrame, label: str, col: str) -> None:
    torch.manual_seed(SEED)
    X, y, bikes_t, cap_t, end_ts = build_sequences(df, col)
    if len(X) == 0:
        print(f"{label:<8}  (aucune sequence contigue — donnees insuffisantes)")
        return

    # Split temporel par end-ts de la fenetre (meme logique que common.temporal_split).
    cut = np.datetime64(pd.Series(end_ts).quantile(1 - TEST_FRACTION))
    gap = np.timedelta64(GAP_MIN, "m")       # unite explicite (evite le warning numpy 2.x)
    tr = end_ts <= (cut - gap)
    te = end_ts > cut
    if tr.sum() == 0 or te.sum() == 0:
        print(f"{label:<8}  (train/test vide)")
        return

    # Standardisation ajustee sur le TRAIN seul (anti-leakage), appliquee partout.
    flat = X[tr].reshape(-1, X.shape[2])
    mu, sd = flat.mean(0), flat.std(0) + 1e-6
    Xn = (X - mu) / sd

    Xtr = torch.from_numpy(Xn[tr]); ytr = torch.from_numpy(y[tr])
    Xte = torch.from_numpy(Xn[te])

    model = GRUReg(X.shape[2], HIDDEN)
    opt = torch.optim.Adam(model.parameters(), lr=LR)
    loss_fn = nn.L1Loss()                      # L1 = optimise le MAE (comme la v2 XGB)

    n_tr = Xtr.shape[0]
    for ep in range(EPOCHS):
        model.train()
        perm = torch.randperm(n_tr)
        tot = 0.0
        for s in range(0, n_tr, BATCH):
            b = perm[s:s + BATCH]
            opt.zero_grad()
            loss = loss_fn(model(Xtr[b]), ytr[b])
            loss.backward()
            opt.step()
            tot += loss.item() * len(b)
        print(f"   [{label}] epoch {ep + 1}/{EPOCHS} — L1 train (delta) = {tot / n_tr:.4f}")

    # Prediction + reconstruction bornee, evaluation sur l'ABSOLU vs persistance.
    model.eval()
    with torch.no_grad():
        preds = []
        for s in range(0, Xte.shape[0], BATCH):
            preds.append(model(Xte[s:s + BATCH]).numpy())
    delta_hat = np.concatenate(preds)
    y_true = bikes_t[te] + y[te]                       # absolu vrai = bikes(t) + delta_vrai
    pred_abs = np.clip(bikes_t[te] + delta_hat, 0, cap_t[te])

    mae_g, rmse_g = mae_rmse(y_true, pred_abs)
    mae_b, rmse_b = mae_rmse(y_true, bikes_t[te])      # persistance
    gain = 100 * (mae_b - mae_g) / mae_b
    print(f"{label:<8} n_test={te.sum():>8,}  MAE_base={mae_b:.3f}  MAE_gru={mae_g:.3f}"
          f"  RMSE_base={rmse_b:.3f}  RMSE_gru={rmse_g:.3f}  gain_MAE={gain:+.1f}%")

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    out = MODELS_DIR / f"gru_{col}.pt"
    torch.save({"state_dict": model.state_dict(), "mu": mu, "sd": sd,
                "K": K, "hidden": HIDDEN, "features": FEATURE_COLS}, out)
    print(f"         -> modele sauvegarde : {out}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=str(DATA_DEFAULT))
    ap.add_argument("--stations", type=int, default=N_STATIONS)
    a = ap.parse_args()

    df = pd.read_parquet(a.data)
    total = df["station_id"].nunique()
    keep = select_stations(df, a.stations)
    df = df[df["station_id"].isin(keep)]
    print(f"[GRU] sous-echantillon : {len(keep):,} / {total:,} stations gardees "
          f"({total - len(keep):,} ecartees) | {len(df):,} lignes\n")

    for label, col in HORIZONS:
        run_horizon(df, label, col)

    print("\n[GRU] gain_MAE > 0 = le GRU bat la persistance (attente : marginal, cf. plafond §6.8).")


if __name__ == "__main__":
    main()
