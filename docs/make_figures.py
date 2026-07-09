"""Génère les figures du README depuis les vraies données (reproductible).

Sorties :
  docs/dashboard_map.png  — carte des stations colorée par disponibilité (état chaud).
  docs/lift_curve.png     — gain XGBoost vs persistance par horizon (MAE & RMSE).

Prérequis : conteneur postgres démarré (docker compose up -d postgres), matplotlib installé
(pip install matplotlib — dépendance d'ASSET, pas de runtime). Lancement :
  python docs/make_figures.py
"""
import json
import os
from pathlib import Path

import matplotlib
import numpy as np
import psycopg
from dotenv import load_dotenv

matplotlib.use("Agg")                      # backend fichier (pas de fenêtre)
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
load_dotenv(ROOT / ".env")

# --- Thème sombre (aligné sur la palette dataviz + le dashboard Streamlit) ---
SURFACE = "#1a1a19"
PAGE = "#0d0d0d"
INK = "#ffffff"
INK2 = "#c3c2b7"
MUTED = "#898781"
GRID = "#2c2c2a"
BLUE = "#3987e5"       # série 1 (MAE)
ORANGE = "#d95926"     # série 8 (RMSE) — paire bleu/orange CVD-safe
# Carte : statut critique -> warning -> bon (vide -> plein), couleurs de la palette.
AVAIL_CMAP = LinearSegmentedColormap.from_list("avail", ["#d03b3b", "#fab219", "#0ca30c"])

CONNINFO = (
    f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
    f"port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ.get('POSTGRES_DB', 'urbanflow')} "
    f"user={os.environ.get('POSTGRES_USER', 'urbanflow')} "
    f"password={os.environ.get('POSTGRES_PASSWORD', '')}"
)


def make_map() -> None:
    """Scatter des stations (lon/lat) coloré par vélos/capacité."""
    ref = {s["station_id"]: s
           for s in json.loads((DOCS.parent / "dashboard" / "stations_information.json")
                                .read_text(encoding="utf-8"))}
    with psycopg.connect(CONNINFO) as c:
        rows = c.execute("SELECT station_id, avg_bikes_available "
                         "FROM station_availability_current").fetchall()

    lon, lat, fill = [], [], []
    for sid, bikes in rows:
        r = ref.get(sid)
        if not r or r.get("lat") is None or not r.get("capacity"):
            continue
        lon.append(r["lon"])
        lat.append(r["lat"])
        fill.append(min((bikes or 0) / r["capacity"], 1.0))

    fig, ax = plt.subplots(figsize=(9, 7.2), dpi=130)
    fig.patch.set_facecolor(PAGE)
    ax.set_facecolor(SURFACE)
    sc = ax.scatter(lon, lat, c=fill, cmap=AVAIL_CMAP, s=9, alpha=0.9, linewidths=0)
    ax.set_aspect(1 / np.cos(np.radians(48.86)))     # correction lat pour Paris
    ax.set_title(f"Disponibilité Vélib' en Île-de-France  ·  {len(fill)} stations",
                 color=INK, fontsize=15, pad=14, loc="left")
    ax.text(0.0, 1.005, "Rouge = station vide  ·  Vert = station pleine de vélos",
            transform=ax.transAxes, color=MUTED, fontsize=9.5, va="bottom")
    for s in ax.spines.values():
        s.set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])
    cb = fig.colorbar(sc, ax=ax, fraction=0.032, pad=0.02)
    cb.set_label("vélos / capacité", color=INK2, fontsize=10)
    cb.ax.yaxis.set_tick_params(color=MUTED, labelcolor=INK2)
    cb.outline.set_visible(False)
    fig.tight_layout()
    out = DOCS / "dashboard_map.png"
    fig.savefig(out, facecolor=PAGE, bbox_inches="tight")
    print(f"[FIG] {out} ({len(fill)} stations)")


def make_lift() -> None:
    """Courbe de lift : gain % du XGBoost sur la persistance, par horizon."""
    horizon = np.array([15, 30, 60, 120])
    mae_base = np.array([0.754, 1.188, 1.818, 2.764])
    mae_xgb = np.array([0.810, 1.251, 1.867, 2.764])
    rmse_base = np.array([1.446, 2.098, 3.035, 4.419])
    rmse_xgb = np.array([1.430, 2.066, 2.955, 4.202])
    gain_mae = 100 * (mae_base - mae_xgb) / mae_base
    gain_rmse = 100 * (rmse_base - rmse_xgb) / rmse_base

    fig, ax = plt.subplots(figsize=(8, 5), dpi=130)
    fig.patch.set_facecolor(PAGE)
    ax.set_facecolor(SURFACE)
    ax.axhline(0, color=MUTED, lw=1, ls="--", zorder=1)      # persistance = référence
    ax.text(122, 0.15, "persistance (référence)", color=MUTED, fontsize=8.5, ha="right")
    ax.plot(horizon, gain_rmse, "-o", color=ORANGE, lw=2.2, ms=7, label="gain RMSE", zorder=3)
    ax.plot(horizon, gain_mae, "-o", color=BLUE, lw=2.2, ms=7, label="gain MAE", zorder=3)
    for x, y in zip(horizon, gain_rmse):
        ax.annotate(f"+{y:.1f}%", (x, y), color=ORANGE, fontsize=9,
                    xytext=(0, 8), textcoords="offset points", ha="center")
    for x, y in zip(horizon, gain_mae):
        ax.annotate(f"{y:+.1f}%", (x, y), color=BLUE, fontsize=9,
                    xytext=(0, -14), textcoords="offset points", ha="center")

    ax.set_title("Le modèle gagne son utilité à horizon long",
                 color=INK, fontsize=15, pad=12, loc="left")
    ax.text(0.0, 1.005, "Gain XGBoost vs persistance (au-dessus de 0 = le modèle fait mieux)",
            transform=ax.transAxes, color=MUTED, fontsize=9.5, va="bottom")
    ax.set_xlabel("horizon de prédiction (minutes)", color=INK2, fontsize=10.5)
    ax.set_ylabel("gain sur l'erreur (%)", color=INK2, fontsize=10.5)
    ax.set_xticks(horizon)
    ax.tick_params(colors=MUTED, labelcolor=INK2)
    ax.grid(True, color=GRID, lw=0.7)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("bottom", "left"):
        ax.spines[s].set_color(GRID)
    leg = ax.legend(facecolor=SURFACE, edgecolor=GRID, labelcolor=INK2,
                    fontsize=10, loc="center left")
    leg.get_frame().set_linewidth(0.7)
    fig.tight_layout()
    out = DOCS / "lift_curve.png"
    fig.savefig(out, facecolor=PAGE, bbox_inches="tight")
    print(f"[FIG] {out}")


if __name__ == "__main__":
    make_map()
    make_lift()
