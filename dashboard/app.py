"""UrbanFlow — Dashboard Streamlit (Sprint 4).

Couche PRÉSENTATION : consomme l'API FastAPI (HTTP) et affiche une carte des stations
Vélib' (colorée par disponibilité) + le détail/prévision d'une station sélectionnée.

Lancement (depuis la racine, .venv activé, API démarrée) :
    streamlit run dashboard/app.py
"""
import sys
from pathlib import Path

import pydeck as pdk
import streamlit as st

# Sous `streamlit run`, seul le dossier du script est sur sys.path -> on ajoute la racine
# du projet pour permettre l'import `from dashboard import data`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dashboard import data  # noqa: E402

st.set_page_config(page_title="UrbanFlow — Vélib' temps réel", page_icon="🚲", layout="wide")


# --- Chargements mis en cache (le référentiel est statique ; les stations, TTL court) ---
@st.cache_data
def _reference() -> dict:
    return data.load_reference()


@st.cache_data(ttl=60)
def _stations() -> list[dict]:
    return data.get_stations()


# --- En-tête + état de l'API -------------------------------------------------
st.title("🚲 UrbanFlow — disponibilité Vélib' temps réel")
health = data.get_health()
if health.get("database") == "up":
    st.caption("🟢 API connectée — base de données joignable")
else:
    st.error(f"🔴 API/DB indisponible ({health.get('status')}). L'API FastAPI tourne-t-elle "
             f"sur {data.API_URL} et le conteneur postgres est-il démarré ?")
    st.stop()

# --- Récupération + jointure coordonnées -------------------------------------
stations = _stations()
df = data.build_map_df(stations, _reference())

# --- Bandeau de métriques ----------------------------------------------------
c1, c2, c3, c4 = st.columns(4)
c1.metric("Stations", f"{len(df):,}".replace(",", " "))
c2.metric("Vélos disponibles", f"{int(df['bikes_available'].sum()):,}".replace(",", " "))
c3.metric("Bornes libres", f"{int(df['docks_available'].sum()):,}".replace(",", " "))
c4.metric("Stations vides", int((df["bikes_available"] == 0).sum()))

# --- Carte (pydeck) ----------------------------------------------------------
st.subheader("Carte des stations")
st.caption("Couleur : 🔴 station vide → 🟢 station pleine de vélos. "
           "Survolez un point pour le détail.")
layer = pdk.Layer(
    "ScatterplotLayer",
    data=df,
    get_position="[lon, lat]",
    get_fill_color="color",
    get_radius=60,
    pickable=True,
    opacity=0.8,
)
view = pdk.ViewState(latitude=48.86, longitude=2.35, zoom=11)   # centré sur Paris
tooltip = {"text": "{name}\nVélos: {bikes_available} / {capacity}\n"
                   "Bornes libres: {docks_available}"}
st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view, tooltip=tooltip))

# --- Détail + prévision d'une station ----------------------------------------
st.subheader("Détail & prévision d'une station")
df_sorted = df.sort_values("name")
labels = df_sorted["name"] + "  (" + df_sorted["station_code"] + ")"
choice = st.selectbox("Choisir une station", options=df_sorted["station_id"],
                      format_func=lambda sid: labels[df_sorted["station_id"] == sid].iloc[0])

row = df_sorted[df_sorted["station_id"] == choice].iloc[0]
fc = data.get_forecast(int(choice))

d1, d2, d3 = st.columns(3)
d1.metric("Vélos maintenant", int(row["bikes_available"]))
d2.metric("Prévision t+15 min", f"{fc['pred_t15']:.0f}")
d3.metric("Prévision t+30 min", f"{fc['pred_t30']:.0f}")
st.caption(f"Méthode de prévision : **{fc['method']}** — à ces horizons, « dans 15/30 min ≈ "
           f"maintenant » est quasi-optimal (plafond de signal, Sprint 3).")
