"""Tests de la couche données du dashboard — fonction pure build_map_df (sans réseau)."""
from dashboard import data


def test_build_map_df_joins_and_filters():
    stations = [
        {"station_id": 1, "station_code": "0001", "bikes_available": 5.0, "docks_available": 5.0},
        {"station_id": 2, "station_code": "0002", "bikes_available": 0.0, "docks_available": 10.0},
        {"station_id": 3, "station_code": "0003", "bikes_available": 3.0, "docks_available": 2.0},
    ]
    reference = {
        1: {"name": "A", "lat": 48.8, "lon": 2.3, "capacity": 10},
        2: {"name": "B", "lat": 48.9, "lon": 2.4, "capacity": 10},
        # station 3 : ABSENTE du référentiel -> doit être écartée (pas de coordonnées)
    }
    df = data.build_map_df(stations, reference)

    # station 3 exclue faute de coordonnées
    assert set(df["station_id"]) == {1, 2}
    # fill_ratio = vélos / capacité
    assert df.loc[df["station_id"] == 1, "fill_ratio"].iloc[0] == 0.5
    assert df.loc[df["station_id"] == 2, "fill_ratio"].iloc[0] == 0.0
    # nom joint depuis le référentiel
    assert df.loc[df["station_id"] == 1, "name"].iloc[0] == "A"


def test_build_map_df_handles_null_capacity():
    """Capacité nulle/0 -> fill_ratio = 0 (pas de division par zéro)."""
    stations = [{"station_id": 1, "station_code": "0001",
                 "bikes_available": 4.0, "docks_available": 1.0}]
    reference = {1: {"name": "A", "lat": 48.8, "lon": 2.3, "capacity": None}}
    df = data.build_map_df(stations, reference)
    assert df["fill_ratio"].iloc[0] == 0.0
