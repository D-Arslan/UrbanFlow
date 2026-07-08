"""Couche PRÉDICTION — abstraction remplaçable (une seule fonction : predict()).

Implémentation par défaut : PERSISTANCE. La meilleure estimation de la disponibilité
future proche = la disponibilité actuelle. Sprint 3 l'a démontré empiriquement (plafond de
signal : aucun modèle ne bat nettement la persistance à t+15/t+30 — voir learning.md §6.8).

Pour brancher un vrai modèle (XGBoost/GRU sérialisés dans ml/models/), il suffit de
réécrire predict() : l'API et le dashboard ne changent pas d'une ligne (couplage minimal).
"""

# Nom de la méthode servie (exposé dans la réponse pour la transparence du contrat).
METHOD = "persistence"


def predict(bikes_now: float | None) -> dict[str, float | None]:
    """Prédit la disponibilité à t+15 et t+30 depuis l'état courant.

    Persistance : on renvoie l'état actuel pour les deux horizons. Si l'état est inconnu
    (None), la prédiction l'est aussi (on ne fabrique pas de valeur).
    """
    return {"t+15": bikes_now, "t+30": bikes_now}
