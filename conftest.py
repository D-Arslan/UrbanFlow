# Présence à la racine : pytest ajoute ce dossier au sys.path -> les tests peuvent faire
# `from api.main import app` / `from dashboard import data` sans installation du projet.
