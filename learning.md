# UrbanFlow — Journal d'apprentissage

> Document vivant : on y consigne **chaque concept, définition et choix d'architecture**
> au fil du projet. Il sert de source unique pour le rapport final (LaTeX).

## Conventions de ce document

- Chaque concept = une **fiche** avec le même gabarit :
  **Définition · Rôle dans UrbanFlow · Pourquoi ce choix · Alternatives · Angle recruteur**.
- Hiérarchie de titres pensée pour LaTeX : `#` → `\chapter`/`section`, `##` → `\section`,
  `###` → `\subsection`.
- Les termes techniques restent en **anglais** (vocabulaire standard du métier),
  les explications en **français**.
- Un **glossaire** synthétique est maintenu en fin de document.

---

# Table des matières

1. [Vision & architecture générale](#1-vision--architecture-générale)
2. [Sprint 1 — Ingestion & streaming minimal](#2-sprint-1--ingestion--streaming-minimal)
   - 2.1 [Concept : Apache Kafka](#21-concept--apache-kafka)
   - 2.2 [Pourquoi un broker plutôt que API → DB direct](#22-pourquoi-un-broker-plutôt-que-api--db-direct)
   - 2.3 [Concept : Docker, image, conteneur, volume, Compose](#23-concept--docker-image-conteneur-volume-compose)
   - 2.4 [Choix d'architecture : Kafka KRaft vs Zookeeper](#24-choix-darchitecture--kafka-kraft-vs-zookeeper)
   - 2.5 [Piège classique : LISTENERS vs ADVERTISED_LISTENERS](#25-piège-classique--listeners-vs-advertised_listeners)
   - 2.6 [Environnement virtuel Python & piège de dépendance](#26-environnement-virtuel-python--piège-de-dépendance)
   - 2.7 [Ingestion : poller (producer) & consumer de test](#27-ingestion--poller-producer--consumer-de-test)
3. [Questions type recruteur (Sprint 1)](#3-questions-type-recruteur-sprint-1)
4. [Sprint 2 — Traitement Spark & double stockage](#4-sprint-2--traitement-spark--double-stockage)
   - 4.1 [Concept : Spark Structured Streaming](#41-concept--spark-structured-streaming)
   - 4.2 [Event-time vs processing-time](#42-event-time-vs-processing-time)
   - 4.3 [Fenêtres temporelles (tumbling / sliding)](#43-fenêtres-temporelles-tumbling--sliding)
   - 4.4 [Watermark](#44-watermark)
   - 4.5 [Choix d'architecture : état chaud (PostgreSQL) vs froid (Parquet/MinIO)](#45-choix-darchitecture--état-chaud-postgresql-vs-froid-parquetminio)
   - 4.6 [Concept : MinIO & stockage objet S3](#46-concept--minio--stockage-objet-s3)
   - 4.7 [foreachBatch & upsert](#47-foreachbatch--upsert)
   - 4.8 [Le job complet & validation bout-en-bout](#48-le-job-complet--validation-bout-en-bout)
   - 4.9 [Pièges rencontrés (Sprint 2)](#49-pièges-rencontrés-sprint-2)
5. [Questions type recruteur (Sprint 2)](#5-questions-type-recruteur-sprint-2)
6. [Sprint 3 — Couche ML (Machine Learning)](#6-sprint-3--couche-ml-machine-learning)
   - 6.1 [Pourquoi une baseline d'abord (persistance)](#61-pourquoi-une-baseline-dabord-persistance)
   - 6.2 [Métriques : MAE & RMSE](#62-métriques--mae--rmse)
   - 6.3 [Fuite de données (data leakage) & cible décalée](#63-fuite-de-données-data-leakage--cible-décalée)
   - 6.4 [Évaluation d'une régression temporelle (split temporel)](#64-évaluation-dune-régression-temporelle-split-temporel)
   - 6.5 [Choix d'architecture : lecture de l'historique (Spark → pandas)](#65-choix-darchitecture--lecture-de-lhistorique-spark--pandas)
   - 6.6 [Piège majeur : « le poller a tourné » ≠ « le Parquet a l'historique »](#66-piège-majeur--le-poller-a-tourné--le-parquet-a-lhistorique)
   - 6.7 [Chaîne de modélisation : baseline, XGBoost, inférence](#67-chaîne-de-modélisation--baseline-xgboost-inférence)
   - 6.8 [Résultats du premier run réel & le paradoxe MAE/RMSE](#68-résultats-du-premier-run-réel--le-paradoxe-maermse)
   - 6.9 [Corriger le paradoxe : delta + L1, GRU, et le plafond de signal](#69-corriger-le-paradoxe--delta--l1-gru-et-le-plafond-de-signal)
   - 6.10 [Horizons longs : la courbe de lift](#610-horizons-longs--la-courbe-de-lift)
7. [Questions type recruteur (Sprint 3)](#7-questions-type-recruteur-sprint-3)
8. [Sprint 4 — Service (API), dashboard & CI](#8-sprint-4--service-api-dashboard--ci)
   - 8.1 [Concept : API REST & FastAPI](#81-concept--api-rest--fastapi)
   - 8.2 [Architecture en couches de l'API](#82-architecture-en-couches-de-lapi)
   - 8.3 [Choix : que sert l'API ? Persistance vs XGBoost réel](#83-choix--que-sert-lapi--persistance-vs-xgboost-réel)
   - 8.4 [L'abstraction predictor remplaçable](#84-labstraction-predictor-remplaçable)
   - 8.5 [Concept : Streamlit & architecture présentation → service](#85-concept--streamlit--architecture-présentation--service)
   - 8.6 [Le problème des coordonnées (station_status vs station_information)](#86-le-problème-des-coordonnées-station_status-vs-station_information)
   - 8.7 [Tester l'API sans base : monkeypatch](#87-tester-lapi-sans-base--monkeypatch)
   - 8.8 [CI GitHub Actions](#88-ci-github-actions)
   - 8.9 [Pièges rencontrés (Sprint 4)](#89-pièges-rencontrés-sprint-4)
9. [Questions type recruteur (Sprint 4)](#9-questions-type-recruteur-sprint-4)
10. [Glossaire](#10-glossaire)

---

# 1. Vision & architecture générale

**UrbanFlow** est un pipeline de données **temps réel** sur la mobilité francilienne
(Vélib' / IDFM). La chaîne : *ingestion → streaming → traitement → stockage → ML → service*.

Principe directeur : une donnée brute entre par un **producer**, transite par un **broker**
(Kafka), et est consommée par un ou plusieurs **consumers** indépendants.

---

# 2. Sprint 1 — Ingestion & streaming minimal

**Objectif :** établir le "tuyau" minimal de bout en bout —
API → poller (producer) → Kafka → consumer de test.
Aucun traitement avancé (Spark, ML, dashboard) à ce stade.

**Ordre de construction et justification :**

| Étape | Quoi | Pourquoi à ce moment |
|-------|------|----------------------|
| 1 | Structure du repo | Poser un terrain propre avant de bâtir. |
| 2 | Docker Compose (Kafka + PostgreSQL) | L'infra doit exister avant de coder contre elle. |
| 3 | Poller Python (producer) | Récupérer la donnée réelle et la publier. |
| 4 | Consumer de test | Prouver que la donnée circule de bout en bout. |

---

## 2.1 Concept : Apache Kafka

**Définition.** Apache Kafka est une **plateforme de streaming d'événements distribuée** :
un système qui reçoit, **stocke durablement** (sur disque) et distribue des flux de messages,
selon un modèle **publish/subscribe**.

**Image mentale.** Un tapis roulant de messages, infini et enregistré : des programmes
**déposent** des messages d'un côté, d'autres les **lisent dans l'ordre** de l'autre.

**Vocabulaire de base.**

| Terme | Définition | Dans UrbanFlow |
|-------|------------|----------------|
| **Broker** | Le serveur Kafka qui reçoit/stocke/sert les messages. | Le conteneur Kafka du `docker-compose`. |
| **Topic** | Un canal nommé, une catégorie de messages. | `velib.stations.raw` (relevés bruts). |
| **Partition** | Sous-division d'un topic → parallélisme + ordre garanti *au sein* d'une partition. | 1 partition au Sprint 1. |
| **Producer** | Programme qui **écrit** dans un topic. | Le poller Python. |
| **Consumer** | Programme qui **lit** un topic. | Le consumer de test, puis Spark. |
| **Offset** | Numéro de position d'un message dans une partition (un marque-page). | Permet de reprendre la lecture où on s'était arrêté. |
| **Rétention** | Durée pendant laquelle Kafka conserve les messages. | Ex. 7 jours, puis suppression automatique. |

**Rôle dans UrbanFlow.** Kafka est le **point de passage central** entre l'ingestion
et tout le reste. Le poller publie une fois ; les consumers (Postgres, Spark, archivage)
se branchent indépendamment.

**Angle recruteur.**
> *« Que se passe-t-il si un consumer tombe en panne 10 min ? »*
> Le producer continue d'écrire dans le topic ; Kafka persiste tout sur disque dans la
> limite de la **rétention**. Au redémarrage, le consumer reprend à son **offset** et
> rattrape le retard, dans l'ordre, sans perte — **sauf** si la panne dépasse la rétention.

---

## 2.2 Pourquoi un broker plutôt que API → DB direct

**La question.** Pourquoi ne pas faire simplement `poller → écrit dans PostgreSQL` ?

**Les 4 raisons (à connaître par cœur).**

1. **Découplage.** Producer et consumer évoluent indépendamment. Si la base est lente
   ou redémarre, le poller dépose dans le topic et continue ; il n'est pas bloqué.

2. **Tampon (pics & pannes).** Kafka **absorbe** les pics de charge (1500 stations d'un
   coup) et les pannes aval en stockant sur disque ; le consumer rattrape à son rythme.

3. **Durabilité & rejouabilité.** La donnée brute reste dans Kafka (rétention). En cas de
   bug de traitement, on peut **rejouer** depuis un offset. En écriture directe, la donnée
   brute est perdue après transformation.

4. **Multi-consumers (pub/sub).** La même donnée doit aller à Postgres *ET* Spark *ET*
   l'archivage. Le poller publie **une fois** ; chacun se branche sur le topic.

**Alternatives écartées.**

| Approche | Pourquoi écartée |
|----------|------------------|
| API → PostgreSQL direct | Pas de tampon, pas de rejeu, couplage fort, un seul consommateur. |
| File d'attente simple (Redis, RabbitMQ) | OK pour des tâches, mais moins adapté au **rejeu** et au stockage durable de flux que Kafka. |

**À retenir (phrase de rapport).**
> *Kafka découple l'ingestion du traitement et sert de tampon durable, rendant le pipeline
> résilient aux pannes, aux pics de charge, et extensible à plusieurs consommateurs.*

> ⚠️ *Idées reçues à éviter :* Kafka **n'est pas** là pour la "sécurité" ni pour la
> "cohérence de la base" (ça, c'est le rôle de PostgreSQL). C'est un **bus d'événements**.

---

## 2.3 Concept : Docker, image, conteneur, volume, Compose

**Le problème résolu.** « Ça marche sur ma machine » : chaque logiciel (Kafka → Java,
PostgreSQL → libs système) a des dépendances précises. Les installer à la main est
fragile, polluant et non reproductible. Docker **empaquette** chaque logiciel avec son
environnement dans une boîte isolée.

**Définitions.**

| Terme | Définition | Analogie |
|-------|------------|----------|
| **Image** | Modèle **figé, en lecture seule**, empaquetant une application + ses dépendances/runtime/versions. | La recette. |
| **Conteneur** | **Instance qui tourne**, créée à partir d'une image. Jetable. | Le plat cuisiné. |
| **Volume** | Stockage **persistant** qui survit à la suppression du conteneur. | Le frigo. |
| **Docker Compose** | Fichier `docker-compose.yml` décrivant **plusieurs services** et leurs liens, lancés ensemble. | La carte du menu complet. |

**Point critique.** Un conteneur est **jetable** : le supprimer efface son contenu,
**sauf** ce qui est dans un **volume**. → Les données PostgreSQL vont dans un volume,
sinon chaque relance = base vide.

**Compose = Infrastructure as Code.** L'infra (Kafka + Postgres + réseau + volumes) est
**décrite dans un fichier versionné**. Une commande (`docker compose up`) démarre tout.
Reproductible à l'identique sur n'importe quelle machine.

**Angle recruteur.**
> *« Différence image / conteneur ? »* → Image = modèle figé en lecture seule ;
> conteneur = instance en exécution issue de l'image (plusieurs conteneurs peuvent
> naître d'une même image).
> *« Pourquoi un volume ? »* → Persister les données au-delà du cycle de vie (jetable)
> du conteneur.

---

## 2.4 Choix d'architecture : Kafka KRaft vs Zookeeper

**Le besoin.** Un cluster Kafka doit **coordonner** ses brokers : élire un contrôleur,
savoir quelle partition vit sur quel broker, suivre les métadonnées du cluster.
Historiquement, ce rôle était délégué à un service **externe** : **Zookeeper**.

**Les deux options.**

| Critère | Zookeeper (historique) | **KRaft** (choisi) ✅ |
|---------|------------------------|----------------------|
| Architecture | 2 services : Kafka **+** Zookeeper | 1 seul service : Kafka gère ses propres métadonnées |
| Conteneurs à lancer | 2 (Kafka + Zookeeper) | 1 |
| Empreinte RAM | Plus élevée (2 JVM) | Plus légère (1 JVM) |
| Coordination | Externe (Zookeeper, protocole ZAB) | Interne (quorum **Raft** entre nœuds *controller*) |
| Statut | **Déprécié** — retiré dans Kafka 4.0 | **Standard** depuis Kafka 3.3+, défaut moderne |
| Documentation | Beaucoup de vieux tutos | Docs récentes |

**Choix retenu : KRaft.** Justification :
1. **Économie de ressources** — un conteneur (et une JVM) de moins, important vu la RAM
   limitée de la machine de dev.
2. **Pérennité** — Zookeeper est officiellement supprimé à partir de Kafka 4.0.
   Apprendre KRaft = apprendre le Kafka **actuel**.
3. **Simplicité opérationnelle** — un seul service à démarrer, configurer, monitorer.
4. **Signal portfolio** — démontre la connaissance des pratiques **à jour**.

**Ce qu'impliquerait Zookeeper à la place (l'alternative documentée).**
- Un **second conteneur** `zookeeper` dans le `docker-compose.yml`, avec ses propres
  ports (2181), volume et configuration.
- Kafka configuré pour **pointer vers Zookeeper** (`KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181`)
  au lieu des variables KRaft (`KAFKA_PROCESS_ROLES`, `KAFKA_CONTROLLER_QUORUM_VOTERS`…).
- **Dépendance d'ordre de démarrage** : Zookeeper doit être prêt **avant** Kafka
  (`depends_on` + healthcheck), sinon Kafka plante au boot.
- **+1 JVM** en mémoire et un composant de plus à superviser, patcher, sécuriser.
- Un point de défaillance supplémentaire dans le cluster.

**À retenir (phrase de rapport).**
> *KRaft supprime la dépendance externe à Zookeeper en intégrant la gestion des
> métadonnées dans Kafka via un quorum Raft, simplifiant le déploiement et réduisant
> l'empreinte ressources — désormais le mode par défaut depuis Kafka 3.3+.*

> **Notion de quorum Raft (pour aller plus loin).** Raft est un algorithme de consensus
> distribué : un ensemble de nœuds élit un *leader* et réplique un journal d'opérations ;
> une décision est validée quand une **majorité** (quorum) est d'accord. En mono-nœud
> (notre cas Sprint 1), le quorum est trivial (1 voteur). Concept clé des systèmes
> distribués → à savoir expliquer.

---

## 2.5 Piège classique : `LISTENERS` vs `ADVERTISED_LISTENERS`

**Le problème.** Un client Kafka se connecte en **deux temps** :
1. Il contacte une **adresse de bootstrap** pour découvrir le cluster.
2. Kafka lui **renvoie l'adresse à laquelle se reconnecter réellement** → c'est
   l'**`ADVERTISED_LISTENERS`**. Le client utilise *cette* adresse pour tout le reste.

**Distinction.**

| Paramètre | Rôle |
|-----------|------|
| `LISTENERS` | Les interfaces/ports sur lesquels Kafka **écoute** réellement (ex. `0.0.0.0:9092`). |
| `ADVERTISED_LISTENERS` | L'adresse que Kafka **annonce** aux clients pour qu'ils s'y reconnectent. |

**Analogie.** `LISTENERS` = numéro du standard ; `ADVERTISED_LISTENERS` = le poste direct
que la standardiste te donne. Mauvais poste annoncé → tu n'atteins jamais l'interlocuteur,
même si le standard répond.

**Conséquence pratique dans UrbanFlow.**
- Sprint 1 : poller **sur l'hôte** → annoncer `PLAINTEXT://localhost:9092`.
- Sprint 2 : poller **dans un conteneur** → il faudra **deux listeners** (un pour l'hôte
  via `localhost`, un pour le réseau Docker via le nom de service `kafka`), sinon le client
  dockerisé reçoit `localhost` et n'atteint pas le broker.

**Bug typique 1.** Annoncer `kafka:9092` à un client hôte → le client tente de résoudre
`kafka` dans le DNS de l'hôte (inexistant) → échec, alors que le port répondait.

**Bug typique 2 (rencontré au Sprint 1).** Kafka refuse de démarrer avec :
`advertised.listeners cannot use the nonroutable meta-address 0.0.0.0`.
- Cause : `0.0.0.0` (= « toutes les interfaces ») écrit en dur dans `KAFKA_LISTENERS`
  se propageait jusqu'à `advertised.listeners`, où la méta-adresse est interdite (on ne
  peut pas *annoncer* « toutes les interfaces » à un client).
- Correctif : utiliser l'**hôte vide** `PLAINTEXT://:9092` (même sens pour l'écoute, sans
  injecter `0.0.0.0`), et garder un `advertised.listeners` **routable** (`localhost:9092`).
- Règle : `0.0.0.0` est OK pour **écouter**, jamais pour **annoncer**.

**Réflexe de débogage appris.** Un conteneur absent de `docker compose ps` (mais présent
en `ps -a` avec `Exited (1)`) = il a crashé au démarrage. La cause est dans
`docker compose logs <service>` (dernières lignes, souvent une exception).

**Notion liée : `replication.factor` & `min.insync.replicas`.**
- `replication.factor` = nombre de copies d'une donnée sur des brokers distincts.
- Mono-nœud (Sprint 1) → forcément `1` (Kafka refuse de démarrer sinon).
- Production (≥3 brokers) → `3` pour tolérer la perte d'un broker ; `min.insync.replicas=2`
  refuse une écriture si moins de 2 copies sont à jour (garantie de durabilité).

---

## 2.6 Environnement virtuel Python & piège de dépendance

**`venv`.** Un environnement virtuel isole les dépendances d'un projet dans un dossier
dédié (`.venv/`), sans polluer le Python système ni les autres projets. On versionne la
**liste** (`requirements.txt`), jamais les paquets eux-mêmes (`.venv/` est gitignoré).
Analogie : c'est aux dépendances Python ce que Docker est aux services.

Commandes :
- Créer : `python -m venv .venv`
- Activer (PowerShell) : `.\.venv\Scripts\Activate.ps1`
- Installer : `pip install -r requirements.txt`

**Piège rencontré (Sprint 1).** `kafka-python==2.0.2` (version la plus citée dans les
vieux tutos) **plante à l'import sous Python 3.12** :
`ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`.
Cause : version de 2020, jamais adaptée aux Python récents.
Correctif : `kafka-python==3.0.2` (branche reprise en 2025, compatible 3.12).
Leçon : **toujours tester l'installation** ; ne pas copier une version d'un tuto ancien.

---

## 2.7 Ingestion : poller (producer) & consumer de test

**Source : API GBFS Vélib'.** Flux `station_status.json` (opérateur Smovengo), ~1517
stations, JSON. Champs clés : `station_id`, `num_bikes_available`, `num_docks_available`,
`last_reported`. Le flux dévie un peu du standard (clés en double snake/camelCase,
`lastUpdatedOther`). Le champ `ttl` annoncé (3600 s) est **trompeur** : la donnée bouge
~chaque minute → on choisit un intervalle de polling raisonnable (60 s), sans suivre le
`ttl` aveuglément.

**Polling.** L'API ne *pousse* rien : on l'**interroge en boucle** (*to poll*).
Boucle = interroger → publier → attendre l'intervalle → recommencer.

**Producer Kafka (`kafka-python`).**
- **Sérialisation** : Kafka ne transporte que des octets → `dict → JSON → bytes`
  (`value_serializer`).
- **Clé du message = `station_id`** : *même clé → même partition* → ordre garanti par
  station quand on aura plusieurs partitions (Sprint 2).
- **`flush()`** : force l'envoi du buffer et attend la confirmation du broker.
- **Boucle résiliente** : `try/except` interne attrape les erreurs (API timeout/5xx,
  Kafka) → log + on continue ; le poller ne meurt jamais sur une panne ponctuelle.
  `KeyboardInterrupt` + `finally` → fermeture propre.

**Consumer Kafka.**
- **Consumer group** (`group_id`) : Kafka mémorise l'offset atteint *par groupe* →
  reprise là où on s'était arrêté (= le mécanisme de résilience vu en §2.1).
- **`auto_offset_reset`** : `earliest` (depuis le début) vs `latest` (nouveaux seulement),
  utilisé seulement à la 1re lecture d'un groupe (sans offset mémorisé).
- **Désérialisation** : symétrique du producer (`bytes → JSON → dict`).

**Sérialiseurs propres (vs `lambda`).** `kafka-python` accepte un `lambda` comme
(dé)sérialiseur mais émet un `DeprecationWarning` (il ne respecte pas l'interface
`kafka.serializer.Serializer`). Bonne pratique : définir des classes héritant de
`Serializer` / `Deserializer` et implémentant `serialize(self, topic, headers, data)` /
`deserialize(...)` renvoyant des octets. Vérifié sans warning via
`python -W error::DeprecationWarning`.

**Auto-création de topic.** Le topic `velib.stations.raw` a été créé automatiquement par
Kafka au 1er message publié (`auto.create.topics.enable` actif). En prod, on préfère
créer les topics explicitement (partitions/réplication maîtrisées).

**Validation bout-en-bout.** Poller publié 6 cycles → `velib.stations.raw:0:9102`
(9102 = 6 × 1517 messages, partition 0). Consumer relit depuis l'offset 0, clé =
`station_id`, valeurs cohérentes avec l'API. ✅ Pipeline API → Kafka → consumer prouvé.

---

# 3. Questions type recruteur (Sprint 1)

Réponses modèles à reformuler avec ses propres mots.

## Q1. Pourquoi Kafka entre l'ingestion et la base, plutôt qu'écrire en direct ?

Écrire `API → base` en direct couple fortement les deux. Kafka apporte 4 garanties :
1. **Découplage** : le producteur publie sans dépendre de l'état de la base.
2. **Tampon** : absorbe pics et pannes aval en persistant sur disque.
3. **Durabilité & rejouabilité** : on peut rejouer depuis un offset (donnée brute conservée).
4. **Multi-consommateurs (pub/sub)** : base + Spark + archivage se branchent indépendamment.

⚠️ À ne PAS dire : « pour la sécurité » ou « la cohérence de la base » (rôle de PostgreSQL).

## Q2. C'est quoi un offset, et que se passe-t-il si un consumer redémarre ?

Un **offset** = position d'un message dans une partition (marque-page), mémorisé **par
consumer group**. Pendant une panne, le producteur continue d'écrire et Kafka persiste
(jusqu'à la **rétention**). Au redémarrage, le consumer reprend à son offset et rattrape
le retard, dans l'ordre. Perte uniquement si la panne dépasse la rétention.
*Détail :* l'ordre n'est garanti **qu'au sein d'une partition** → d'où la **clé**
(`station_id`) pour router les relevés d'une station vers la même partition.

## Q3. KRaft ou Zookeeper, et pourquoi ?

**KRaft** : la coordination du cluster (contrôleur, métadonnées) est internalisée dans
Kafka via un **quorum Raft**, au lieu d'un service externe Zookeeper. Bénéfices : un
service de moins, moins de RAM, archi plus simple. **Défaut depuis Kafka 3.3**, Zookeeper
**retiré en 4.0**. Zookeeper ne sert plus qu'à maintenir d'anciens clusters.
*Bonus :* `0.0.0.0` interdit en `advertised.listeners` car on annonce une adresse
**routable** à un client (`0.0.0.0` = « toutes les interfaces », OK pour écouter seulement).

---

# 4. Sprint 2 — Traitement Spark & double stockage

**Objectif :** consommer le topic Kafka avec **Spark Structured Streaming**, nettoyer,
agréger en **fenêtres temporelles**, puis écrire dans **deux destinations** aux usages
opposés : l'**état courant** dans PostgreSQL (upsert) et l'**historique** en Parquet sur
MinIO (S3).

Chaîne réalisée : `Kafka → Spark (clean → window/watermark) → {PostgreSQL (chaud) + Parquet/MinIO (froid)}`.

**Ordre de construction et justification :**

| Étape | Quoi | Pourquoi à ce moment |
|-------|------|----------------------|
| 0 | Spark + MinIO dans Compose | L'infra de traitement/stockage doit exister avant le job. |
| 1 | Lire Kafka + désérialiser + event-time | Valider le maillon source avant tout traitement (sink console). |
| 2 | Nettoyer (filtres + déduplication) | Fiabiliser le flux avant d'agréger. |
| 3 | Agréger en fenêtres 5 min + watermark | Le cœur métier : disponibilité par station dans le temps. |
| 4 | Sink PostgreSQL (upsert) | Servir l'état courant en lecture rapide. |
| 5 | Sink Parquet/MinIO (append) | Archiver l'historique immuable pour le ML (Sprint 3). |

> Méthode : on **valide chaque maillon avec un sink console** avant d'ajouter le suivant.
> C'est le réflexe pro pour construire un pipeline streaming sans se noyer dans les erreurs.

---

## 4.1 Concept : Spark Structured Streaming

**Définition.** Spark Structured Streaming est un moteur de traitement de flux bâti sur
l'idée qu'un **flux = une table qui grandit sans fin** (*unbounded table*). On écrit la
**même requête que pour du batch** (`groupBy`, `agg`, `filter`…), et Spark la ré-exécute
**de façon incrémentale** à chaque arrivée de données.

**Image mentale.** Une table SQL classique à laquelle de **nouvelles lignes s'ajoutent en
bas** en permanence (chaque message Kafka = une ligne). On interroge cette table « vivante »
comme si elle était figée ; le moteur s'occupe du calcul continu.

**Vocabulaire de base.**

| Terme | Définition | Dans UrbanFlow |
|-------|------------|----------------|
| **Unbounded table** | Table conceptuelle alimentée en continu par le flux. | Le topic `velib.stations.raw` vu comme table. |
| **Micro-batch** | Spark accumule les nouveaux messages sur un court intervalle, puis traite ce paquet. | Mode d'exécution par défaut. |
| **Trigger** | Fréquence de déclenchement d'un micro-batch. | Défaut (dès que des données arrivent). |
| **Checkpoint** | Écriture sur disque de la progression (offsets lus, état des agrégats). | Volume `spark_checkpoints`. |
| **Output mode** | Ce qu'on émet : `append` (nouvelles lignes), `update` (lignes modifiées), `complete` (tout). | `update` → Postgres ; `append` → Parquet. |

**Rôle dans UrbanFlow.** C'est la **couche de traitement** : il transforme le flux brut en
agrégats métier et alimente les deux stockages.

**Pourquoi ce choix.** (1) On réutilise l'API DataFrame (déjà connue en batch) ; (2)
**tolérance aux pannes** via checkpoints (reprise exacte au redémarrage = *exactly-once*) ;
(3) écosystème mûr (connecteurs Kafka, JDBC, S3).

**Alternatives écartées.**

| Approche | Pourquoi écartée (à ce stade) |
|----------|-------------------------------|
| Kafka Streams / Flink | Très bons en streaming, mais Spark est plus polyvalent (batch + ML Sprint 3) et plus demandé en data science. |
| Consumer Python maison | Pas de fenêtres/watermark/exactly-once « gratuits » ; on réinventerait Spark. |

**Angle recruteur.**
> *« Comment Spark garantit-il l'exactly-once après un crash ? »*
> Grâce aux **checkpoints** : il persiste les offsets Kafka lus et l'état des agrégations.
> Au redémarrage, il reprend exactement au dernier batch validé — pas de perte ni de
> double comptage (à condition d'un sink idempotent ou transactionnel).

---

## 4.2 Event-time vs processing-time

**Définition.** Deux notions de temps coexistent :
- **Event-time** : quand l'événement s'est **réellement produit** (ici `last_reported`,
  l'horodatage de la mesure de la station).
- **Processing-time** : quand Spark **traite** le message.

**Rôle dans UrbanFlow.** On agrège **sur l'event-time** : « dispo de la station X entre
14h00 et 14h05 » doit se baser sur l'heure de la **mesure**, pas sur l'heure de calcul.

**Pourquoi ce choix.** Les données arrivent **en désordre et en retard** (retry réseau du
poller, latence Kafka). Sur le processing-time, une mesure prise à 14h04 mais traitée à
14h08 tomberait dans la mauvaise tranche → statistiques faussées. L'event-time rend le
résultat **déterministe**, quel que soit le moment où Spark voit la donnée.

**Mise en œuvre.** `last_reported` est un **entier (secondes Unix)** → casté en `timestamp`
Spark (`col("last_reported").cast("timestamp")`) pour devenir la colonne d'event-time.

**Angle recruteur.**
> *« Pourquoi event-time plutôt que processing-time ? »* → Parce que l'ordre d'arrivée
> n'est pas fiable ; seul l'instant réel de l'événement donne des agrégats temporels justes
> et reproductibles.

---

## 4.3 Fenêtres temporelles (tumbling / sliding)

**Définition.** Agréger un flux infini impose de **découper le temps** en fenêtres et
d'agréger *par fenêtre* (sinon « la moyenne depuis le début des temps » n'a aucun sens et
grossit sans fin).

**Deux types.**

| Type | Description | Un événement appartient à… |
|------|-------------|-----------------------------|
| **Tumbling** (fixe) | Tranches contiguës sans chevauchement (5 min : `14:00–14:05`, `14:05–14:10`). | **exactement une** fenêtre. |
| **Sliding** (glissante) | Fenêtre de taille L qui avance d'un pas P < L (5 min toutes les 1 min). | **plusieurs** fenêtres. |

**Choix dans UrbanFlow.** **Tumbling 5 min** : « disponibilité moyenne par station, sans
recouvrement ». Code : `groupBy(window(col("event_time"), "5 minutes"), col("station_id"))`.

**Pourquoi ce choix.** Une moyenne nette par tranche suffit pour l'état courant et pour
constituer une série temporelle régulière (matière du ML). Le sliding serait pertinent
pour un lissage « 5 dernières minutes rafraîchies chaque minute » — pas nécessaire ici.

**Angle recruteur.**
> *« Tumbling vs sliding, un cas d'usage chacun ? »* → Tumbling : compteurs par tranche
> disjointe (dispo / 5 min). Sliding : moyennes mobiles lissées qui se recouvrent.

---

## 4.4 Watermark

**Définition.** Un **watermark** est un **seuil de patience** sur l'event-time : il déclare
jusqu'à quel retard Spark accepte d'intégrer une donnée tardive. Concrètement, Spark suit
le **max event-time vu** et trace une ligne `max − seuil` ; toute donnée plus ancienne que
cette ligne est **ignorée**, et toute fenêtre entièrement passée sous la ligne est
**fermée, finalisée, et son état libéré**.

**Le problème résolu.** Sans watermark, Spark devrait garder **toutes les fenêtres
ouvertes pour toujours** (au cas où une donnée en retard arriverait) → la mémoire (l'état)
grossit sans fin. Le watermark **borne l'état**.

**Double rôle dans UrbanFlow** (`withWatermark("event_time", "10 minutes")`) :
1. **Fenêtres** : ferme et émet les fenêtres de 5 min une fois le retard de 10 min écoulé.
2. **Déduplication** : borne l'ensemble des clés « déjà vues ». Le poller republie tout
   l'instantané toutes les 60 s ; sans watermark, l'état de `dropDuplicates` (clés
   `(station_id, last_reported)`) croîtrait indéfiniment.

**Le compromis clé.** **Latence vs exhaustivité.** Watermark long = on attend plus les
retardataires (résultats plus complets mais plus tardifs) ; watermark court = résultats
rapides mais on jette davantage de données en retard.

**Règle à retenir.** Ce n'est **pas** l'heure d'**arrivée** qui décide, mais la comparaison
**event-time de la donnée vs ligne du watermark** (`max event-time vu − seuil`). Plus
ancien que la ligne → jeté ; sinon → gardé.

**Angle recruteur.**
> *« À quoi sert un watermark, et l'effet d'un seuil trop court/long ? »* → Il borne
> l'état en mémoire et gère les retards. Trop court : on perd des données légitimement en
> retard. Trop long : on consomme plus de mémoire et on retarde la finalisation des fenêtres.

> **Pourquoi la déduplication a besoin d'un watermark (alors que le batch non).** En batch,
> l'ensemble est **fini** : comparer chaque ligne aux autres se termine. En streaming,
> Spark doit **mémoriser les clés vues** indéfiniment pour repérer un doublon ; le watermark
> lui dit jusqu'à quand garder ces clés, bornant l'état.

---

## 4.5 Choix d'architecture : état chaud (PostgreSQL) vs froid (Parquet/MinIO)

**Le besoin.** Deux questions très différentes, aux **profils d'accès opposés** :
« quel est l'état **maintenant** ? » et « que s'est-il passé **dans le temps** ? ».

| | **État chaud → PostgreSQL** | **Historique → Parquet/MinIO** |
|---|---|---|
| Question | « état **maintenant** ? » | « historique **dans le temps** ? » |
| Écriture | **Upsert** (1 ligne/station, écrasée) | **Append** (jamais d'écrasement) |
| Volume | Petit, borné (~1500 stations) | Énorme, croît sans fin |
| Lecture | Point query rapide, indexée | Gros scans analytiques |
| Consommateur | Dashboard/API temps réel | Entraînement ML (Sprint 3), analyse batch |
| Format | Ligne (row), transactionnel | Colonne (Parquet), compressé, immuable |

**Choix retenu : les deux, chacun pour son usage.** Postgres = **photo de l'instant** (une
ligne par station, upsert) ; Parquet = **bande vidéo complète** (on empile, immuable).

**Pourquoi pas l'inverse.**
- Tout dans Postgres → la table explose, devient lente, coût transactionnel inutile sur de
  la donnée jamais modifiée.
- Tout en Parquet → pour « l'état maintenant » il faudrait scanner des Go ; inutilisable
  en temps réel.

**Lien théorique.** C'est l'esprit des architectures **Lambda / Kappa** : une couche de
**serving chaude** (faible latence) + une couche **froide/historique** (gros volume,
immuable) pour le batch/ML.

**Mise en œuvre.** Deux **requêtes streaming indépendantes** sur la même source, chacune
avec son **output mode** et son **checkpoint** : `update` → Postgres, `append` → Parquet.

**Angle recruteur.**
> *« Pourquoi ne pas tout stocker dans PostgreSQL ? »* → Profils d'accès opposés : état
> courant (petit, point query, upsert) vs historique (massif, scans, immuable). Le bon
> outil pour chaque usage ; sinon la base enfle et ralentit.

---

## 4.6 Concept : MinIO & stockage objet S3

**Définition.** **MinIO** est un serveur de **stockage objet compatible S3**, auto-hébergé
(open-source, en conteneur). On range des **objets** (fichiers) dans des **buckets**,
adressés par une clé — le modèle d'Amazon S3.

**Compatible S3 = portabilité.** MinIO parle **le même protocole/API que S3**. Le code
Spark (`s3a://urbanflow/...`) tournerait **tel quel** sur le vrai S3 d'AWS en prod. On
développe gratuitement en local, on déploie dans le cloud **sans changer le code**.

**Rôle dans UrbanFlow.** Le stockage froid : les fichiers **Parquet** de l'historique
(`history/stations/event_date=YYYY-MM-DD/part-*.snappy.parquet`).

**Pourquoi ce choix (vs un dossier local).** (1) C'est l'archi data réaliste (en prod
l'historique va sur S3/GCS, pas sur un disque local) ; (2) `s3a://` est la config qu'un
recruteur attend ; (3) console web pour visualiser les buckets.

**Détails techniques retenus.**
- **`path-style access`** activé : MinIO exige `endpoint/bucket/clé` (et non le
  `bucket.endpoint` en sous-domaine d'AWS).
- **SSL désactivé** en local (`http`).
- **Connecteur `hadoop-aws:3.3.4`** (+ `aws-java-sdk-bundle`), version Hadoop **alignée**
  sur celle embarquée par Spark 3.5 (sinon erreurs cryptiques).
- **Init container** (`minio-init` avec le client `mc`) crée le bucket `urbanflow` au
  démarrage → reproductible, pas d'étape manuelle.

**Parquet.** Format **colonne**, compressé (snappy), au schéma intégré. Idéal pour les
scans analytiques (on ne lit que les colonnes utiles) et le ML. **Partitionnement par
date** (`partitionBy("event_date")`) = un dossier par jour → les lectures futures ne
scannent que les jours pertinents (*partition pruning*).

**Angle recruteur.**
> *« Pourquoi MinIO et pas un simple dossier ? »* → Stockage objet compatible S3 : code
> portable vers le cloud sans modification, et archi conforme aux standards data.

---

## 4.7 foreachBatch & upsert

**Le problème.** Le sink JDBC natif de Spark ne sait faire que `append` ou `overwrite` —
**pas d'upsert** (`INSERT … ON CONFLICT DO UPDATE`). Or on veut **une ligne par station,
mise à jour** à chaque fenêtre.

**La solution : `foreachBatch`.** C'est la **porte de sortie** de Structured Streaming : à
chaque micro-batch, Spark fournit un **DataFrame batch classique** et laisse faire
**n'importe quel code / n'importe quelle destination**.

**Pattern d'upsert retenu** (dans `foreachBatch`) :
1. écrire le micro-batch dans une **table de staging** (JDBC `overwrite`) ;
2. exécuter, via une connexion JDBC, un `INSERT … SELECT DISTINCT ON (station_id) …
   ORDER BY window_start DESC … ON CONFLICT (station_id) DO UPDATE …`.

**Subtilités importantes.**
- **`DISTINCT ON (station_id) … ORDER BY window_start DESC`** : dans un batch (mode
  `update`), une station peut apparaître pour **plusieurs fenêtres** ; on ne garde que la
  **plus récente**. (Sans ça, `ON CONFLICT` planterait : « cannot affect row a second time ».)
- **Garde-fou `WHERE EXCLUDED.window_start >= cur.window_start`** : une vieille fenêtre
  n'écrase jamais une plus récente déjà stockée (anti-régression).

**Alternative écartée.** Installer `psycopg2` et collecter le batch côté driver : possible
(volume faible), mais ajoute une dépendance Python et sort de l'approche « tout JDBC ».

**Angle recruteur.**
> *« Comment fais-tu un upsert depuis Spark Streaming alors que le sink JDBC ne le supporte
> pas ? »* → `foreachBatch` → staging → `INSERT … ON CONFLICT DO UPDATE`, avec `DISTINCT ON`
> pour ne garder que la dernière fenêtre par station.

---

## 4.8 Le job complet & validation bout-en-bout

**Pipeline du job** (`spark/streaming_job.py`) :

```
read_kafka_stream      # source Kafka (schéma fixe key/value/offset…)
  -> parse_stations     # value(bytes) -> JSON -> colonnes typées + event_time
  -> validate_stations  # filtres SANS état (NULL, is_installed=1)
       ├─> deduplicate_stations  # AVEC état (watermark + dropDuplicates)
       │     -> aggregate_availability  # fenêtres 5 min : avg bikes/docks, n_obs
       │          -> start_postgres_sink  (update,  foreachBatch upsert)
       └─> start_parquet_sink            (append,  Parquet/MinIO, partitionBydate)
```

**Point d'architecture clé : séparer `validate` (stateless) et `deduplicate` (stateful).**
Les **deux requêtes** (Postgres et Parquet) ne doivent **pas partager un nœud avec état** :
deux requêtes streaming partageant un opérateur *stateful* (ici `dropDuplicates`) font
**collisionner leurs *state stores*** (erreur `Error reading delta file … does not exist`).
→ Postgres lit la branche dédupliquée+agrégée ; Parquet lit la branche **validée brute**
(sans état). Bonus : l'historique brut conserve **toutes** les observations (idéal ML).

**Validation obtenue (données réelles).**
- **PostgreSQL** : **1513 lignes = 1513 stations distinctes** (1 ligne/station, PK + upsert
  vérifiés), `updated_at` rafraîchi **en temps réel** à chaque cycle du poller.
- **MinIO** : fichiers Parquet écrits en continu sous
  `history/stations/event_date=2026-06-26/part-*.snappy.parquet` (partition par date).
- Job stable ~20 min / 40 micro-batches avant un **timeout Kafka transitoire** (réseau
  instable de l'environnement) — pas un bug ; en prod → politique de **restart**.

---

## 4.9 Pièges rencontrés (Sprint 2)

**1. Deux listeners Kafka (hôte vs réseau Docker).** Le poller (sur l'hôte) joint
`localhost:9092` ; Spark (dans un conteneur) doit joindre `kafka:29092`. On déclare **deux
`advertised.listeners`** (un par origine) car `localhost` annoncé à un conteneur pointe sur
lui-même. *(Concept détaillé en §2.5.)*

**2. Permissions volume / cache Ivy.** L'image `apache/spark` tourne en **uid 185**, mais
les volumes Docker sont créés par **root** → écritures refusées sur le cache des jars
(`.ivy2`) et les checkpoints. Correctif dev : `user: root` sur le service `spark`. En prod,
on gérerait proprement UID et droits.

**3. Résolution `--packages` sur réseau instable.** Le téléchargement Maven des connecteurs
(Kafka, JDBC, hadoop-aws + bundle AWS ~280 Mo) échouait par intermittence
(`Connection refused`). Solution : **découpler** le téléchargement du job via un **warmup**
jetable relancé en boucle jusqu'à ce que le cache Ivy soit chaud ; ensuite le job résout
**instantanément** depuis le cache (volume `ivy_cache`). Leçon : **figer les versions** des
jars en accord avec la version de Spark (3.5.3 / Scala 2.12 / Hadoop 3.3.4).

**4. Zombies Spark après arrêt.** Tuer le client `docker compose exec` **ne tue pas** le
process Spark **à l'intérieur** du conteneur → des jobs zombies survivent et entrent en
**conflit de checkpoint** (`Multiple streaming queries are concurrently using …`). Reset
propre : `docker compose restart spark` (le process principal `tail -f /dev/null` redémarre,
tuant tous les jobs enfants). Vérifier ensuite `0` process `java`.

**5. Collision de state store entre deux requêtes.** Voir §4.8 : ne pas partager un nœud
*stateful* entre plusieurs requêtes streaming. Symptôme : `Error reading delta file
…/state/…/1.delta does not exist`. Correctif : séparer les branches stateless/stateful.

**6. Batch 0 vide & buffering de logs.** Avec `startingOffsets=latest`, le **premier
micro-batch est vide** (aucune donnée publiée depuis l'abonnement) — normal. Et un `print`
Python redirigé vers un fichier est **bufferisé** (les logs n'apparaissent pas en direct).
Pour juger l'avancement réel, inspecter les **commits de checkpoint** (numéros de batch
validés) plutôt que les logs.

---

# 5. Questions type recruteur (Sprint 2)

Réponses modèles à reformuler avec ses propres mots.

## Q1. Qu'est-ce que le modèle « unbounded table » de Structured Streaming ?

Un flux est traité comme une **table qui grandit sans fin** : chaque message ajoute une
ligne. On écrit la **même requête que pour du batch** et Spark l'exécute **incrémentalement**
par **micro-batches**, en persistant sa progression via des **checkpoints** (reprise
*exactly-once* après crash).

## Q2. Event-time vs processing-time, et pourquoi ça compte ?

Event-time = instant **réel** de l'événement (`last_reported`) ; processing-time = instant
de **traitement**. On agrège sur l'event-time car les données arrivent en désordre/retard ;
sinon les fenêtres temporelles seraient faussées et non reproductibles.

## Q3. À quoi sert un watermark ?

À **borner l'état en mémoire** et **gérer les données en retard** : Spark garde
`max_event_time − seuil` comme ligne ; au-delà, il ferme les fenêtres et oublie les vieilles
clés. Compromis **latence vs exhaustivité**. Sert **aussi** à borner l'état de
**déduplication**.

## Q4. Pourquoi deux stockages (PostgreSQL + Parquet) ?

Profils d'accès **opposés** : état courant (petit, point query rapide, **upsert**) vs
historique (massif, scans analytiques, **append immuable**, pour le ML). Esprit Lambda/Kappa.
Tout mettre dans l'un ou l'autre dégrade soit la latence, soit le volume/coût.

## Q5. Comment réalises-tu un upsert depuis Spark Streaming ?

Le sink JDBC ne fait pas d'upsert → **`foreachBatch`** : on écrit le batch dans un
**staging**, puis `INSERT … SELECT DISTINCT ON (station_id) … ON CONFLICT (station_id) DO
UPDATE`, avec un garde-fou anti-régression sur `window_start`.

## Q6. Pourquoi as-tu dû séparer le nettoyage en deux fonctions ?

Parce que **deux requêtes streaming ne peuvent pas partager un opérateur avec état**
(collision de *state store*). La dédup (`dropDuplicates` + watermark) est *stateful* : elle
reste sur la branche Postgres ; la branche Parquet lit le flux **validé sans état**.

## Q7. Comment garantirais-tu un redémarrage propre après un crash ?

Grâce aux **checkpoints** (offsets + état) : la requête reprend au dernier batch validé.
En complément, une **politique de restart** qui relance la requête sur erreur transitoire
(ex. le `TimeoutException` Kafka observé).

---

# 6. Sprint 3 — Couche ML (Machine Learning)

**Objectif :** prédire la **disponibilité future** d'une station (nombre de vélos) à
**t+15 / t+30 min**, à partir de l'historique Parquet accumulé au Sprint 2. C'est un
problème de **régression** sur **série temporelle**.

Chaîne cible du sprint : `Parquet (historique) → dataset (features passées + cible décalée
validée) → split temporel → {baseline persistance, XGBoost/LightGBM, (bonus) LSTM/GRU} →
comparaison MAE/RMSE → modèle sérialisé + script d'inférence`.

**Ordre de construction et justification :**

| Étape | Quoi | Pourquoi à ce moment |
|-------|------|----------------------|
| 1 | `build_dataset.py` (Parquet → X passées + y décalée **validée**) | Sans dataset propre, tout le reste est faussé (leakage). |
| 2 | **Baseline de persistance** + MAE/RMSE | L'étalon obligatoire avant tout modèle (voir §6.1). |
| 3 | XGBoost / LightGBM | Premier vrai modèle ; doit **battre** la baseline. |
| 4 | (bonus) LSTM/GRU PyTorch | Modèle séquentiel, pour comparer une approche *deep*. |
| 5 | Comparaison + sérialisation + inférence | Choisir le meilleur, le figer (`.pkl`), le servir. |

> Principe directeur du sprint : **toujours mesurer contre une baseline**, et **ne jamais
> laisser le futur entrer dans les features** (anti-leakage). Ces deux réflexes structurent
> chaque décision ci-dessous.

---

## 6.1 Pourquoi une baseline d'abord (persistance)

**Définition.** Une **baseline** est un modèle volontairement trivial qui sert d'**étalon**.
Pour une série temporelle, la baseline naturelle est la **persistance** :
« dans 15 min, ce sera comme maintenant » → `ŷ(t+15) = bikes(t)`.

**Rôle dans UrbanFlow.** Donner un **point de référence** chiffré : un modèle n'a de valeur
que s'il **bat nettement** la persistance sur la même période de test.

**Pourquoi ce choix (4 raisons à connaître).**
1. **Une métrique seule ne veut rien dire.** MAE = 2,3 vélos est bon ou mauvais *seulement*
   par rapport à un repère. La baseline fixe ce repère.
2. **Détecteur de fuite.** Un modèle qui écrase la baseline de façon *trop* belle (MAE quasi
   nulle) trahit presque toujours un **leakage**, pas du génie.
3. **Justification coût/complexité.** En prod, un modèle à entraîner/monitorer/servir ne se
   justifie que s'il surpasse *clairement* la solution gratuite.
4. **Adversaire redoutable.** La dispo varie **lentement** sur 15 min → « pareil que
   maintenant » est *déjà* très bon. L'écart se creuse surtout à **t+30** et aux **heures de
   pointe** : c'est là qu'un vrai modèle mérite sa place.

**Angle recruteur.**
> *« Votre modèle bat à peine la persistance — le mettez-vous en prod ? »* → Non : un gain
> marginal (~0,1 vélo) ne justifie pas le coût opérationnel. Je teste d'abord des horizons
> plus longs (t+30) et les heures de pointe, là où la persistance échoue.

---

## 6.2 Métriques : MAE & RMSE

**MAE (Mean Absolute Error).** Moyenne des **écarts en valeur absolue** entre prédiction et
réalité : `MAE = (1/n) Σ |yᵢ − ŷᵢ|`. Avantage : exprimée **dans l'unité du problème**
(« on se trompe en moyenne de 1,5 vélo ») → directement interprétable.

**RMSE (Root Mean Squared Error).** Racine de la moyenne des **erreurs au carré** :
`RMSE = √[(1/n) Σ (yᵢ − ŷᵢ)²]`. Le carré **pénalise fortement les grosses erreurs**.

**Règle de lecture (à retenir).** **RMSE ≥ MAE toujours.** Plus l'**écart** entre les deux
est grand, plus il existe **quelques très grosses erreurs** isolées. MAE = 1,5 / RMSE = 2 →
modèle régulier ; MAE = 1,5 / RMSE = 6 → bon en moyenne mais se plante violemment parfois
(typiquement : rate les stations qui se vident d'un coup à l'heure de pointe).

**Rôle dans UrbanFlow.** On reporte **les deux** : MAE pour communiquer (« erreur typique »),
RMSE pour repérer les ratés coûteux (prédire 8 vélos quand la station est **vide**).

**Pourquoi ces deux-là.** MAE = robuste et lisible ; RMSE = sensible aux catastrophes, qui
comptent ici car une station vide/pleine mal prédite dégrade l'expérience usager. Les **3
modèles + la baseline** sont évalués sur **exactement le même test** → comparaison honnête.

**Angle recruteur.**
> *« MAE ou RMSE ? »* → Je reporte les deux. MAE pour l'erreur typique interprétable, RMSE
> pour pénaliser les grosses erreurs. L'écart entre les deux me dit si le modèle a des ratés
> isolés.

---

## 6.3 Fuite de données (data leakage) & cible décalée

**Définition.** Il y a **fuite** quand une information **indisponible au moment de la
prédiction** se glisse dans les features d'entraînement. Le modèle paraît brillant à
l'évaluation et s'effondre en production, où cette information n'existe pas encore.

**Règle d'or.**
> Au temps `t`, on n'utilise QUE de l'information horodatée **≤ t**. La cible est la **seule**
> chose qui vient du futur, et elle ne doit **jamais** nourrir `X`, directement ou non.

**Comment une cible décalée crée du leakage.** On construit la cible par décalage avant :
`y(t) = bikes(t + 15 min)`. Le décalage est **légitime** (c'est ce qu'on veut prédire) ; le
danger vient de ce qu'on fait **autour** :

| Piège | Mécanisme | Correctif |
|-------|-----------|-----------|
| **Feature qui regarde devant** | Moyenne glissante *centrée* incluant `t+5…t+15` → futur dans `X`. | Fenêtres **strictement passées** `[t−Δ, t]`. |
| **Décalage qui saute un trou** | Le poller a eu des coupures ; un `shift` de N lignes peut viser une mesure **6 h** plus tard, pas +15 min. | **Valider le vrai Δt** de la paire et jeter hors tolérance. |
| **Mélange entre stations** | Un `shift` sans regroupement va chercher la valeur d'une *autre* station. | Décalage **par station** (partition + tri temporel). |
| **Contamination train/test** | Scaler ajusté sur tout le dataset, ou **split aléatoire** sur du temporel. | Fit sur le train seul ; **split temporel** (§6.4). |

**Rôle dans UrbanFlow.** `build_dataset.py` doit garantir : features ≤ t, cible = vraie
valeur observée à t+horizon (Δt validé), décalage par `station_id`.

**Angle recruteur.**
> *« Comment garantissez-vous l'absence de data leakage en série temporelle ? »* → Règle ≤ t
> sur toutes les features, cible décalée **par entité** avec validation du Δt réel, et
> **split chronologique** avec embargo (jamais de shuffle).

---

## 6.4 Évaluation d'une régression temporelle (split temporel)

**Le problème.** Sur des données temporelles, le **split aléatoire** (`shuffle=True`) est
**faux** pour deux raisons :
1. **On entraîne sur le futur** pour prédire le passé → impossible en prod, métrique
   mensongèrement optimiste.
2. **Autocorrélation** : `bikes(t) ≈ bikes(t+5min)`. Le shuffle range deux lignes quasi
   identiques de part et d'autre → le test n'est pas indépendant → métriques gonflées.

**La solution : split temporel.**
```
|—————————— TRAIN ——————————|  gap  |———— TEST ————|
start                       T_cut              end
                            (tout le test est STRICTEMENT après le train)
```
- Entraîner sur `[start … T_cut]`, évaluer sur `(T_cut … end]` (ex. derniers ~20 % du temps).
- **`gap` / embargo obligatoire ≥ horizon.** Comme la cible est `t+15`, les dernières lignes
  du train ont une cible **dans** la période de test → la frontière fuit. On jette une bande
  de la taille de l'horizon entre fin du train et début du test.
- **Walk-forward** (`TimeSeriesSplit`) : plusieurs coupures qui avancent dans le temps →
  teste la **stabilité** sur plusieurs périodes, pas un seul coup de chance.
- **Split global, pas par station** : même `T_cut` pour toutes les stations.

**Rôle dans UrbanFlow.** Tous les modèles partagent **le même** découpage temporel ; la
comparaison vs baseline n'a de sens que sur la même fenêtre de test.

**Angle recruteur.**
> *« Pourquoi pas un k-fold classique sur du temporel ? »* → Il entraîne sur le futur et
> exploite l'autocorrélation → fuite + métriques gonflées. J'utilise un **split
> chronologique** (idéalement walk-forward) avec un **embargo ≥ horizon** entre train et test.

---

## 6.5 Choix d'architecture : lecture de l'historique (Spark → pandas)

**Le besoin.** Construire le dataset ML à partir des Parquet d'historique stockés sur
**MinIO** (`s3a://urbanflow/history/stations`, partitionnés par `event_date`). Deux étapes
de nature différente : (a) **lire un gros volume** depuis le stockage objet et le
**dédupliquer** ; (b) faire du **feature engineering temporel** fin (grille régulière,
forward-fill, lags, cible décalée).

**Particularité des données (rappel §6.3).** Le Parquet brut contient **une ligne par
station par cycle de poll** (~60 s), donc des **doublons massifs** : `last_reported` ne
change que quand la station change d'état, mais le poller republie tout l'instantané chaque
minute. Avant dédup, plusieurs millions de lignes ; après dédup sur
`(station_id, last_reported)`, on retombe sur les **vraies mesures distinctes** (volume bien
plus petit, confortable en mémoire).

**Les deux options.**

| Critère | **A. Hybride Spark → pandas** (choisi) ✅ | B. Tout pandas |
|---------|------------------------------------------|----------------|
| Lecture MinIO | Spark, connecteur `s3a://` **déjà câblé** (Sprint 2) | `pandas`/`pyarrow` + `s3fs` à reconfigurer |
| Dédup du gros volume | Distribuée, côté Spark **avant** de rapatrier | En mémoire, sur le brut complet (risque OOM) |
| Feature engineering | pandas (`resample`, `asof`, `shift`, `rolling`) — **bien plus ergonomique** | pandas (idem) |
| Volume rapatrié (`toPandas`) | Petit (déjà dédupliqué/projeté) | Potentiellement énorme |
| Cohérence projet | Réutilise l'infra ; **Spark batch relit le lake qu'il a écrit en streaming** | Contourne Spark |

**Choix retenu : A (hybride).** On confie à **Spark** ce qu'il fait de mieux — lire le
stockage objet à l'échelle, **filtrer/réduire** (ex. une observation par station et par
bin de 5 min) — puis **pandas** prend le relais pour le feature engineering temporel, où il
est nettement plus expressif (`reindex`, `asof`, `shift`, `rolling`).

**Mécanisme du handoff (réalité constatée).** L'image `apache/spark:3.5.3` **n'embarque pas
pandas**. Plutôt que d'ajouter une dépendance à un conteneur jetable (`pip install` perdu à
chaque recréation) pour un `toPandas()`, on passe la main **par un fichier** : Spark écrit
une **grille compacte en Parquet dans un dossier monté** (`ml/data/…`), que le `.venv` de
l'hôte relit **en local** (aucune config S3 côté hôte). Bonus : les deux étapes sont
**découplées et relançables** indépendamment.

**Pourquoi pas B (tout pandas).**
1. Il faudrait **reconfigurer** un accès S3 (`s3fs` + credentials) alors que Spark sait déjà
   parler à MinIO.
2. La dédup se ferait **en mémoire sur le brut complet** (plusieurs Mo→Go) → risque de
   saturation ; en A, Spark déduplique **avant** le rapatriement.
3. On perdrait le **fil narratif portfolio** : montrer un pipeline **lambda/kappa** complet
   où la couche froide (Parquet/MinIO) **réalimente le ML** via le même moteur Spark.

**Frontière de responsabilité (règle retenue).** *Spark = I/O + réduction de volume
(lecture, dédup, projection, éventuels filtres lourds) ; pandas/scikit = logique
temporelle fine + modélisation.* On rapatrie **le plus tard et le plus petit possible**.

**Alternative pour le futur (passage à l'échelle).** Si l'historique devenait trop gros pour
tenir en RAM après dédup, on garderait **tout le feature engineering dans Spark**
(window functions `last(... ignoreNulls)` pour le forward-fill, `lag()` pour les décalages)
et on n'écrirait que le dataset final. Pour ce projet (quelques jours × ~1500 stations),
l'hybride suffit et reste lisible.

**Angle recruteur.**
> *« Pourquoi mélanger Spark et pandas plutôt que l'un ou l'autre ? »* → Chacun sur sa
> force : Spark lit/déduplique le lake à l'échelle (I/O distribué, connecteur S3 déjà en
> place), pandas fait le feature engineering temporel fin. On rapatrie un volume **réduit**,
> et on évite de charger le brut dupliqué en mémoire.

---

## 6.6 Piège majeur : « le poller a tourné » ≠ « le Parquet a l'historique »

**Le constat (inspection `ml/inspect_history.py`, sous-incrément 1a).** On croyait disposer
de *plusieurs jours* d'historique. La réalité mesurée :

| Source | Contenu réel |
|--------|--------------|
| **Parquet (froid)** | **~20 min** (33 286 lignes brutes = ~22 cycles × 1514 stations ; ~2 mesures distinctes/station) |
| **Kafka** | **vide** — topic `velib.stations.raw` inexistant → **aucun rejeu possible** |
| Axe temporel | `last_reported` seul, sparse **et** parfois bloqué (des stations à `2021-02-21`) |

**Pourquoi (leçon d'architecture).** Le Parquet **ne se remplit que pendant que le job
Spark tourne**. Le poller alimente **Kafka**, pas directement le Parquet ; tant qu'aucun
consumer ne lit, rien n'atterrit dans le froid. Or au Sprint 2 le job Spark n'a tourné
qu'~20 min (§4.8). Et Kafka, entre-temps, s'est vidé (rétention écoulée / volume réinitialisé)
→ la **rejouabilité** (§2.1) qui aurait pu tout sauver n'a plus rien à rejouer.

**Conséquence.** Impossible de faire un vrai Sprint 3 sur 20 min : pas de **split temporel**
(§6.4) possible, quasi aucune paire (features `t`, cible `t+15`). → Il faut **collecter pour
de vrai** avant de mesurer quoi que ce soit.

**Décision retenue.** *Collecte continue MAINTENANT (poller + Spark) + écriture du code
Sprint 3 en parallèle*, testé « à blanc » sur les 20 min (métriques non significatives),
puis **relance sur données réelles** une fois plusieurs jours accumulés.

**Amélioration de pipeline associée : `ingested_at`.** On stampe désormais, **côté poller**,
un horodatage de **capture** (`ingested_at`, epoch s), **identique pour tout un cycle de
poll**. Bénéfices :
1. **Horloge fiable et régulière** (un point par cycle ≈ 1 min), **indépendante** de
   `last_reported` → base temporelle propre pour la grille ML (§6.3).
2. **Neutralise les stations bloquées** (`last_reported` figé en 2021) : on pourra dater et
   filtrer sur une horloge de confiance.
3. Les ~11 lignes dupliquées d'une même mesure reçoivent des `ingested_at` **distincts**
   → la duplication du poller **devient** la série temporelle régulière recherchée.

**Pourquoi côté poller (pas Spark).** `ingested_at` = instant de l'**appel API réel**
(l'observation), voyageant **durablement** dans Kafka pour tout consumer. Un
`current_timestamp()` côté Spark ne capturerait que l'instant de **traitement** (retardé, et
seulement quand Spark tourne) — moins fiable comme horloge.

**Angle recruteur.**
> *« Vous disiez avoir des jours de données, il n'y avait que 20 min — comment l'avez-vous
> vu, et qu'en avez-vous conclu ? »* → Par une étape d'**inspection** avant modélisation
> (volume, couverture, profil des trous). J'ai compris que le stockage froid ne se remplit
> que quand le consumer tourne, corrigé l'axe temporel (`ingested_at`), et lancé une vraie
> collecte avant d'entraîner — plutôt que de produire des métriques sur des données
> insuffisantes.

---

## 6.7 Chaîne de modélisation : baseline, XGBoost, inférence

**Les fichiers produits** (`ml/`) :

| Fichier | Rôle | Où il tourne |
|---------|------|--------------|
| `backfill_kafka_to_parquet.py` | Kafka (buffer durable) → Parquet `ml/measures` (batch, idempotent) | Spark (conteneur) |
| `build_grid.py` | mesures → **grille régulière 5 min** par station (réduction) | Spark (conteneur) |
| `build_dataset.py` | grille → **features ≤ t + cibles t+15/t+30 validées** | pandas (hôte) |
| `common.py` | **split temporel** + embargo + features + métriques (partagés) | pandas (hôte) |
| `train_baseline.py` | **persistance** + MAE/RMSE (l'étalon, §6.1) | pandas (hôte) |
| `train_xgb.py` | **XGBoost** par horizon + comparaison baseline + sérialisation | pandas (hôte) |
| `predict.py` | recharge le modèle sérialisé et **prédit** l'état futur | pandas (hôte) |

**Validation par données SYNTHÉTIQUES.** Faute d'historique réel suffisant (§6.6), on a
**smoke-testé** toute la chaîne sur une grille synthétique (`_make_synthetic_grid.py` :
cycle journalier + bruit + trous). Cela **prouve que le code tourne** — les métriques
obtenues sont **factices** (aucune valeur métier), mais la méthode est démontrée :
- 15 stations × 4 jours → 17 103 lignes de dataset valides ;
- **baseline** t+15 MAE ≈ 2,27 / t+30 ≈ 2,34 (t+30 pire que t+15 → normal) ;
- **XGBoost** t+15 MAE ≈ 1,77 (**+22 %**), t+30 ≈ 1,81 (**+23 %**) → bat la persistance
  (ici XGBoost exploite le cycle horaire injecté).

**Ce que la chaîne garantit (rappels).**
1. **Handoff Spark→pandas par fichier** (§6.5) : Spark réduit le volume, pandas fait le FE.
2. **Anti-leakage** (§6.3) : features ≤ t (forward-fill borné), cible depuis la série
   **observée** (jamais fillée), signe du décalage contrôlé.
3. **Comparaison honnête** (§6.4) : baseline et modèles partagent **le même** split temporel
   + embargo (`common.py`) et le **même** test.

**Fait (jour J).** La chaîne a été relancée sur ~4 jours de données réelles → **résultats
et interprétation en §6.8**. Bonus non codé : un petit **LSTM/GRU** (PyTorch) comme 3ᵉ modèle
séquentiel.

**Angle recruteur.**
> *« Comment garantissez-vous que votre modèle est réellement utile et honnêtement
> évalué ? »* → Une **baseline de persistance** comme plancher, le **même split temporel +
> embargo** pour tous, des features **strictement ≤ t**, et une cible bâtie sur des mesures
> **réellement observées**. Le modèle n'est retenu que s'il **bat nettement** la baseline.

---

## 6.8 Résultats du premier run réel & le paradoxe MAE/RMSE

**Volume traité (données réelles).** Backfill de **7 357 148** mesures Kafka →
`ml/measures` ; grille 5 min = **1 602 412** points (1 516 stations) ; dataset =
**1 562 291** lignes valides, du **2026-07-01 11:50** au **2026-07-05 10:35** (~4 jours,
avec un trou dû à un reboot machine, cf. §6.6). Test temporel ≈ 309 k lignes (t+15).

**Résultats (même split temporel + embargo pour tous, §6.4).**

| Horizon | MAE base | MAE XGB | RMSE base | RMSE XGB | gain MAE |
|---------|---------:|--------:|----------:|---------:|---------:|
| t+15    | **0,754** | 0,810 | 1,446 | **1,430** | −7,4 % |
| t+30    | **1,188** | 1,250 | 2,098 | **2,065** | −5,2 % |

**Le paradoxe : XGBoost PERD sur le MAE mais GAGNE sur le RMSE.** Résultat contre-intuitif,
parfaitement réel, et très instructif.

- **Pourquoi le MAE monte.** La persistance est **quasi parfaite sur la majorité des cas
  calmes** : une station qui ne bouge pas en 15 min a une erreur **exactement nulle**. Il y
  a énormément de ces cas. XGBoost, qui régresse vers l'espérance conditionnelle, sort une
  valeur **légèrement lissée** même sur un cas calme (7,9 au lieu de 8) → il transforme des
  milliers d'erreurs nulles en petites erreurs non nulles → **MAE ↑**.
- **Pourquoi le RMSE baisse.** Sur la minorité de cas **volatils** (heure de pointe, station
  qui se vide), la persistance se plante lourdement ; XGBoost réduit ces **gros** écarts.
  Comme le RMSE **élève au carré**, raboter les grosses erreurs pèse davantage → **RMSE ↓**.
- **En un mot :** XGBoost **redistribue l'erreur** — beaucoup de petites erreurs en plus
  (mauvais MAE) pour raboter quelques grosses (bon RMSE).

**Les deux causes racines.**
1. **Objectif mal aligné.** XGBoost optimise par défaut l'erreur **quadratique**
   (`reg:squarederror`) → il vise le RMSE, alors qu'on le juge au MAE.
2. **Rapport signal/bruit.** À t+15, l'essentiel du signal est « pas de changement », que la
   persistance capte **gratuitement à erreur nulle** ; le modèle ne peut pas faire mieux que
   zéro sur ces cas, et son bruit d'approximation sur la majorité facile domine le MAE.

**La leçon centrale.** C'est **exactement** pour cela qu'on fait **toujours une baseline
d'abord** (§6.1). Sans elle, on aurait sérialisé et « mis en prod » un XGBoost en croyant
progresser, alors qu'il **dégrade** la métrique de service. La baseline a joué son rôle de
**garde-fou**.

**Pistes d'amélioration (prochaine itération).**
1. **Prédire le DELTA** : cible = `bikes(t+h) − bikes(t)` (le *changement*). La persistance
   devient le trivial « prédire 0 » ; le modèle n'apporte de valeur que là où il y a du
   **vrai signal**. Inverse souvent le verdict.
2. **Entraîner sur la métrique évaluée** : `objective="reg:absoluteerror"` (optimise le MAE)
   ou Huber (`reg:pseudohubererror`).
3. **Cibler l'évaluation** sur les **heures de pointe** / stations volatiles, là où le modèle
   a un edge (déjà visible sur le RMSE).
4. **Features & tuning** : indicateurs de tendance récente, marqueur heure de pointe,
   capacité station, puis optimisation des hyperparamètres.

**Angle recruteur.**
> *« Votre modèle a un meilleur RMSE mais un moins bon MAE que la baseline — que concluez-
> vous ? »* → Le modèle **redistribue l'erreur** : il réduit les grosses erreurs (RMSE) au
> prix de petites erreurs sur les cas calmes que la persistance prédit exactement (MAE). La
> cause : l'objectif d'entraînement (quadratique) n'est pas la métrique évaluée (MAE), et le
> signal à t+15 est dominé par « pas de changement ». Je reformulerais la cible en **delta**
> et entraînerais avec un **objectif L1** avant d'envisager la prod.

---

## 6.9 Corriger le paradoxe : delta + L1, GRU, et le plafond de signal

Suite directe de §6.8 : la v1 de XGBoost dégradait le MAE. On applique les correctifs, puis
on ajoute un 3ᵉ modèle séquentiel — et on **mesure honnêtement** jusqu'où on peut aller.

### Option 1 — XGBoost v2 : cible DELTA + objectif L1 (`train_xgb_delta.py`)

**Deux changements** (le reste identique, pour comparer proprement) :
1. **Cible DELTA** : prédire `Δ = bikes(t+h) − bikes(t)` (le *changement*), pas l'absolu. La
   persistance devient le trivial « Δ = 0 » ; le modèle ne dépense plus sa capacité à
   ré-apprendre « ça reste pareil » et se concentre sur le vrai signal. **Identité clé :**
   `bikes(t+h) − (bikes(t)+Δ̂) = Δ_vrai − Δ̂` → le **MAE sur le delta = MAE sur l'absolu** →
   comparaison à la **même** baseline, sans distorsion.
2. **Objectif L1** (`reg:absoluteerror`) : optimise le **MAE** (métrique évaluée), au lieu du
   quadratique qui vise le RMSE. Reconstruction bornée `clip(bikes(t)+Δ̂, 0, capacité)`.

**Résultat — le paradoxe est corrigé :**

| Horizon | Baseline | v1 (absolu, L2) | **v2 (delta, L1)** |
|---------|---------:|----------------:|-------------------:|
| t+15 MAE | 0,754 | 0,810 (−7,4 %) | **0,754 (−0,1 %)** |
| t+30 MAE | 1,188 | 1,250 (−5,2 %) | **1,189 (−0,0 %)** |

La v2 **ne dégrade plus** : elle **égale** la persistance. Le bruit auto-infligé sur les cas
calmes a disparu.

**Diagnostic « où le modèle gagne-t-il ? »** — sur le sous-ensemble des cas ayant *réellement*
bougé (`|Δ|≥1`), là où la persistance échoue : t+15 → **+0,1 %**, t+30 → **+0,6 %**. Le modèle
bat la persistance, **de très peu**, et l'edge **grandit avec l'horizon** — direction prédite
en §6.1.

### Option 2 — 3ᵉ modèle : GRU séquentiel (`train_gru.py`)

**Idée.** Au lieu de lags fabriqués à la main, on donne au réseau la **séquence brute** des
K=12 derniers pas (60 min) ; il apprend lui-même la dynamique. **GRU** (Gated Recurrent Unit)
= RNN à portes, plus simple qu'un LSTM. Mêmes choix que la v2 : **cible delta**, **perte L1**,
même **split temporel + embargo**, standardisation **fit sur train seul**, fenêtres
**strictement contiguës** (pas de couture par-dessus un trou). Sous-échantillon de **200
stations** (démo ; GRU CPU sur 1,5 M séquences = trop lourd), écart **loggé**.

**Résultat (sur le sous-échantillon, baseline recalculée dessus) :**

| Horizon | MAE base | MAE GRU | gain |
|---------|---------:|--------:|-----:|
| t+15 | 0,784 | 0,787 | −0,3 % |
| t+30 | 1,243 | 1,245 | −0,2 % |

La **perte d'entraînement plafonne dès l'epoch 2** : le GRU converge vers « Δ≈0 » → il
**retombe sur la persistance**, exactement comme XGBoost.

### La conclusion centrale : le plafond est dans la DONNÉE, pas dans le modèle

Trois familles de modèles (persistance, arbres boostés, réseau récurrent) butent sur **le
même mur**. À 15-30 min, avec des features **temporelles seules**, le signal exploitable
au-delà de la persistance est **minime** : l'état actuel encode déjà l'essentiel du futur
proche. Ce n'est **pas** un défaut de modèle — c'est une **propriété du problème**.

**Le vrai levier pour progresser** n'est donc pas un modèle plus gros, mais un **signal plus
riche** : surtout le **spatial** (état des stations **voisines** → capte le rééquilibrage et
les reports de demande), puis météo, événements, jour férié. C'est la bonne recommandation
d'ingénieur : *diagnostiquer le plafond avant de sur-investir dans la complexité.*

**Comparaison finale des 3 modèles vs baseline** (données réelles, ~4 jours) :

| Modèle | MAE t+15 | MAE t+30 | Verdict |
|--------|---------:|---------:|---------|
| Persistance (baseline) | **0,754** | **1,188** | l'étalon, redoutable à court terme |
| XGBoost v1 (absolu, L2) | 0,810 | 1,250 | **pire** (paradoxe MAE/RMSE) |
| XGBoost v2 (delta, L1) | 0,754 | 1,189 | **égale** ; bat sur cas volatils / horizon long |
| GRU (delta, L1) | ≈ persistance | ≈ persistance | même plafond |

### Piège rencontré : PyTorch `WinError 1114` à l'import

La build **`torch==2.12.1+cpu`** échoue à l'import sous Windows (`OSError: [WinError 1114]`,
init DLL de `c10.dll`), alors que le **VC++ Redistributable** et l'**AVX** du CPU sont OK.
Cause : build récente bancale. **Correctif** : figer une version stable
(`torch==2.8.0+cpu`, index `download.pytorch.org/whl/cpu`). Leçon (comme `kafka-python` en
§2.6) : **la dernière version n'est pas toujours la bonne** ; tester l'import et figer.

**Angle recruteur.**
> *« Votre modèle sophistiqué (GRU) ne bat pas la simple persistance — c'est un échec ? »* →
> Non : j'ai **mesuré un plafond de signal**. Trois familles de modèles convergent vers la
> persistance → le futur proche est déjà encodé dans l'état présent. Le résultat utile est ce
> **diagnostic** : il oriente vers un **signal plus riche** (spatial : stations voisines)
> plutôt que vers une complexité de modèle stérile. Savoir **quand s'arrêter** est une
> compétence d'ingénieur.

---

## 6.10 Horizons longs : la courbe de lift

**Contexte (expérience post-Sprint 4).** Le §6.9 conclut au **plafond de signal** à t+15/t+30.
Question naturelle : *et si on prédisait plus loin ?* À horizon long, une station **change
vraiment** (le centre se vide le matin, se remplit le soir) → la persistance devrait
s'effondrer et laisser un modèle prendre l'avantage. On a donc étendu les horizons à
**t+60 (1h)** et **t+120 (2h)**.

**Mise en œuvre (rigueur anti-leakage).** Cibles `target_60`/`target_120` construites comme
les autres (série **observée**, jamais forward-fillée), et surtout **embargo élargi** :
`GAP_MIN` passe de 30 à **120 min** (l'embargo entre train et test doit couvrir le **plus
grand** horizon, sinon une cible du train déborde dans le test → fuite à la frontière). Même
split, mêmes features `≤ t`, même baseline — comparaison honnête.

**Résultats (test, 4 horizons).**

| Horizon | MAE persist. | MAE XGB | gain MAE | RMSE persist. | RMSE XGB | gain RMSE |
|---------|-----------|--------|----------|------------|---------|-----------|
| t+15 | 0.754 | 0.810 | **−7.4 %** | 1.446 | 1.430 | **+1.1 %** |
| t+30 | 1.188 | 1.251 | **−5.3 %** | 2.098 | 2.066 | **+1.5 %** |
| t+60 | 1.818 | 1.867 | **−2.7 %** | 3.035 | 2.955 | **+2.6 %** |
| t+120 | 2.764 | 2.764 | **≈ 0 %** | 4.419 | 4.202 | **+4.9 %** |

**Lecture — le paradoxe MAE/RMSE (§6.8) amplifié par l'horizon.** Les deux tendances sont
**monotones** : plus l'horizon s'allonge, plus le modèle rattrape/dépasse la persistance.
- **MAE (cas médian)** : XGBoost reste derrière, mais l'écart **se referme** (−7.4 % → ≈ 0 %).
  Même à 2h, **la majorité** des stations bougent peu → la persistance est *exactement* juste
  sur le cas typique, et XGBoost y ajoute un léger bruit de lissage. Le plafond sur le médian
  est très haut.
- **RMSE (grosses erreurs)** : XGBoost **gagne**, et le gain **grandit** (+1.1 % → +4.9 %).
  À horizon long, il y a **plus de bouleversements** (vidage/remplissage aux heures de pointe) ;
  le modèle **rabote ces erreurs coûteuses** que la persistance encaisse de plein fouet.

**Conclusion — nuancée et honnête.** Le modèle **devient utile à horizon long, sur les cas
volatils** (RMSE), de façon croissante. Mais sur le cas médian (MAE) la persistance tient
encore à 2h, **bridée par seulement ~4 jours de données** (le cycle journalier n'est vu que
~4 fois → XGBoost apprend mal « à 8h ça se vide »). La tendance MAE (−7.4 → 0) suggère un
**croisement au-delà de 2h**, *conditionné* à **plus de données** et aux **features spatiales**.
Autrement dit : la courbe **quantifie** la conclusion du §6.9 — le vrai levier reste la
**donnée** (plus de jours) et le **signal spatial**, pas seulement l'horizon.

**Angle recruteur.**
> *« Vous avez allongé l'horizon : le modèle gagne-t-il enfin ? »* → Sur les **grosses
> erreurs** (RMSE), oui, et de plus en plus (+1 % → +5 %). Sur le **cas médian** (MAE), pas
> encore à 2h — la persistance reste dure à battre car la plupart des stations bougent peu, et
> je n'ai que 4 jours pour apprendre le cycle journalier. J'ai une **courbe de lift** qui le
> montre et qui pointe le prochain investissement : plus de données + features spatiales.

---

# 7. Questions type recruteur (Sprint 3)

Réponses modèles à reformuler avec ses propres mots.

## Q1. Pourquoi toujours commencer par une baseline ?

Parce qu'une métrique seule n'a aucun sens : **MAE = 0,8 vélo** est bon ou mauvais seulement
par rapport à un repère. La baseline (ici **persistance** : « comme maintenant ») fixe ce
repère, sert de **détecteur de fuite** (un modèle *trop* bon trahit un leakage), et justifie
la complexité (un modèle qui ne bat pas le « gratuit » ne mérite pas la prod). Sur UrbanFlow,
elle a **évité de déployer** un XGBoost qui dégradait le MAE.

## Q2. Qu'est-ce que la fuite de données, et comment une cible décalée peut en créer ?

**Fuite** = une info **indisponible à la prédiction** (futur ou test) qui entre dans les
features → score gonflé à l'éval, effondrement en prod. Une cible `bikes(t+15)` est saine,
mais le danger vient d'autour : feature qui **regarde devant** (moyenne centrée), `shift` qui
**saute un trou** de collecte, décalage **inter-stations**, ou **split aléatoire**. Règle :
au temps `t`, **rien d'horodaté après `t`** dans `X` ; **signe du décalage** = négatif passé
(feature), positif futur (cible).

## Q3. Pourquoi pas un k-fold classique sur des données temporelles ?

Parce qu'il **entraîne sur le futur** pour prédire le passé (impossible en prod) et exploite
l'**autocorrélation** (deux instants voisins quasi identiques de part et d'autre du split →
test non indépendant → métriques gonflées). On utilise un **split chronologique** (train =
passé, test = futur) avec un **embargo ≥ horizon** entre les deux, idéalement en
**walk-forward**.

## Q4. MAE ou RMSE ? Que dit l'écart entre les deux ?

On reporte **les deux**. MAE = erreur typique interprétable (« ~0,75 vélo ») ; RMSE pénalise
les **grosses** erreurs. **RMSE ≥ MAE** toujours ; un **grand écart** signale quelques ratés
violents (stations qui se vident à l'heure de pointe). Sur UrbanFlow, RMSE ≈ 2× MAE → il y a
bien des cas volatils rares mais coûteux.

## Q5. Votre XGBoost a un meilleur RMSE mais un moins bon MAE que la baseline. Pourquoi ?

Il **redistribue l'erreur** : sur les cas calmes (la majorité), la persistance prédit
**exactement** juste (erreur 0) et XGBoost ajoute un petit bruit de lissage → MAE ↑ ; sur les
cas volatils, il **rabote les grosses erreurs** → RMSE ↓. Causes : **objectif quadratique**
(vise le RMSE, pas le MAE évalué) et **signal dominé par « pas de changement »** à t+15.
Correctif : cible en **delta** + **objectif L1**.

## Q6. Comment avez-vous géré les trous de collecte (reboot, veille) ?

Le Parquet n'a d'historique que quand le pipeline tourne (§6.6) ; on lit donc Kafka en
**backfill batch** (rétention 20 j) plutôt qu'un stream fragile. Les trous se voient à la
**reindexation sur grille 5 min** : les bins manquants restent NaN, le forward-fill des
features est **borné** (tolérance) et la **cible n'est jamais forward-fillée** → les lignes
sans mesure réelle à t+h sont **jetées** (pas d'entraînement sur une réponse fabriquée).

---

# 8. Sprint 4 — Service (API), dashboard & CI

**Objectif :** rendre le pipeline **consultable** — par une machine (API HTTP/JSON) et par
un humain (dashboard cartographique) — et **verrouiller la qualité** avec de l'intégration
continue. On expose l'**état chaud** (PostgreSQL, Sprint 2) et une **prévision** t+15/t+30.

Chaîne du sprint : `PostgreSQL → FastAPI (JSON) → Streamlit (carte) ; GitHub Actions (lint + tests)`.

**Ordre de construction et justification :**

| Étape | Quoi | Pourquoi à ce moment |
|-------|------|----------------------|
| 1 | Deps + squelette `api/`, `dashboard/` | Poser les couches avant de coder. |
| 2 | API lecture PostgreSQL (`/stations`) | Le plus simple : la table chaude existe déjà. |
| 3 | Prévision (`/forecast`) via `predictor` | Exposer la valeur ML derrière une abstraction. |
| 4 | Dashboard Streamlit (carte) | Couche présentation, consomme l'API (jamais la base). |
| 5 | Tests pytest (API isolée) | Contrat vérifiable, prérequis d'une CI. |
| 6 | CI GitHub Actions | Filet de sécurité qualité à chaque push. |

> Principe directeur du sprint : **séparer les responsabilités en couches** (config /
> données / modèles / service / présentation). Chaque couche est remplaçable et testable
> isolément — ce qui rend l'API testable **sans base** et le dashboard indépendant du schéma SQL.

---

## 8.1 Concept : API REST & FastAPI

**Définition.** Une **API REST** expose des **ressources** (`/stations`, `/stations/{id}`)
via des verbes HTTP (`GET`, `POST`…) et renvoie du **JSON**. **FastAPI** est un framework
Python qui transforme une fonction en *endpoint* : on **type** les entrées/sorties, il gère
la validation, la sérialisation, les codes HTTP, et **génère la doc interactive** (`/docs`,
standard **OpenAPI/Swagger**).

**Rôle dans UrbanFlow.** La **couche service** entre les données (PostgreSQL, modèles ML) et
le monde extérieur (dashboard, autres clients). Un contrat stable et documenté.

**Pourquoi ce choix.**
1. **Typage = contrat.** Les modèles **Pydantic** décrivent la forme exacte des réponses ;
   FastAPI **valide** ce qui sort (une clé manquante = erreur serveur, pas un JSON silencieux
   et faux). Le contrat est explicite et auto-documenté.
2. **ASGI = asynchrone.** FastAPI tourne sur un serveur **ASGI** (`uvicorn`) : l'appli et le
   serveur sont séparés (FastAPI = *quoi* répondre, uvicorn = *comment* servir sur un port).
3. **Doc gratuite.** `/docs` généré depuis les types : démonstration immédiate pour un jury.

**Alternatives.** **Flask** (plus ancien, WSGI synchrone, pas de typage natif ni de doc
auto), **Django REST** (lourd, orienté ORM/monolithe). FastAPI = meilleur ratio légèreté /
fonctionnalités pour un microservice de lecture.

**Angle recruteur.**
> *« Quelle différence entre FastAPI et le serveur ? »* → FastAPI décrit **quoi** répondre ;
> `uvicorn` (ASGI) est le **serveur** qui expose l'appli sur un port et gère les connexions.
> On lance `uvicorn api.main:app`.

---

## 8.2 Architecture en couches de l'API

L'API est **volontairement découpée** en 5 fichiers à responsabilité unique :

| Fichier | Responsabilité | Ne connaît pas |
|---------|----------------|----------------|
| `config.py` | Charge le `.env`, construit la *conninfo* | HTTP, SQL métier |
| `models.py` | Schémas **Pydantic** (forme des réponses) | La base, les routes |
| `db.py` | **Seule** couche qui écrit du SQL | HTTP, Pydantic |
| `predictor.py` | Logique de prédiction (une fonction) | La base, HTTP |
| `main.py` | Routes HTTP (assemble les autres) | Les détails SQL |

**Pourquoi ce choix.** La séparation rend chaque couche **remplaçable** et **testable** :
- on peut **substituer** `db.py` par un double de test → l'API se teste **sans PostgreSQL**
  (§8.7) ;
- le SQL est **centralisé** (`db.py`) : une seule place à auditer pour l'injection (requêtes
  **paramétrées** `%s`, jamais de f-string dans le SQL) ;
- `psycopg` v3 avec `row_factory=dict_row` renvoie chaque ligne en **dict** (clés = colonnes),
  que FastAPI mappe directement sur le modèle Pydantic. Des **alias SQL**
  (`avg_bikes_available AS bikes_available`) alignent le schéma base sur le contrat API.

**Choix de connexion.** Une **connexion courte par requête** (`with psycopg.connect(...)`).
Simple et robuste au trafic d'un portfolio ; en production on utiliserait un **pool**
(`psycopg_pool`) pour amortir le coût d'ouverture. C'est un compromis assumé, pas un oubli.

**Angle recruteur.**
> *« Pourquoi isoler l'accès aux données ? »* → Pour tester le reste sans base, centraliser
> la sécurité SQL, et pouvoir changer de stockage sans toucher aux routes.

---

## 8.3 Choix : que sert l'API ? Persistance vs XGBoost réel

**Le problème.** Servir la **dispo courante** est trivial (la table chaude l'a). Servir une
**prédiction** rouvre une question laissée par le Sprint 3 :
- `predict.py` (Sprint 3) prédit depuis un **vecteur de features complet** (`bikes_lag5/10/15`,
  `roll_mean30`…). Or la table chaude ne stocke **que** `avg_bikes`/`avg_docks` — **pas les
  lags**. Reconstruire les features en ligne = relire l'historique à chaque requête (lent,
  fragile) ou enrichir tout le pipeline Spark (hors périmètre).
- Surtout, le Sprint 3 a démontré le **plafond de signal** (§6.9) : la **persistance** est
  quasi-optimale à t+15/t+30. Un gros XGBoost servirait **peu de gain** pour **beaucoup** de
  complexité.

**Décision.** Servir la **persistance** (`pred_t15 = pred_t30 = bikes_now`), et **l'assumer**
explicitement (champ `method: "persistence"` dans la réponse, mention dans le dashboard).

**Pourquoi ce choix.** Il est **cohérent avec notre propre résultat scientifique** : déployer
un modèle qu'on a mesuré comme non-supérieur serait malhonnête et coûteux. Afficher la
persistance **avec son explication** démontre qu'on **comprend le problème** plutôt qu'on
empile des modèles. La porte reste ouverte (§8.4).

**Alternatives.** *(a)* API « courant seul » (n'exploite pas le ML) ; *(b)* « XGBoost réel »
(narratif MLOps « charger un modèle sérialisé et le servir » — défendable si l'objectif est
de **montrer l'ingénierie de service** plutôt que la performance ; on l'a écarté ici faute de
features en base et vu le plafond de signal).

**Angle recruteur.**
> *« Vous servez juste la persistance, où est le ML ? »* → Le ML du Sprint 3 a **prouvé** que
> la persistance est le plancher optimal à cet horizon. Servir un modèle plus lourd
> contredirait ma propre mesure. J'expose ce résultat honnêtement et je garde une abstraction
> prête à brancher XGBoost si les features spatiales font bouger le plafond.

---

## 8.4 L'abstraction predictor remplaçable

Toute la logique de prédiction vit derrière **une seule fonction** `predictor.predict(bikes_now)`
qui renvoie `{t+15, t+30}`. Aujourd'hui : persistance (renvoie `bikes_now`). Demain : charger
un XGBoost/GRU sérialisé (`ml/models/`) et reconstruire les features — **sans toucher** à
l'API ni au dashboard.

**Pourquoi ce choix.** C'est le patron **Strategy** : le *point de variation* (comment
prédire) est isolé du reste (routes, UI). Le **couplage** est minimal → on paie la dette
seulement quand on décide de la payer. Le champ `method` rend le changement **traçable** côté
client.

**Angle recruteur.**
> *« Comment brancheriez-vous le vrai modèle plus tard ? »* → Je réécris `predict()` : je
> charge le `.json` XGBoost et reconstruis le vecteur de features `≤ t`. Rien d'autre ne bouge,
> et `method` passe de `persistence` à `xgboost` — le client voit le changement.

**Réalisation concrète (démo horizons longs).** Pour *montrer* le modèle en action, on a ajouté
un endpoint **`GET /stations/{id}/forecast_model`** (module `api/model_forecast.py`) qui sert les
prédictions XGBoost **t+15/30/60/120** depuis le **dernier état connu du dataset** (§6.10). Deux
points de rigueur : *(a)* **honnêteté** — la base chaude n'ayant pas les features, c'est une démo
sur données historiques, étiquetée `as_of` (pas du temps réel) ; *(b)* **CI préservée** — les
imports lourds (`xgboost`, Parquet) sont **lazy** (dans la fonction, pas au niveau module) →
importer `api.main` en CI ne tire pas `xgboost` (absent des deps allégées). Le dashboard compare
alors **persistance (plate) vs XGBoost (qui diverge à horizon long)** — la courbe de lift du §6.10,
rendue visuelle. *(Piège d'affichage : `st.line_chart` trie l'axe X en **texte** → `"t+120"` avant
`"t+15"` ; on indexe donc par les **minutes numériques** pour un ordre correct.)*

---

## 8.5 Concept : Streamlit & architecture présentation → service

**Définition.** **Streamlit** transforme un script Python en app web (widgets, carte, graphes)
sans écrire de HTML/JS : le script se **ré-exécute de haut en bas** à chaque interaction, et
`@st.cache_data` mémorise les résultats coûteux (ici : le référentiel statique, et les appels
API avec un `ttl` court).

**Rôle dans UrbanFlow.** La **couche présentation**. Décision d'architecture clé : le
dashboard **consomme l'API** (HTTP via `httpx`), il ne lit **jamais** PostgreSQL directement.

**Pourquoi ce choix (Streamlit → API, pas Streamlit → SQL).**
1. **Séparation nette** : la présentation ne connaît pas le schéma SQL ; si la base change, seule
   l'API bouge.
2. **Logique non dupliquée** : les prédictions, la forme des réponses, la sécurité SQL vivent à
   **un** endroit (l'API), pas recopiées dans le dashboard.
3. **Découpage testable et « pro »** : la logique données du dashboard (`data.py`, jointure
   coordonnées) est **pure** (sans Streamlit) → testable en headless (§8.7).

**Alternatives.** Streamlit → PostgreSQL direct (moins de couches mais couplage fort au schéma
et duplication) ; un front lourd (React) — surdimensionné pour un dashboard analytique.

**Angle recruteur.**
> *« Pourquoi ne pas lire la base directement depuis Streamlit ? »* → Pour ne pas coupler la
> présentation au schéma ni dupliquer la logique. L'API est le **contrat unique** ; le dashboard
> n'en est qu'un client parmi d'autres possibles.

---

## 8.6 Le problème des coordonnées (station_status vs station_information)

**Le piège.** Une carte exige **lat/lon + nom** — qu'on **n'a jamais ingérés**. Le poller
(Sprint 1) ne lit que le flux GBFS **`station_status`** (vélos/bornes, *dynamique*). Les
coordonnées sont dans un **autre** flux, **`station_information`** (nom, lat, lon, capacité —
*métadonnées statiques*). La table chaude n'a donc aucune coordonnée.

**Solution.** Récupérer `station_information` **une fois** et le **figer** en fichier de
référence (`dashboard/stations_information.json`, ~240 Ko, 1517 stations). Le dashboard le
charge et le **joint** à la dispo courante.

**Choix de la clé de jointure (vérifié, pas supposé).** Les IDs GBFS ayant pu être renumérotés
depuis le Sprint 1, on a **mesuré** le recouvrement entre la base et le flux : `station_id` et
`station_code` matchent **tous deux 1512/1513**. On joint sur **`station_id`** (clé entière).
La station non appariée (sans coordonnées) est **écartée** de la carte proprement.

**Pourquoi un fichier figé plutôt qu'un fetch live.** Métadonnées **statiques** → déterministe,
reproductible, **aucune dépendance réseau** à la démo. Compromis : à rafraîchir manuellement si
le parc de stations évolue.

**Angle recruteur.**
> *« D'où viennent les points de la carte ? »* → D'un second flux GBFS, `station_information`,
> que je n'avais pas ingéré (je ne captais que le statut dynamique). Je l'ai figé en référentiel
> et joint sur `station_id`, après avoir **vérifié** le recouvrement des clés.

---

## 8.7 Tester l'API sans base : monkeypatch

**Le besoin.** La CI tourne sur un runner **sans PostgreSQL**. On veut tester le **contrat**
de l'API (routes, sérialisation, 404, logique du predictor) sans dépendre d'une vraie base.

**La technique.** Le `TestClient` de FastAPI appelle l'appli **en mémoire** (pas de serveur à
lancer ; il s'appuie sur `httpx`). `monkeypatch` **substitue** les fonctions de `api.db` par
des doubles qui renvoient des données factices — c'est *exactement* pourquoi on a isolé `db.py`
(§8.2). On teste `/health` (up **et** degraded), la liste, le détail, le **404**, et la
**persistance** (`pred == bikes_now`). Bilan : **9 tests**, aucune base requise.

**Pourquoi ce choix.** Tests **rapides**, **déterministes**, exécutables **partout**. On teste
la **logique de l'API**, pas PostgreSQL (qui a ses propres garanties). L'alternative (démarrer
un vrai Postgres en CI via *service container* + schéma + seed) est plus lourde et testerait
surtout la base, pas notre code.

**Angle recruteur.**
> *« Comment tester une API sans sa base ? »* → En isolant l'accès données derrière une couche
> qu'on **substitue** en test (`monkeypatch`). Le `TestClient` exécute l'appli en mémoire ; on
> vérifie le contrat HTTP, pas le moteur SQL.

---

## 8.8 CI GitHub Actions

**Définition.** Un **workflow** (`.github/workflows/ci.yml`) déclenché à chaque `push`/`pull_request` :
un **runner** Ubuntu jetable installe Python, les deps, puis lance **lint (ruff) + tests (pytest)**.
Échec → PR marquée rouge.

**Deux décisions non triviales.**
1. **Deps allégées** (`requirements-dev.txt`) : installer tout `requirements.txt` tirerait
   `torch`/`xgboost`/Spark **inutilement** — et sur Linux, `torch` par défaut = build **CUDA de
   plusieurs Go**. On installe un **sous-ensemble** (FastAPI, psycopg, httpx, pandas, pytest,
   ruff) → CI rapide.
2. **Lint et tests scopés** : `ruff check api dashboard tests` (pas le code Sprint 1-3, écrit
   hors contrainte de linter) ; `pytest` limité à `tests/` (voir §8.9, piège de découverte).

**Pourquoi ce choix.** La CI doit être **rapide** (feedback en < 1 min) et **honnête** (ne
tester que ce qu'on maîtrise). On a **prouvé** le workflow en le rejouant dans un **venv neuf**
avant de pousser (ruff clean + 9 tests) → garantie qu'aucune dépendance ne manque.

**Angle recruteur.**
> *« Que met-on dans une CI, et pourquoi allégée ? »* → Lint + tests à chaque push. J'exclus les
> deps ML lourdes (torch CUDA = plusieurs Go) car la CI ne teste que la couche service : rapidité
> et pertinence priment.

---

## 8.9 Pièges rencontrés (Sprint 4)

- **Conflit de port 5432.** Un **PostgreSQL natif Windows** occupait déjà `localhost:5432` →
  l'API tombait sur la mauvaise base (échec d'auth). Correctif : rendre le port **configurable**
  (`${POSTGRES_PORT}:5432` dans Compose) et publier le conteneur sur **5433**. `POSTGRES_PORT`
  a un **double usage** : port publié par Compose **et** port de connexion de l'API — donc
  cohérents par construction. (Réseau **interne** Docker inchangé : Spark → `postgres:5432`.)
- **`pytest` découvre TOUT le repo.** Des scripts Sprint 1-3 nommés `test_*.py`
  (`consumer/test_consumer.py`) importent des deps lourdes (`kafka`) absentes de la CI → **erreur
  de collecte** qui casse la CI. Symptôme masqué en local (venv complet). Correctif :
  `testpaths = tests` dans `pytest.ini`. *(Attrapé en simulant la CI dans un venv propre.)*
- **`torch` en CI = build CUDA.** `pip install torch` sur Linux tire par défaut une build GPU de
  plusieurs Go. D'où le `requirements-dev.txt` sans torch (§8.8).
- **`sys.path` sous `streamlit run`.** Streamlit ne met que le **dossier du script** sur le
  `sys.path`, pas la racine → `from dashboard import data` casse. Correctif : insérer la racine
  du projet en tête d'`app.py`. (Même famille que le `conftest.py` racine pour pytest.)

---

# 9. Questions type recruteur (Sprint 4)

Réponses modèles à reformuler avec ses propres mots.

## Q1. Pourquoi FastAPI et un découpage en couches ?

FastAPI donne un **contrat typé** (Pydantic valide les réponses), la **doc auto** (`/docs`,
OpenAPI) et un serveur **ASGI**. Le découpage `config / models / db / predictor / main` isole
les responsabilités : le SQL est **centralisé** (sécurité, injection), et l'accès données est
**substituable** → l'API se teste **sans base**. Chaque couche est remplaçable sans toucher aux
autres.

## Q2. Vous ne servez que la persistance : où est le ML ?

Le Sprint 3 a **démontré** le *plafond de signal* : à t+15/t+30 aucun modèle ne bat nettement la
persistance. Servir un XGBoost lourd contredirait ma propre mesure et exigerait de reconstruire
les features (absentes de la base chaude). J'expose donc la persistance **honnêtement** (champ
`method`) derrière une **abstraction** prête à recevoir un vrai modèle si les features spatiales
font bouger le plafond.

## Q3. Comment testez-vous l'API sans base de données ?

En isolant l'accès données dans `db.py`, que je **substitue** en test (`monkeypatch`) par des
doubles. Le `TestClient` de FastAPI exécute l'appli **en mémoire**. Je vérifie le **contrat**
(codes HTTP, sérialisation, 404, persistance) — 9 tests, aucun Postgres requis, exécutables en CI.

## Q4. Pourquoi le dashboard passe-t-il par l'API plutôt que lire la base ?

Pour **découpler** la présentation du schéma SQL et **ne pas dupliquer** la logique (prédiction,
forme des réponses). L'API est le **contrat unique** ; le dashboard n'en est qu'un client. Bonus :
la logique données du dashboard reste **pure** (sans Streamlit) donc testable en headless.

## Q5. D'où viennent les coordonnées de la carte ?

Pas de l'ingestion : le poller ne captait que `station_status` (dynamique). Les lat/lon sont dans
`station_information` (statique), que j'ai **figé en référentiel** et **joint sur `station_id`** —
après avoir **vérifié** que la clé recouvre bien la base (1512/1513).

## Q6. Qu'avez-vous mis dans la CI, et quel piège avez-vous corrigé ?

Lint (ruff) + tests (pytest) à chaque push, avec des **deps allégées** (pas de torch CUDA de
plusieurs Go). Piège corrigé : `pytest` découvrait des scripts `test_*` du Sprint 1 important
`kafka` → j'ai scopé la découverte à `tests/`. Je l'ai attrapé en **rejouant la CI dans un venv
neuf** avant de pousser.

---

# 10. Glossaire

| Terme | Définition courte |
|-------|-------------------|
| **Broker** | Serveur Kafka qui stocke et distribue les messages. |
| **Conteneur** | Instance en exécution créée à partir d'une image Docker (jetable). |
| **auto_offset_reset** | Où un consumer démarre sans offset mémorisé : `earliest` (début) ou `latest` (nouveaux). |
| **Consumer** | Programme qui lit les messages d'un topic. |
| **Consumer group** | Ensemble de consumers partageant un `group_id` ; Kafka mémorise l'offset par groupe. |
| **GBFS** | General Bikeshare Feed Specification : format JSON standard des données de vélos en libre-service. |
| **Polling** | Interroger une source à intervalle régulier (par opposition à recevoir un push). |
| **Sérialisation** | Conversion objet ↔ octets (Kafka ne transporte que des bytes). |
| **ttl** | Time to live : durée de fraîcheur annoncée d'une donnée (indice, pas garantie). |
| **Docker Compose** | Fichier décrivant plusieurs services conteneurisés et leurs liens. |
| **Image** | Modèle figé en lecture seule empaquetant une application + ses dépendances. |
| **KRaft** | Mode Kafka sans Zookeeper : métadonnées gérées en interne via un quorum Raft. |
| **Raft** | Algorithme de consensus distribué (leader + réplication + quorum majoritaire). |
| **Listeners / Advertised listeners** | Ports d'écoute réels de Kafka / adresse annoncée aux clients pour reconnexion. |
| **Replication factor** | Nombre de copies d'une donnée sur des brokers distincts (tolérance aux pannes). |
| **Volume** | Stockage Docker persistant qui survit à la suppression d'un conteneur. |
| **Zookeeper** | Ancien service externe de coordination de Kafka (déprécié, retiré en 4.0). |
| **Offset** | Position d'un message dans une partition (marque-page de lecture). |
| **Partition** | Sous-division d'un topic (parallélisme + ordre local). |
| **Producer** | Programme qui écrit des messages dans un topic. |
| **Pub/Sub** | Modèle où un message publié une fois est lu par plusieurs abonnés. |
| **Rétention** | Durée de conservation des messages dans Kafka. |
| **Topic** | Canal/catégorie nommé(e) de messages dans Kafka. |
| **Structured Streaming** | Moteur Spark traitant un flux comme une table illimitée, via des requêtes batch exécutées par micro-batches. |
| **Unbounded table** | Représentation d'un flux comme une table alimentée en continu. |
| **Micro-batch** | Paquet de nouveaux messages traité d'un coup par Spark à chaque trigger. |
| **Checkpoint** | Persistance de la progression d'un stream (offsets + état) pour reprise *exactly-once*. |
| **Output mode** | `append` / `update` / `complete` : ce qu'une requête streaming émet à chaque batch. |
| **Event-time** | Instant réel de l'événement (ici `last_reported`), base des fenêtres. |
| **Processing-time** | Instant où Spark traite le message (à ne pas confondre avec l'event-time). |
| **Window (tumbling / sliding)** | Découpage du temps pour agréger : tranches disjointes / fenêtres glissantes recouvrantes. |
| **Watermark** | Seuil de patience sur l'event-time : borne l'état mémoire et gère les données en retard. |
| **Stateful / stateless** | Opérateur avec / sans état conservé entre micro-batches (ex. `dropDuplicates` est stateful). |
| **State store** | Stockage de l'état d'un opérateur stateful, propre à chaque requête (ne pas partager). |
| **foreachBatch** | Porte de sortie streaming : exécute du code arbitraire sur chaque micro-batch (DataFrame batch). |
| **Upsert** | Insérer ou mettre à jour (`INSERT … ON CONFLICT DO UPDATE`) ; clé = `station_id`. |
| **État chaud / froid** | Données servies en temps réel (Postgres) vs historique massif immuable (Parquet). |
| **MinIO** | Serveur de stockage objet auto-hébergé, compatible API S3. |
| **S3A** | Connecteur Hadoop pour lire/écrire sur un stockage S3 (`s3a://`). |
| **Parquet** | Format de fichier en colonnes, compressé, au schéma intégré ; idéal scans analytiques/ML. |
| **Partition pruning** | Lecture ciblée des seules partitions utiles (ex. par `event_date`) grâce au partitionnement. |
| **Init container** | Conteneur jetable qui prépare un état initial (ex. créer le bucket) puis s'arrête (`Exited 0`). |
| **Baseline** | Modèle trivial servant d'étalon ; ici la **persistance** (`ŷ(t+15)=bikes(t)`). |
| **Persistance** | Baseline « ce sera comme maintenant » : prédire la dernière valeur observée. |
| **MAE** | Mean Absolute Error : moyenne des écarts absolus ; dans l'unité du problème (vélos). |
| **RMSE** | Root Mean Squared Error : racine de la moyenne des erreurs au carré ; pénalise les gros écarts. |
| **Data leakage** | Fuite : information indisponible à la prédiction (futur/test) qui entre dans `X`. |
| **Cible décalée (target shift)** | Cible construite par décalage avant : `y(t)=valeur(t+horizon)`. |
| **Horizon** | Délai de prédiction visé (ici t+15 / t+30 min). |
| **Autocorrélation** | Corrélation d'une série avec elle-même décalée ; rend les points voisins quasi identiques. |
| **Split temporel** | Découpage train/test **chronologique** (test strictement après le train). |
| **Walk-forward (TimeSeriesSplit)** | Validation par coupures temporelles successives qui avancent dans le temps. |
| **Gap / embargo** | Bande jetée entre train et test (≥ horizon) pour éviter la fuite à la frontière. |
| **Cible delta** | Prédire le changement `bikes(t+h)−bikes(t)` ; persistance = « delta 0 ». |
| **Objectif L1 / L2** | Perte optimisée : L1 (`reg:absoluteerror`) vise le MAE, L2 (quadratique) le RMSE. |
| **RNN** | Réseau récurrent : traite une séquence pas à pas via un état caché (mémoire). |
| **LSTM / GRU** | Variantes de RNN à *portes* (retenir/oublier) ; GRU = plus simple/rapide. |
| **Séquence / fenêtre (K)** | Suite des K derniers pas donnée en entrée d'un modèle séquentiel. |
| **Plafond de signal** | Limite de prédictibilité propre aux données : aucun modèle ne la dépasse. |
| **ingested_at** | Horodatage de capture stampé par le poller (epoch s), identique par cycle ; horloge ML fiable. |
| **Backfill** | Remplir a posteriori un stockage (ici Parquet) en rejouant une source (ici Kafka) — impossible si la source est vide. |
| **Gradient boosting** | Ensemble d'arbres construits séquentiellement, chacun corrigeant l'erreur des précédents. |
| **XGBoost** | Implémentation performante du gradient boosting (arbres) ; 1er vrai modèle vs baseline. |
| **Smoke-test** | Test minimal vérifiant qu'une chaîne s'exécute de bout en bout (ici sur données synthétiques). |
| **API REST** | Interface exposant des ressources via HTTP (`GET`/`POST`…) et renvoyant du JSON. |
| **FastAPI** | Framework Python d'API : fonctions typées → endpoints + validation + doc auto. |
| **ASGI / uvicorn** | Interface serveur asynchrone / le serveur qui exécute l'appli FastAPI sur un port. |
| **Pydantic** | Bibliothèque de modèles typés ; décrit et valide la forme des entrées/sorties. |
| **OpenAPI / Swagger** | Spécification de l'API générée depuis les types ; alimente la doc interactive `/docs`. |
| **Endpoint** | Point d'accès d'une API (couple URL + verbe HTTP), ex. `GET /stations/{id}`. |
| **Requête paramétrée** | SQL où les valeurs passent par `%s`/binding (jamais concaténées) → anti-injection. |
| **dict_row (psycopg)** | Fabrique de lignes de psycopg renvoyant chaque ligne en `dict` (clés = colonnes). |
| **Pool de connexions** | Réservoir de connexions DB réutilisées (amortit le coût d'ouverture) — prod. |
| **Persistance (service)** | Prévision servie = état courant (`pred = bikes_now`), assumée via le champ `method`. |
| **Strategy (patron)** | Isoler un point de variation derrière une interface (ici `predictor.predict`). |
| **Streamlit** | Framework transformant un script Python en app web (widgets/carte) sans HTML/JS. |
| **st.cache_data** | Mémoïsation Streamlit d'un résultat coûteux (référentiel, appel API) avec `ttl` optionnel. |
| **pydeck** | Bibliothèque de cartes (deck.gl) ; ici scatter des stations coloré par disponibilité. |
| **station_status / station_information** | Flux GBFS dynamique (vélos/bornes) / statique (nom, lat/lon, capacité). |
| **Référentiel** | Table statique de métadonnées jointe aux données dynamiques (ici coordonnées des stations). |
| **CI (Intégration Continue)** | Exécution automatique de lint + tests à chaque push/PR. |
| **GitHub Actions / runner** | Service CI de GitHub / machine jetable qui exécute le workflow. |
| **TestClient** | Client de test FastAPI appelant l'appli **en mémoire** (via httpx), sans serveur. |
| **monkeypatch** | Fixture pytest qui **substitue** un attribut/fonction le temps d'un test (ici `api.db`). |
| **ruff** | Linter Python très rapide (règles pycodestyle/pyflakes/isort). |
| **testpaths** | Option pytest limitant la découverte des tests à un dossier (ici `tests/`). |
