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
6. [Glossaire](#6-glossaire)

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

# 6. Glossaire

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
