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
3. [Glossaire](#3-glossaire)

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

# 3. Glossaire

| Terme | Définition courte |
|-------|-------------------|
| **Broker** | Serveur Kafka qui stocke et distribue les messages. |
| **Conteneur** | Instance en exécution créée à partir d'une image Docker (jetable). |
| **Consumer** | Programme qui lit les messages d'un topic. |
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
