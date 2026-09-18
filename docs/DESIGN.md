# UrbanFlow — design decisions and limits

The README keeps one line per decision. This file holds the reasoning, the measurements and
the caveats. Every number is traceable to a versioned file or says that it is not.

## 1. Design decisions, and why

**Kafka between the API and everything else.** The poller (`ingestion/poller.py`) polls the
GBFS `station_status` feed every 60 s and publishes one message per station, keyed by
`station_id`, with an `ingested_at` timestamp stamped **at capture time**, identical for a
whole poll cycle. Writing straight to a database would couple the poller to the consumer's
availability and lose the replay. The replay is not theoretical: the ML history of Sprint 3
was rebuilt entirely from Kafka's 20-day retention (see §2) after the cold store turned out
to hold twenty minutes of data. `ingested_at` is the ML clock because the feed's own
`last_reported` is sparse and, for some stations, frozen in 2021.

**KRaft, no ZooKeeper.** Kafka 3.9 runs its own Raft quorum; one container, one process,
one fewer moving part. The one trap is the listener pair: `KAFKA_LISTENERS` uses the empty
host (`://:9092`) because `0.0.0.0` is refused on the advertised side, and there are two data
listeners, `localhost:9092` for host clients and `kafka:29092` for containers, because the
advertised address is what a client reconnects to.

**Two sinks, two access patterns.** "What is the state now?" and "what happened over time?"
have opposite profiles: point query versus scan, upsert versus append, bounded versus
unbounded. PostgreSQL keeps one row per station (`station_availability_current`, upserted by
`INSERT … ON CONFLICT` from a staging table in `foreachBatch`, with a guard so an older window
never overwrites a newer one). Parquet on MinIO appends every validated observation,
partitioned by `event_date`. Putting everything in Postgres bloats a transactional table with
data that is never updated; putting everything in Parquet makes "now" a scan.

**Stateless validation split from stateful deduplication.** Two streaming queries that share
a stateful operator (`dropDuplicates` with watermark) collide on the same state store and fail
with `Error reading delta file … does not exist`. The job (`spark/streaming_job.py`) therefore
validates without state, then forks: the Postgres branch deduplicates, windows over 5 minutes
and upserts; the Parquet branch reads the validated raw stream. A side effect is that the cold
history keeps every observation, which is what a dataset builder wants.

**Watermark and 5-minute tumbling windows.** Event time comes from the message, not from the
processing clock, so a late message lands in its own window and the watermark bounds how long
Spark keeps a window open. Windows are averaged (`avg_bikes_available`, `n_observations`)
because the poller publishes the full snapshot every minute whether or not a station changed.

**Spark reduces, pandas engineers.** The `apache/spark:3.5.3` image has no pandas, and
Spark is the component that already speaks `s3a://` to MinIO. So Spark reads the measures,
keeps the freshest observation per station and 5-minute bin (`ml/build_grid.py`), and writes
a compact Parquet grid to a directory mounted from the host. The host venv then does the
temporal feature engineering in pandas (`ml/build_dataset.py`), where `reindex`, `shift` and
`rolling` are far more expressive than window functions. Hand-off by file rather than
`toPandas()`: the two steps are decoupled and re-runnable. At a scale where the grid no longer
fits in memory, the feature engineering would move into Spark window functions.

**Leakage is a construction rule, not a check.** In `build_dataset.py`:

- each station is reindexed on a continuous 5-minute grid, so a silent station shows up as a
  gap rather than being invented;
- features are forward-filled **at most two steps** (10 minutes), then lags at −5/−10/−15 min
  and a 30-minute rolling mean and standard deviation, all ending at *t*;
- targets `t+15 … t+120` are shifted from the **observed** series, never the filled one: if the
  future bin was not measured, the target is NaN and the row is dropped for that horizon;
- the split in `ml/common.py` is chronological (last 20 % of time is test) with an embargo of
  `GAP_MIN = 120` minutes, the largest horizon, so no training target falls inside the test
  period. There is one split function, imported by the baseline and every model.

**A baseline first, and a metrics file rather than a print.** Persistence (`prediction =
bikes(t)`) is the reference every model is compared to, on the same test rows. Since
2026-09-16, `train_baseline.py` and `train_xgb.py` write `ml/results/metrics_<run>.json` with
the dataset size, period, split parameters and per-horizon MAE/RMSE; `docs/make_figures.py`
draws the lift curve from that file. Persistence is recomputed inside the XGBoost run rather
than read from the baseline file so both numbers come from exactly the same rows.

**Serve the baseline when the baseline wins.** The hot table stores averaged counts, not the
lag and rolling features the model needs, and the measurement (§2) says the model does not
beat persistence at the served horizons. `/stations/{id}/forecast` therefore returns
persistence with `method: "persistence"` behind a one-function `predictor` (Strategy pattern):
swapping in a model touches nothing else, and the client sees the change in the field.
`/stations/{id}/forecast_model` serves the four XGBoost models on the **last known feature
vector of the historical dataset**, labelled with `as_of`; it is a demonstration of the
serving layer, not a live forecast, and it answers 503 with the command to run when the
gitignored dataset or models are absent. Heavy imports there are lazy so the CI, which installs
neither xgboost nor torch, can import the app.

**A thin dashboard over HTTP.** Streamlit only talks to the API. Coordinates come from a
second GBFS feed, `station_information`, that the poller never ingested; it is frozen as
`dashboard/stations_information.json` (1517 stations, July 2026 snapshot) and joined on
`station_id` after checking that the key covers the database (1512 of 1513 at the time).

## 2. What was measured, and what it does and does not establish

Source: [../ml/results/metrics_xgb.json](../ml/results/metrics_xgb.json) and
[metrics_baseline.json](../ml/results/metrics_baseline.json), recomputed on 2026-09-16 on the
dataset built in July (1 566 818 rows, 1510 stations, 2026-07-01 11:50 to 2026-07-05 10:35,
about 300 000 test rows per horizon). Values are identical to the July run at three decimals
(fixed seed, same file).

| horizon | MAE persistence | MAE XGBoost | RMSE persistence | RMSE XGBoost |
|---|---|---|---|---|
| t+15 | 0.754 | 0.810 | 1.446 | 1.430 |
| t+30 | 1.188 | 1.251 | 2.098 | 2.066 |
| t+60 | 1.818 | 1.867 | 3.035 | 2.955 |
| t+120 | 2.764 | 2.764 | 4.419 | 4.202 |

**The MAE/RMSE paradox.** XGBoost v1 (absolute target, squared loss) has a worse MAE and a
better RMSE than persistence at every horizon up to 60 min. The squared objective optimises the
RMSE: the model smooths, which trims the large errors of stations that empty or fill and adds a
small error to the majority that do not move at all, where persistence is exactly right.

**Fixing the paradox does not beat persistence.** Two changes were tried, both on the same
split (numbers from the July training logs, not from a versioned file):

- XGBoost v2, delta target `bikes(t+h) − bikes(t)` with an L1 objective
  (`ml/train_xgb_delta.py`): MAE 0.754 at t+15 and 1.189 at t+30, equal to persistence.
  On the subset of rows that actually moved (|Δ| ≥ 1) it wins by +0.1 % and +0.6 %.
- GRU on the raw 60-minute sequence, delta target, L1 loss, standardisation fitted on train
  only, windows that never bridge a gap (`ml/train_gru.py`): MAE 0.787 vs 0.784 at t+15 and
  1.245 vs 1.243 at t+30 on a **200-station subsample** (CPU budget), with the baseline
  recomputed on that subsample. The training loss plateaus at epoch 2: the network learns
  "Δ ≈ 0".

Three families converging on the same wall is the finding: with temporal features only, the
present state already encodes almost all of the near future. The ceiling is a property of the
data, not a failure of the models.

**The lift grows with the horizon.** Extending to t+60 and t+120 required raising the embargo
from 30 to 120 minutes, otherwise a training target would overlap the test period. On the RMSE
the gain rises monotonically, +1.1 % → +1.5 % → +2.6 % → +4.9 %; on the MAE the deficit closes,
−7.4 % → −5.3 % → −2.7 % → 0.0 %. At two hours, stations do move (rush-hour emptying and
filling), and the model trims those errors; the median station still moves little, and the
model has seen the daily cycle four times.

### Caveats on the measurement

- **Four days of data, one period.** The test set is the last 20 % of a single week in July.
  There is no cross-period validation and no confidence interval on the gains; at ~300 000 test
  rows the numbers are stable, but they describe those four days.
- **The gains are small.** +4.9 % RMSE at t+120 is the largest effect. Nothing here would
  justify the model in production on its own; it justifies collecting more and adding spatial
  features.
- **The delta model and the GRU numbers are not versioned.** They come from the training
  output of 2026-07-05, kept in the author's notes. Only the baseline and XGBoost v1 write a
  metrics file.
- **Hyperparameters were not tuned** (300 trees, depth 6, learning rate 0.05, fixed seed). A
  tuned model might do better; the ceiling argument rests on three families, not on tuning.
- **The dataset is gitignored** (22 MB). It is reproducible from Kafka only while the retention
  window holds the July messages, which it no longer does; a fresh clone rebuilds a new dataset
  from a new collection.

## 3. Verified on a fresh clone (2026-09-18)

A clone into a temporary directory, `.env` from the example, a separate compose project with
new volumes. Observed: the schema is created by initdb; the poller publishes 1518 stations per
cycle (the GBFS feed grew by one since the July reference); `spark-submit --packages` resolves
16 artifacts (334 MB) and the streaming job upserts 1516 stations within five minutes, with
Parquet partitions on MinIO; `/health`, `/stations` and the dashboard answer; `/forecast_model`
answers 503 with the command to run; backfill (13 644 measures), `build_grid` (4 548 points)
and the three host scripts run to completion. With ten minutes of data `build_dataset` yields
zero valid rows and the training scripts say so instead of failing. Two things this run
surfaced: MinIO's images are no longer on Docker Hub (the compose file now pulls from
quay.io), and the hot-table windows lag the wall clock by about an hour because event time is
the feed's `last_reported`, not `ingested_at` (see known debt).

## 4. Known debt

- **The cold Parquet has no consumer.** The ML path re-reads Kafka because the Sprint 2 job
  ran for about twenty minutes and the history was empty. Pointing `build_grid.py` at
  `s3a://urbanflow/history/stations` once the streaming job runs continuously is the intended
  design.
- **No Dockerfile** for the poller, the API or the dashboard; they run in a host venv. The
  compose file carries the infrastructure only.
- **No restart policy** for the Spark job: a transient Kafka timeout stops it, and stopping the
  `docker compose exec` client does not stop the job in the container (`docker compose restart
  spark` does).
- **MinIO images unpinned** (`minio/minio:latest`, `minio/mc:latest`); Postgres pinned to the
  major only.
- **Hot-table windows are keyed on the feed's `last_reported`**, which lagged the clock by about
  55 minutes on 2026-09-18 (`updated_at` shows the real freshness). Windowing on `ingested_at`,
  the capture clock the ML path already uses, is the fix; not done to keep the Sprint 2 job as
  validated.
- **The station reference is a snapshot.** `stations_information.json` is from July 2026 and
  is refreshed by hand; the feed already has one more station.
- **Three station counts** describe three real sets: 1517 in the GBFS reference, 1513 that
  reported a state during the collection (rows in the hot table), 1510 with enough continuous
  history to enter the ML dataset.
- **`ml/data/dataset_synth.parquet`**, the smoke-test dataset mentioned in the training scripts'
  docstrings, predates the t+60/t+120 targets and no longer runs them.

## 5. Pitfalls met along the way

Recorded because each cost time and each is easy to hit again:

- `kafka-python` 2.0.2 does not import on Python 3.12; 3.0.2 does.
- `spark-submit --packages` re-resolves the Maven graph on every run and fails on an unstable
  network. Warm the Ivy cache once (volume `ivy_cache`), then pass `--jars` with the cached files.
- Versions must line up: Spark 3.5.3 / Scala 2.12 / Hadoop 3.3.4 / `hadoop-aws:3.3.4` /
  `aws-java-sdk-bundle:1.12.262` / `postgresql:42.7.4`.
- `torch==2.12.1+cpu` fails to import on Windows (`WinError 1114` in `c10.dll`); 2.8.0 works.
- A native PostgreSQL on 5432 silently captures the API's connection: `POSTGRES_PORT` is both
  the published port and the API's port so they cannot diverge.
- `pytest` collects every `test_*.py` in the repository, including a Kafka CLI consumer that
  the CI cannot import; `testpaths = tests` in `pytest.ini`.
- `streamlit run` puts only the script's directory on `sys.path`; `dashboard/app.py` inserts the
  project root first.
- `st.line_chart` sorts a text axis lexically (`t+120` before `t+15`); index by minutes.
