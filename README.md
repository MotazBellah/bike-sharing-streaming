# Citibike Streaming Pipeline

Ingests Citibike trip events from Redpanda, aggregates them in PyFlink, and serves
the results from Redis over HTTP with sub-millisecond reads.

```
event_generator.py → Redpanda → PyFlink → Redis → FastAPI
```

---

## Quickstart

Requires [uv](https://docs.astral.sh/uv/), Docker, and (for the Kubernetes path)
minikube and OpenTofu.

```bash
make sync                      # uv installs deps, fetching Python 3.11 if needed
make up                        # Redpanda, Flink (JM+TM), Redis, API
make topic                     # 3 partitions (auto-create would give only 1)
make generate CSV=202604-citibike-tripdata.csv
```

`make topic` matters: an auto-created topic gets a single partition, which pins
the source to one subtask and hides any cross-partition ordering behaviour. The
job is designed for a partitioned topic — see the trip-stats section below.

The generator loads the **entire** CSV into memory and sorts it before
publishing, so a full month (~190MB, ~2M rows) needs several GB of RAM. Start
with a slice:

```bash
head -50000 202604-citibike-tripdata/202604-citibike-tripdata-part1.csv > sample.csv
make generate CSV=sample.csv
```

Then:

```bash
curl localhost:8000/stations/busiest
curl localhost:8000/stations/6140.05/last-activity
curl localhost:8000/stations/6140.05/bike-balance
curl localhost:8000/stations/6140.05/trip-stats
```

The Flink dashboard is at `localhost:8081`, and `make test` runs the unit suite
with no infrastructure needed.

Data files are not committed — download a month of trip history from
[citibikenyc.com/system-data](https://citibikenyc.com/system-data).

---

## API

| Endpoint | Returns |
|---|---|
| `GET /stations/{id}/last-activity` | Timestamp and type of the most recent event |
| `GET /stations/{id}/bike-balance` | Net bike delta since stream start |
| `GET /stations/busiest` | Station with the highest event count |
| `GET /stations/{id}/trip-stats` | Mean duration of departing and arriving trips |
| `GET /health` | Redis connectivity check |

A station with no events yet returns `404` — indistinguishable from an unknown
station id, since the pipeline only learns stations that appear in the stream.

---

## Architecture

### Aggregation happens on write, not on read

There is no aggregation query anywhere. Each event triggers a small state update
in Flink, which emits the station's new state to Redis. By the time a request
arrives, the answer is already sitting under a key, so every endpoint is one
Redis read — `HGETALL`, or `ZREVRANGE` for busiest. That is what buys the latency.

### Redis is a materialized view, not a cache

There is no database behind Redis, and that is intentional. **Redpanda is the
source of truth**: durable, ordered, replayable. Redis holds a *projection* of
that log, and the Flink job is the process that keeps the projection current.

If Redis is lost, no data is lost. Restart the job from empty state, replay the
topic from offset 0, and the views rebuild exactly. This is why the Kubernetes
deployment configures no Redis persistence — it is derived data by construction.

### State lives in Flink, so Redis writes are idempotent

This is the load-bearing decision.

Flink has no official Redis connector — the Apache Bahir one was
[retired](https://issues.apache.org/jira/browse/BAHIR-259) and never supported
PyFlink. The sink here is therefore a plain `FlatMapFunction` holding a redis-py
client, which sits outside Flink's checkpoint barrier protocol and gets
**at-least-once** delivery.

That would normally be fatal. `bike-balance` and `busiest` are *running totals*,
so if the sink issued `INCRBY 1` per event, every recovery would replay events
already counted and corrupt the totals permanently.

The fix is to invert where state lives:

| | accumulator | Redis write | replay-safe |
|---|---|---|---|
| Naive | Redis | `INCRBY 1` (delta) | ✗ corrupts on every recovery |
| **This design** | **Flink keyed state** | **`HSET balance 42` (absolute)** | **✓ restates a value** |

Flink owns the counters in checkpointed keyed state and emits the *current total*.
Every Redis write is an absolute `HSET` or `ZADD`, so replaying one is a no-op.
At-least-once delivery becomes sufficient, and the missing exactly-once sink stops
mattering. See [`redis_writer.py`](flink_job/redis_writer.py).

### The trip-stats join needs a second keying

The generator keys events by `station_id` ([line 134](event_generator.py#L134)),
which partitions all four aggregates perfectly — except one. A ride's
`trip_start` and `trip_end` carry *different* station ids, so the two halves land
on different partitions and per-partition state cannot pair them.

So the job keys the same stream twice:

```
Redpanda ──┬── key_by(station_id) ── StationStateFunction ──────────────┐
           │                                                            ├── Redis
           └── key_by(ride_id) ── TripJoinFunction ── key_by(station_id) ┘
                                       └── TripStatsFunction
```

`TripJoinFunction` holds the first half of a ride in keyed state until its
counterpart arrives, then emits the duration to *both* stations — `depart` for
the origin, `arrive` for the destination. Halves that never pair expire via
`StateTtlConfig` after 24h rather than pinning state forever.

**The join must be order-agnostic, and this is not optional.** Because the two
halves carry different station ids, they are produced to *different partitions*,
and Flink reads partitions independently with no ordering guarantee between them.
A `trip_end` therefore arrives before its `trip_start` routinely — not rarely.

An earlier version assumed `trip_start` always came first and discarded any
unmatched `trip_end`. Against a 3-partition topic that silently dropped **32% of
all trips** (33,916 of 49,954 rides matched). The endpoints still looked
plausible and the totals were internally consistent, which is what made it
dangerous. Parking whichever half arrives first takes the match rate to 100%.

Both orderings are pinned by tests in `TestJoinRideHalves`.

### Averages are stored as sum + count

A mean cannot be updated incrementally. Flink accumulates `depart_sum`/
`depart_count` and `arrive_sum`/`arrive_count`; the API divides at read time.

### No watermarks

Nothing in the job is windowed or event-time dependent — the aggregates are
unbounded running totals over the whole stream. Watermarks would be pure
overhead, so the source uses `WatermarkStrategy.no_watermarks()`.

---

## Deploying to minikube

```bash
minikube start --cpus=4 --memory=8192
make deploy
```

`make deploy` builds both images, side-loads them with `minikube image load`
(hence `imagePullPolicy: Never` — there is no registry), and applies the
OpenTofu config, which brings up:

```
cert-manager (Helm) ─▶ flink-kubernetes-operator (Helm) ─▶ FlinkDeployment (local chart)
Redpanda (manifests)   Redis (manifests)                   FastAPI (manifests)
```

**The CRD ordering problem.** The `FlinkDeployment` is a custom resource, so it
cannot be created with `kubernetes_manifest`: that resource validates against the
cluster's schema at *plan* time, and the CRD does not exist until the operator has
been applied. `depends_on` does not help, because plan runs before any apply. The
CR is therefore wrapped in a small local Helm chart
([`tofu/charts/citibike-job`](tofu/charts/citibike-job)) and installed via
`helm_release`, which only contacts the cluster during apply.

Tear down with `make destroy`.

### Notes from actually running this

A few things that are easy to get wrong, all of them found by deploying rather
than by reading:

**Kubernetes injects `REDIS_PORT` and it shadows your own.** Every Service in a
namespace produces Docker-link-style variables, so a Service named `redis` sets
`REDIS_PORT=tcp://10.x.x.x:6379`. That silently overrides the application's own
`REDIS_PORT` default and crashes the API on `int()` at startup. Both the API
Deployment and the FlinkDeployment therefore set it explicitly.

**The Flink operator chart is served from `archive.apache.org`.** Apache keeps
only the current release on `downloads.apache.org`, so any pinned version 404s
there sooner or later — including the one the operator docs suggest.

**`minikube image load <name>` does not work with the podman driver.** minikube
cannot read podman's local image store, and podman namespaces local builds under
`localhost/`. `make images` therefore goes through `docker save` to a tar, and
the image variables carry the `localhost/` prefix. On the Docker driver, drop the
prefix.

**Loading several GB of image tarballs can destabilise CRI-O.** On a single-node
minikube this showed up as `ListImages` deadline-exceeded errors and, eventually,
`image not known` for an image the kubelet had already reported as present.
Recreating the cluster clears it. Allocate generously — `--cpus=4 --memory=8192`
is the practical floor for Redpanda plus a Flink JobManager and TaskManager.

---

## Decisions and assumptions

**`bike-balance` is a signed delta, not an absolute count.** The stream never
reveals the initial dock inventory, so the value starts at 0 and may go negative.
The endpoint says so in its response.

**The provided generator contains a bug, fixed with a one-token change.**
`parse_args()` defines `--broker` ([line 37](event_generator.py#L37)) but `main()`
read `args.brokers` ([line 114](event_generator.py#L114)), so every non-dry-run
invocation raised `AttributeError` before publishing anything — including the
exact command the brief documents.

This is the one place the submission deviates from "do not modify it". The whole
diff is:

```diff
-        print(f"Connecting to Redpanda at {args.brokers}...")
+        print(f"Connecting to Redpanda at {args.broker}...")
```

The variable is only used in that log line, so the change is inert beyond making
the script run. It was deliberately preferred over a wrapper script: the brief
documents one way to invoke the generator, and that command should work as
written rather than requiring a different entry point.

**`last-activity` is overwritten unconditionally.** Events are keyed by
`station_id` in both Kafka and Flink, so a station's events share a partition and
arrive in produce order — the newest event really is the latest one. This assumes
the generator's own timestamp sort; an unsorted producer would need a guard.

**A ride half that never finds its counterpart is dropped.** Once the 24h TTL
expires, a lone `trip_start` or `trip_end` has no measurable duration — the other
half lies outside the ingested window. Guessing a duration would be worse than
omitting the trip.

**The Flink image builds natively on both amd64 and arm64.** Neither
`apache-flink` nor `pemja` — its C-extension bridge between the Python and Java
runtimes — publishes a manylinux aarch64 wheel, so arm64 must compile pemja from
source. The Dockerfile installs `build-essential` and `python3-dev` for that and
purges them in the same layer, so the toolchain never reaches the final image.
The JDK headers pemja also needs are already in the base image.

The alternative was pinning to `linux/amd64` and emulating, which is simpler but
makes the image unrunnable on a `minikube` cluster on Apple Silicon — such a
cluster is arm64, and would need binfmt/QEMU registered inside the node. Building
natively keeps the Kubernetes path working on either architecture.

**The local cluster runs with no TLS or auth.** Redpanda is deployed single-node
with `tls.enabled=false` and SASL off. Appropriate for a local dev cluster,
not a deployment posture to ship.

**Redis is deployed as plain manifests rather than a chart.** It needs no
configuration here, and this avoids a dependency on the Bitnami chart catalog.

**Dependencies are managed with uv, from a single lockfile.** One `uv.lock`
covers every component, with [PEP 735](https://peps.python.org/pep-0735/)
dependency groups (`flink`, `api`, `generator`, `dev`) keeping each image lean —
the API image installs only its group, never PyFlink's ~400MB tree.

Two constraints are pinned deliberately in `pyproject.toml`. `requires-python`
is capped at `<3.12` because PyFlink 1.20 publishes no wheels beyond 3.11, and
`.python-version` pins 3.11 so uv fetches a correct interpreter regardless of
what the host has installed. A `setuptools<81` build constraint is also needed:
`apache-beam` (a transitive PyFlink dependency) has a `setup.py` that imports
`pkg_resources`, which setuptools 81 removed — without it, installing on a
platform lacking prebuilt beam wheels (macOS arm64, for one) fails outright.

---

## Why Flink

Honest framing: for this workload Flink is more machinery than the problem
strictly requires. The four aggregates are unbounded running counters with no
windowing, no watermarks, and no late-data reconciliation — a ~150-line Python
consumer using Redis' atomic `INCRBY`/`ZINCRBY` would produce identical output
with a fraction of the operational surface.

Flink earns its place on two counts. The `ride_id` join with TTL'd keyed state is
genuinely cleaner as a `KeyedProcessFunction` than as hand-rolled Redis
bookkeeping. And the checkpointed-state design above gives correct recovery
semantics that the naive version has to reconstruct by hand (typically by
committing Kafka offsets into Redis inside the same transaction).

The tradeoff worth naming: Flink's headline feature is exactly-once, and the
absent Redis connector means it cannot be realised at the final hop. The
idempotent-write design recovers the *guarantee* without the mechanism.

---

## Testing

```bash
make test
```

The aggregation logic in [`aggregates.py`](flink_job/aggregates.py) is pure —
no Flink, no Redis, no I/O — so the semantics that actually matter (balance
direction, out-of-order guarding, sum/count means, duration parsing) are covered
by fast unit tests. Flink operators own only state and timers.

## System design

[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) covers the shape of the pipeline,
why aggregation happens on write, why Flink, and what would change for
production.

## Engineering log

[`docs/ENGINEERING_LOG.md`](docs/ENGINEERING_LOG.md) records every problem hit
while building and deploying this, with the symptom, the root cause and the fix —
including a join bug that silently discarded 32% of trips while leaving every
metric internally consistent.

## Possible next steps

- **Cold path**: a second consumer group archiving raw events to MinIO as
  Iceberg, making history queryable via Trino. Redis answers "now"; Iceberg
  would answer "over time".
- **Integration tests** with testcontainers against real Redpanda and Redis.
- **RocksDB state backend** and object-store checkpoints, if state outgrew heap.
- **Observability**: Flink metrics to Prometheus, consumer lag alerting.
