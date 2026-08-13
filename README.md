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
 make generate CSV=202604-citibike-tripdata/202604-citibike-tripdata-part1.csv
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

## Deploying to minikube

```bash
minikube start --cpus=4 --memory=16192
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

**Accessing the API locally.** The `citibike-api` service is a `NodePort`
(port 30080), so on Linux you can hit it directly at
`http://$(minikube ip):30080`. On macOS/Windows the minikube VM/container
network usually isn't reachable that way, so port-forward instead:

```bash
kubectl -n citibike port-forward svc/citibike-api 8000:8000
curl localhost:8000/stations/busiest
```

The Flink dashboard can be reached the same way:

```bash
kubectl -n citibike port-forward svc/citibike-aggregator-rest 8081:8081
```

Tear down with `make destroy`.

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
