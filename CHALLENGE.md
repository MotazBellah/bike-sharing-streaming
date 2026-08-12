# Data Engineering Coding Challenge

## Task

Build a small but working end-to-end pipeline that ingests bike-sharing events and makes them queryable with low latency.

---

## Dataset

Download one or more months of **Citibike NYC trip history**:
- https://citibikenyc.com/system-data

We have been using "202604-citibike-tripdata.zip", but any with the same cols will do.

Do not submit data files.

---

## Stack

```
event_generator.py → Redpanda → [ ??? ] → Redis → HTTP API
```

We provide you with the `event_generator.py` script, which reads the Citibike CSV and publishes events to Redpanda. You must implement the rest of the pipeline.

---

## Event Generator

`event_generator.py` reads the Citibike CSV and publishes events to Redpanda. Do not modify it.

Basic usage:

```bash
python event_generator.py --file citibike_data.csv --topic citibike-events --broker localhost:19092
```

Each event is a JSON object:

```json
{
  "ride_id": "string",
  "event_type": "trip_start | trip_end",
  "station_id": "string",
  "station_name": "string",
  "rideable_type": "classic_bike | electric_bike | docked_bike",
  "member_casual": "member | casual",
  "timestamp": "2024-01-15T08:32:01Z"
}
```

---

## API Requirements

Expose four endpoints:

| Endpoint | Returns |
|---|---|
| `GET /stations/{station_id}/last-activity` | Timestamp and type of the most recent event at a station |
| `GET /stations/{station_id}/bike-balance` | Balance of available bikes at a station since stream start |
| `GET /stations/busiest` | Station with the highest total event count since stream start |
| `GET /stations/{station_id}/trip-stats` | Average duration in seconds of trips departing from and arriving at a station |

---

## Deliverables

A git repository containing:

- `docker-compose.yml` — brings up the full stack (Redpanda, ingestion, Redis, API)
- `README.md` — how to run it, and a brief note on your key architectural choices
- All source code and configuration file required
- Provide a way to deploy this in a local `minikube` Kubernetes cluster using OpenTofu/Terraform.

---

## Notes

- Estimated effort: 4 to 6 hours
- If something is unclear to you, make a decision and document it in your README
