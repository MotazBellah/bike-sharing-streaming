"""
Event Generator — Citibike NYC
==============================
Reads a Citibike trip history CSV and publishes events to a Redpanda topic,
simulating a live event stream.

Each CSV row produces two events:
  - trip_start: from the origin station at started_at
  - trip_end:   at the destination station at ended_at

Events are published in chronological order with configurable pacing.

Expected CSV columns (current Citibike format):
    ride_id, rideable_type, started_at, ended_at,
    start_station_name, start_station_id,
    end_station_name, end_station_id,
    start_lat, start_lng, end_lat, end_lng, member_casual

Basic usage:
    python event_generator.py --file citibike_data.csv --topic citibike-events --broker localhost:19092
"""

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


def parse_args():
    parser = argparse.ArgumentParser(description="Citibike event generator for Redpanda")
    parser.add_argument("--file", required=True, help="Path to Citibike CSV file")
    parser.add_argument("--topic", default="citibike-events", help="Redpanda topic name")
    parser.add_argument("--broker", default="localhost:19092", help="Redpanda broker address")
    parser.add_argument("--interval", type=float, default=0.1, help="Seconds between ticks (default: 0.1)")
    parser.add_argument("--burst", type=int, default=1, help="Events per tick (default: 1)")
    parser.add_argument("--dry-run", action="store_true", help="Print events to stdout, do not publish")
    return parser.parse_args()


def load_events(filepath: str) -> list[dict]:
    events = []

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader):
            try:
                ride_id = row["ride_id"].strip()
                start_station_id = row["start_station_id"].strip()
                end_station_id = row["end_station_id"].strip()

                # Skip rows with missing station data
                if not ride_id or not start_station_id or not end_station_id:
                    continue

                rideable_type = row["rideable_type"].strip()
                member_casual = row["member_casual"].strip()
                started_at = _normalise_timestamp(row["started_at"].strip())
                ended_at = _normalise_timestamp(row["ended_at"].strip())

                events.append({
                    "ride_id": ride_id,
                    "event_type": "trip_start",
                    "station_id": start_station_id,
                    "station_name": row["start_station_name"].strip(),
                    "rideable_type": rideable_type,
                    "member_casual": member_casual,
                    "timestamp": started_at,
                })

                events.append({
                    "ride_id": ride_id,
                    "event_type": "trip_end",
                    "station_id": end_station_id,
                    "station_name": row["end_station_name"].strip(),
                    "rideable_type": rideable_type,
                    "member_casual": member_casual,
                    "timestamp": ended_at,
                })

            except (KeyError, ValueError) as e:
                print(f"[warn] Skipping row {i}: {e}", file=sys.stderr)
                continue

    # Sort events by timestamp to ensure chronological order
    events.sort(key=lambda e: e["timestamp"])
    return events


def _normalise_timestamp(raw: str) -> str:
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    raise ValueError(f"Unparseable timestamp: {raw!r}")


def main():
    args = parse_args()

    print(f"Loading events from {args.file}...")
    events = load_events(args.file)
    print(f"Loaded {len(events)} events.")

    producer = None
    if args.dry_run:
        print("Dry-run mode: printing to stdout.")
    else:
        print(f"Connecting to Redpanda at {args.broker}...")
        producer = KafkaProducer(
            bootstrap_servers=[args.broker],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
        print(f"Publishing to topic '{args.topic}'...")

    published = 0
    batch = []

    for event in events:
        batch.append(event)

        if len(batch) >= args.burst:
            for e in batch:
                if args.dry_run:
                    print(json.dumps(e))
                else:
                    producer.send(args.topic, value=e, key=e["station_id"].encode("utf-8"))
                published += 1
            batch = []
            print(f"\r[{datetime.now().isoformat()}] Published {published} events", flush=True)
            time.sleep(args.interval)

    for e in batch:
        if args.dry_run:
            print(json.dumps(e))
        else:
            producer.send(args.topic, value=e, key=e["station_id"].encode("utf-8"))
        published += 1

    if producer:
        producer.flush()
        producer.close()

    print(f"\nDone. Total events published: {published}")


if __name__ == "__main__":
    main()
