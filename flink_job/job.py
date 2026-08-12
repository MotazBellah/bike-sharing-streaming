"""Citibike streaming aggregation job.

    Redpanda ──┬── key_by(station_id) ── StationStateFunction ──┐
               │                                                ├── RedisWriter ── Redis
               └── key_by(ride_id) ── TripJoinFunction          │
                        └── key_by(station_id) ── TripStatsFunction ┘

Two keyings of the same source: station_id for the per-station counters, and
ride_id for the start/end join, which then re-keys by station to accumulate.
"""

from __future__ import annotations

import json
import logging
import os

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

from .functions import StationStateFunction, TripJoinFunction, TripStatsFunction
from .redis_writer import RedisWriter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ("ride_id", "event_type", "station_id", "timestamp")


def parse_event(raw: str) -> dict | None:
    """Decode a record, returning None for anything malformed.

    A single bad record should not take down the job, so parsing is total and
    filtering happens downstream.
    """
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("skipping unparseable record: %.120s", raw)
        return None

    if not all(event.get(field) for field in REQUIRED_FIELDS):
        logger.warning("skipping record with missing fields: %s", event)
        return None
    return event


def build_source(brokers: str, topic: str, group_id: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(brokers)
        .set_topics(topic)
        .set_group_id(group_id)
        # "since stream start" in the brief means the whole topic, so on a cold
        # start we read from the beginning rather than the committed offset.
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def main():
    brokers = os.getenv("KAFKA_BROKERS", "redpanda:9092")
    topic = os.getenv("KAFKA_TOPIC", "citibike-events")
    group_id = os.getenv("KAFKA_GROUP_ID", "citibike-flink")
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    parallelism = int(os.getenv("FLINK_PARALLELISM", "2"))

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)
    # Checkpoints are what make the Redis writes recoverable: state rewinds,
    # the replay restates the same absolute values, and Redis converges.
    env.enable_checkpointing(30_000)

    events = (
        env.from_source(
            build_source(brokers, topic, group_id),
            # No windows or event-time logic anywhere in this job, so watermarks
            # would be dead weight.
            WatermarkStrategy.no_watermarks(),
            "citibike-events",
        )
        .map(parse_event, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda event: event is not None)
        .name("parse-events")
    )

    station_updates = (
        events.key_by(lambda e: e["station_id"], key_type=Types.STRING())
        .process(StationStateFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("station-state")
    )

    trip_updates = (
        events.key_by(lambda e: e["ride_id"], key_type=Types.STRING())
        .process(TripJoinFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("trip-join")
        .key_by(lambda t: t["station_id"], key_type=Types.STRING())
        .process(TripStatsFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("trip-stats")
    )

    (
        station_updates.union(trip_updates)
        .flat_map(RedisWriter(redis_host, redis_port), output_type=Types.STRING())
        .name("redis-writer")
        # RedisWriter emits nothing, so this sink stays silent. It exists
        # because Flink requires the job graph to terminate in one.
        .print()
    )

    env.execute("citibike-aggregator")


if __name__ == "__main__":
    main()
