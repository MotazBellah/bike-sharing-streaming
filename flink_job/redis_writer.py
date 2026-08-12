"""Redis sink."""

from __future__ import annotations

import logging

import redis
from pyflink.datastream.functions import FlatMapFunction, RuntimeContext

logger = logging.getLogger(__name__)

ACTIVITY_KEY = "stations:activity"


def station_state_key(station_id: str) -> str:
    return f"station:{station_id}:state"


def trip_stats_key(station_id: str) -> str:
    return f"station:{station_id}:trip_stats"


class RedisWriter(FlatMapFunction):
    def __init__(self, host: str, port: int = 6379):
        self._host = host
        self._port = port
        self._client: redis.Redis | None = None

    def open(self, ctx: RuntimeContext):
        # Opened per subtask, not per record — a client here is a connection pool.
        self._client = redis.Redis(
            host=self._host, port=self._port, decode_responses=True
        )

    def close(self):
        if self._client is not None:
            self._client.close()

    def flat_map(self, value: dict):
        station_id = value["station_id"]
        pipe = self._client.pipeline(transaction=True)

        if value["kind"] == "station_state":
            pipe.hset(station_state_key(station_id), mapping=value["fields"])
            # ZADD with an absolute score, so busiest stays correct under replay.
            pipe.zadd(ACTIVITY_KEY, {station_id: value["activity_score"]})
        elif value["kind"] == "trip_stats":
            pipe.hset(trip_stats_key(station_id), mapping=value["fields"])
        else:
            logger.warning("unknown update kind: %s", value["kind"])
            return []

        pipe.execute()
        return []
