"""Aggregation logic.

Free of Flink, Redis and I/O so it can be unit-tested in milliseconds. The
operators in `functions.py` own state and timers; the state objects here own
the rules for updating themselves.

The dataclasses are mutable on purpose. Flink deserializes a fresh object from
managed state on every `value()` call, so there is no shared instance to
corrupt by mutating in place.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime

TRIP_START = "trip_start"
TRIP_END = "trip_end"

# A trip_start means a bike left the dock, a trip_end means one arrived.
BALANCE_DELTA = {TRIP_START: -1, TRIP_END: +1}


@dataclass
class StationState:
    """Everything the station-keyed stream tracks for one station."""

    last_timestamp: str = ""
    last_event_type: str = ""
    balance: int = 0
    event_count: int = 0

    def record(self, event: dict) -> None:
        # last_* is overwritten unconditionally. Events are keyed by station_id
        # in both Kafka and Flink, so a station's events share a partition and
        # arrive in produce order — the newest event really is the latest one.
        self.last_timestamp = event["timestamp"]
        self.last_event_type = event["event_type"]
        self.balance += BALANCE_DELTA.get(event["event_type"], 0)
        self.event_count += 1

    def as_redis_hash(self) -> dict[str, str]:
        return {k: str(v) for k, v in asdict(self).items()}


@dataclass
class TripStatsState:
    """Running sum/count for trips departing from and arriving at one station.

    Averages are not stored: you cannot update a mean incrementally without
    also keeping the count, so we keep both and divide at read time.
    """

    depart_sum: int = 0
    depart_count: int = 0
    arrive_sum: int = 0
    arrive_count: int = 0

    def record_departure(self, duration: int) -> None:
        self.depart_sum += duration
        self.depart_count += 1

    def record_arrival(self, duration: int) -> None:
        self.arrive_sum += duration
        self.arrive_count += 1

    def as_redis_hash(self) -> dict[str, str]:
        return {k: str(v) for k, v in asdict(self).items()}


@dataclass(frozen=True)
class RideHalf:
    """One half of a ride, waiting for its counterpart."""

    event_type: str
    station_id: str
    timestamp: str


def as_ride_half(event: dict) -> RideHalf:
    return RideHalf(
        event_type=event["event_type"],
        station_id=event["station_id"],
        timestamp=event["timestamp"],
    )


def parse_timestamp(raw: str) -> datetime:
    """Accept both the generator's `...+00:00` and the brief's `...Z`."""
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def trip_duration_seconds(started_at: str, ended_at: str) -> int:
    return int((parse_timestamp(ended_at) - parse_timestamp(started_at)).total_seconds())


def join_ride_halves(pending: RideHalf, event: dict) -> list[dict] | None:
    """Join a ride's two halves, in whichever order they arrived.

    The halves carry different station_ids, so they are produced to different
    Kafka partitions and read by independent Flink subtasks. There is no
    ordering guarantee between partitions, so `trip_end` routinely arrives
    before its `trip_start`. Treating either half as "the first one seen" is
    what keeps those trips from being dropped.

    Returns None when the two halves cannot form a trip (same type twice, i.e.
    a duplicate or a redelivery).
    """
    incoming = as_ride_half(event)
    if incoming.event_type == pending.event_type:
        return None

    start, end = (
        (pending, incoming) if pending.event_type == TRIP_START else (incoming, pending)
    )
    duration = trip_duration_seconds(start.timestamp, end.timestamp)

    # One trip updates two stations: where it left from, where it arrived.
    return [
        {"station_id": start.station_id, "role": "depart", "duration": duration},
        {"station_id": end.station_id, "role": "arrive", "duration": duration},
    ]
