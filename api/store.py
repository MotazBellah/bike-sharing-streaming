"""Redis read layer."""

from __future__ import annotations

import redis

from .models import BikeBalance, BusiestStation, LastActivity, TripStats

ACTIVITY_KEY = "stations:activity"


class Store:
    def __init__(self, client: redis.Redis):
        self._client = client

    def ping(self) -> bool:
        return bool(self._client.ping())

    def last_activity(self, station_id: str) -> LastActivity | None:
        state = self._client.hgetall(f"station:{station_id}:state")
        if not state or not state.get("last_timestamp"):
            return None
        return LastActivity(
            station_id=station_id,
            timestamp=state["last_timestamp"],
            event_type=state["last_event_type"],
        )

    def bike_balance(self, station_id: str) -> BikeBalance | None:
        balance = self._client.hget(f"station:{station_id}:state", "balance")
        if balance is None:
            return None
        return BikeBalance(station_id=station_id, balance=int(balance))

    def busiest_station(self) -> BusiestStation | None:
        top = self._client.zrevrange(ACTIVITY_KEY, 0, 0, withscores=True)
        if not top:
            return None
        station_id, score = top[0]
        return BusiestStation(station_id=station_id, event_count=int(score))

    def trip_stats(self, station_id: str) -> TripStats | None:
        stats = self._client.hgetall(f"station:{station_id}:trip_stats")
        if not stats:
            return None

        depart_count = int(stats.get("depart_count", 0))
        arrive_count = int(stats.get("arrive_count", 0))

        return TripStats(
            station_id=station_id,
            # The mean is derived here rather than stored: a running average
            # cannot be updated incrementally, so Flink accumulates sum+count.
            avg_departure_seconds=(
                round(int(stats["depart_sum"]) / depart_count, 2) if depart_count else None
            ),
            avg_arrival_seconds=(
                round(int(stats["arrive_sum"]) / arrive_count, 2) if arrive_count else None
            ),
            departure_trips=depart_count,
            arrival_trips=arrive_count,
        )
