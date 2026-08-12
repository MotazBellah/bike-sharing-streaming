"""Flink operators.

Each function owns Flink managed state and emits the *absolute* current value
of that state, never a delta. That is what makes the Redis sink idempotent:
on recovery Flink rewinds to the last checkpoint and replays, and every replayed
write simply restates a value rather than incrementing a counter a second time.
"""

from __future__ import annotations

from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

from .aggregates import (
    TRIP_END,
    TRIP_START,
    StationState,
    TripStatsState,
    as_ride_half,
    join_ride_halves,
)

# How long an unmatched ride half is kept while waiting for its counterpart.
PENDING_TRIP_TTL_HOURS = 24


class StationStateFunction(KeyedProcessFunction):
    """Keyed by station_id. Serves last-activity, bike-balance and busiest."""

    def open(self, ctx: RuntimeContext):
        self._state = ctx.get_state(
            ValueStateDescriptor("station_state", Types.PICKLED_BYTE_ARRAY())
        )

    def process_element(self, value: dict, ctx: KeyedProcessFunction.Context):
        state = self._state.value() or StationState()
        state.record(value)
        self._state.update(state)

        yield {
            "kind": "station_state",
            "station_id": ctx.get_current_key(),
            "fields": state.as_redis_hash(),
            # Doubles as the sorted-set score backing GET /stations/busiest.
            "activity_score": state.event_count,
        }


class TripJoinFunction(KeyedProcessFunction):
    """Keyed by ride_id. Pairs each trip_start with its trip_end."""

    def open(self, ctx: RuntimeContext):
        descriptor = ValueStateDescriptor("pending_half", Types.PICKLED_BYTE_ARRAY())
        # A ride whose counterpart never arrives would otherwise pin state forever.
        descriptor.enable_time_to_live(
            StateTtlConfig.new_builder(Time.hours(PENDING_TRIP_TTL_HOURS))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        self._pending = ctx.get_state(descriptor)

    def process_element(self, value: dict, ctx: KeyedProcessFunction.Context):
        if value["event_type"] not in (TRIP_START, TRIP_END):
            return

        pending = self._pending.value()
        if pending is None:
            # First half of this ride to arrive — park it, whichever half it is.
            # It is NOT safe to assume that is the trip_start: the two halves
            # live on different partitions and arrive in either order.
            self._pending.update(as_ride_half(value))
            return

        emissions = join_ride_halves(pending, value)
        if emissions is None:
            # Same half twice: a duplicate or redelivery. Keep what we have.
            return

        self._pending.clear()
        yield from emissions


class TripStatsFunction(KeyedProcessFunction):
    """Keyed by station_id. Accumulates sum+count so the API can take the mean."""

    def open(self, ctx: RuntimeContext):
        self._state = ctx.get_state(
            ValueStateDescriptor("trip_stats", Types.PICKLED_BYTE_ARRAY())
        )

    def process_element(self, value: dict, ctx: KeyedProcessFunction.Context):
        state = self._state.value() or TripStatsState()
        if value["role"] == "depart":
            state.record_departure(value["duration"])
        else:
            state.record_arrival(value["duration"])
        self._state.update(state)

        yield {
            "kind": "trip_stats",
            "station_id": ctx.get_current_key(),
            "fields": state.as_redis_hash(),
        }
