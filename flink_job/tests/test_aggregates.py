from flink_job.aggregates import (
    StationState,
    TripStatsState,
    as_ride_half,
    join_ride_halves,
    trip_duration_seconds,
)


def event(event_type: str, timestamp: str, station_id: str = "S1") -> dict:
    return {
        "ride_id": "R1",
        "event_type": event_type,
        "station_id": station_id,
        "station_name": "Test Station",
        "rideable_type": "classic_bike",
        "member_casual": "member",
        "timestamp": timestamp,
    }


class TestStationState:
    def test_trip_start_decrements_balance(self):
        state = StationState()
        state.record(event("trip_start", "2026-04-01T08:00:00+00:00"))
        assert state.balance == -1
        assert state.event_count == 1

    def test_trip_end_increments_balance(self):
        state = StationState()
        state.record(event("trip_end", "2026-04-01T08:00:00+00:00"))
        assert state.balance == 1

    def test_balance_is_a_net_delta_and_may_go_negative(self):
        # We never learn the initial dock inventory, so balance is a signed
        # delta since stream start rather than an absolute bike count.
        state = StationState()
        for _ in range(3):
            state.record(event("trip_start", "2026-04-01T08:00:00+00:00"))
        assert state.balance == -3

    def test_last_activity_tracks_most_recent_event(self):
        state = StationState()
        state.record(event("trip_start", "2026-04-01T08:00:00+00:00"))
        state.record(event("trip_end", "2026-04-01T09:00:00+00:00"))
        assert state.last_timestamp == "2026-04-01T09:00:00+00:00"
        assert state.last_event_type == "trip_end"

    def test_event_count_counts_both_event_types(self):
        state = StationState()
        state.record(event("trip_start", "2026-04-01T08:00:00+00:00"))
        state.record(event("trip_end", "2026-04-01T08:30:00+00:00"))
        assert state.event_count == 2

    def test_serialises_every_field_as_a_string_for_redis(self):
        state = StationState()
        state.record(event("trip_end", "2026-04-01T08:00:00+00:00"))
        assert state.as_redis_hash() == {
            "last_timestamp": "2026-04-01T08:00:00+00:00",
            "last_event_type": "trip_end",
            "balance": "1",
            "event_count": "1",
        }


class TestTripDuration:
    def test_computes_seconds(self):
        assert trip_duration_seconds("2026-04-01T08:00:00+00:00", "2026-04-01T08:12:00+00:00") == 720

    def test_handles_mixed_timestamp_formats(self):
        assert trip_duration_seconds("2026-04-01T08:00:00Z", "2026-04-01T08:01:00+00:00") == 60


class TestJoinRideHalves:
    """The two halves of a ride are produced to different partitions (they carry
    different station ids), so Flink may read them in either order.
    """

    START = event("trip_start", "2026-04-01T08:00:00+00:00", station_id="S1")
    END = event("trip_end", "2026-04-01T08:12:00+00:00", station_id="S2")

    def test_start_seen_first(self):
        assert join_ride_halves(as_ride_half(self.START), self.END) == [
            {"station_id": "S1", "role": "depart", "duration": 720},
            {"station_id": "S2", "role": "arrive", "duration": 720},
        ]

    def test_end_seen_first_gives_identical_result(self):
        # The regression that mattered: an earlier version dropped these
        # outright, losing ~32% of trips against a 3-partition topic.
        assert join_ride_halves(as_ride_half(self.END), self.START) == [
            {"station_id": "S1", "role": "depart", "duration": 720},
            {"station_id": "S2", "role": "arrive", "duration": 720},
        ]

    def test_duration_is_never_negative_regardless_of_arrival_order(self):
        forward = join_ride_halves(as_ride_half(self.START), self.END)
        reverse = join_ride_halves(as_ride_half(self.END), self.START)
        assert forward[0]["duration"] == reverse[0]["duration"] == 720

    def test_duplicate_half_is_rejected(self):
        assert join_ride_halves(as_ride_half(self.START), self.START) is None
        assert join_ride_halves(as_ride_half(self.END), self.END) is None


class TestTripStatsState:
    def test_departures_and_arrivals_accumulate_separately(self):
        state = TripStatsState()
        state.record_departure(600)
        state.record_arrival(300)
        assert (state.depart_sum, state.depart_count) == (600, 1)
        assert (state.arrive_sum, state.arrive_count) == (300, 1)

    def test_sum_and_count_yield_the_mean(self):
        state = TripStatsState()
        for duration in (600, 900, 300):
            state.record_departure(duration)
        assert state.depart_sum / state.depart_count == 600

    def test_serialises_every_field_as_a_string_for_redis(self):
        state = TripStatsState()
        state.record_arrival(300)
        assert state.as_redis_hash() == {
            "depart_sum": "0",
            "depart_count": "0",
            "arrive_sum": "300",
            "arrive_count": "1",
        }
