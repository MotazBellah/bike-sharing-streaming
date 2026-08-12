"""Endpoint tests.

Backed by a fake Redis rather than a real one, so these run in milliseconds
with no infrastructure. The keys are seeded in exactly the shape the Flink
RedisWriter produces.
"""

import fakeredis
import pytest
import redis
from fastapi.testclient import TestClient

from api.main import app, get_store
from api.store import ACTIVITY_KEY, Store


def seed(client: fakeredis.FakeRedis):
    """Write the keys the Flink job would have written."""
    client.hset(
        "station:S1:state",
        mapping={
            "last_timestamp": "2026-04-15T20:15:51.012000+00:00",
            "last_event_type": "trip_end",
            "balance": "-14",
            "event_count": "203",
        },
    )
    client.hset(
        "station:S2:state",
        mapping={
            "last_timestamp": "2026-04-15T18:02:11.000000+00:00",
            "last_event_type": "trip_start",
            "balance": "7",
            "event_count": "451",
        },
    )
    client.zadd(ACTIVITY_KEY, {"S1": 203, "S2": 451})

    # 3 departures totalling 1800s (mean 600), 2 arrivals totalling 900s (mean 450).
    client.hset(
        "station:S1:trip_stats",
        mapping={
            "depart_sum": "1800",
            "depart_count": "3",
            "arrive_sum": "900",
            "arrive_count": "2",
        },
    )
    # Arrivals only — departures must come back null, not zero.
    client.hset(
        "station:S2:trip_stats",
        mapping={"depart_sum": "0", "depart_count": "0", "arrive_sum": "300", "arrive_count": "1"},
    )


@pytest.fixture
def client():
    fake = fakeredis.FakeRedis(decode_responses=True)
    seed(fake)
    # TestClient is deliberately not used as a context manager: that would run
    # the lifespan and try to open a real Redis connection.
    app.dependency_overrides[get_store] = lambda: Store(fake)
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def empty_client():
    fake = fakeredis.FakeRedis(decode_responses=True)
    app.dependency_overrides[get_store] = lambda: Store(fake)
    yield TestClient(app)
    app.dependency_overrides.clear()


class TestHealth:
    def test_ok(self, client):
        assert client.get("/health").json() == {"status": "ok"}

    def test_returns_503_when_redis_is_down(self):
        class Dead(Store):
            def ping(self):
                raise redis.ConnectionError("connection refused")

        app.dependency_overrides[get_store] = lambda: Dead(None)
        response = TestClient(app).get("/health")
        app.dependency_overrides.clear()

        assert response.status_code == 503
        assert "redis unavailable" in response.json()["detail"]


class TestLastActivity:
    def test_returns_most_recent_event(self, client):
        body = client.get("/stations/S1/last-activity").json()
        assert body == {
            "station_id": "S1",
            "timestamp": "2026-04-15T20:15:51.012000+00:00",
            "event_type": "trip_end",
        }

    def test_unknown_station_is_404(self, client):
        assert client.get("/stations/NOPE/last-activity").status_code == 404


class TestBikeBalance:
    def test_negative_balance_is_preserved(self, client):
        # Balance is a signed delta, so a negative value is valid, not an error.
        body = client.get("/stations/S1/bike-balance").json()
        assert body["balance"] == -14

    def test_response_carries_only_the_data(self, client):
        # The "net delta, not absolute count" caveat belongs in the schema,
        # not echoed back on every request.
        assert client.get("/stations/S1/bike-balance").json() == {
            "station_id": "S1",
            "balance": -14,
        }

    def test_unknown_station_is_404(self, client):
        assert client.get("/stations/NOPE/bike-balance").status_code == 404


class TestBusiest:
    def test_returns_highest_event_count(self, client):
        assert client.get("/stations/busiest").json() == {
            "station_id": "S2",
            "event_count": 451,
        }

    def test_busiest_is_not_parsed_as_a_station_id(self, client):
        # /stations/busiest is declared before /stations/{station_id}/... —
        # reorder the routes and this silently starts 404ing.
        assert client.get("/stations/busiest").status_code == 200

    def test_404_before_any_events(self, empty_client):
        assert empty_client.get("/stations/busiest").status_code == 404


class TestTripStats:
    def test_means_are_derived_from_sum_and_count(self, client):
        body = client.get("/stations/S1/trip-stats").json()
        assert body["avg_departure_seconds"] == 600.0  # 1800 / 3
        assert body["avg_arrival_seconds"] == 450.0  # 900 / 2
        assert body["departure_trips"] == 3
        assert body["arrival_trips"] == 2

    def test_missing_direction_is_null_not_zero(self, client):
        # A mean over zero trips is undefined; 0.0 would read as "instant trips".
        body = client.get("/stations/S2/trip-stats").json()
        assert body["avg_departure_seconds"] is None
        assert body["departure_trips"] == 0
        assert body["avg_arrival_seconds"] == 300.0

    def test_unknown_station_is_404(self, client):
        assert client.get("/stations/NOPE/trip-stats").status_code == 404


class TestSchema:
    def test_openapi_documents_every_endpoint(self, client):
        paths = client.get("/openapi.json").json()["paths"]
        assert set(paths) == {
            "/health",
            "/stations/busiest",
            "/stations/{station_id}/last-activity",
            "/stations/{station_id}/bike-balance",
            "/stations/{station_id}/trip-stats",
        }

    def test_nullable_means_are_declared_nullable(self, client):
        schema = client.get("/openapi.json").json()
        props = schema["components"]["schemas"]["TripStats"]["properties"]
        assert {"type": "null"} in props["avg_departure_seconds"]["anyOf"]
