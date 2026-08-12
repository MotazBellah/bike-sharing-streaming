"""HTTP serving layer over the Redis materialized views."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Annotated

import redis
from fastapi import Depends, FastAPI, HTTPException

from .models import BikeBalance, BusiestStation, Health, LastActivity, TripStats
from .store import Store

state: dict[str, Store] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
    )
    state["store"] = Store(client)
    yield
    client.close()


app = FastAPI(
    title="Citibike Station API",
    description="Low-latency reads over Flink-computed station aggregates.",
    lifespan=lifespan,
)


def get_store() -> Store:
    """Injected, so tests can override it."""
    return state["store"]


# Annotated rather than a Depends() default: the modern FastAPI style, and it
# keeps the dependency out of the function signature's default arguments.
StoreDep = Annotated[Store, Depends(get_store)]


def or_404(value, station_id: str):
    """A miss means no events for that station have been seen yet.

    Indistinguishable from an unknown station id, since the pipeline only ever
    learns about stations that appear in the stream.
    """
    if value is None:
        raise HTTPException(
            status_code=404, detail=f"No data yet for station {station_id!r}"
        )
    return value


@app.get("/health", response_model=Health)
def health(store: StoreDep):
    try:
        store.ping()
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=f"redis unavailable: {exc}") from exc
    return Health()


# Declared before /stations/{station_id}/... so "busiest" is never parsed
# as a station id.
@app.get(
    "/stations/busiest",
    response_model=BusiestStation,
    summary="Station with the highest total event count",
)
def busiest(store: StoreDep):
    result = store.busiest_station()
    if result is None:
        raise HTTPException(status_code=404, detail="No events processed yet")
    return result


@app.get(
    "/stations/{station_id}/last-activity",
    response_model=LastActivity,
    summary="Most recent event at a station",
)
def last_activity(station_id: str, store: StoreDep):
    return or_404(store.last_activity(station_id), station_id)


@app.get(
    "/stations/{station_id}/bike-balance",
    response_model=BikeBalance,
    summary="Net bike delta at a station since stream start",
)
def bike_balance(station_id: str, store: StoreDep):
    return or_404(store.bike_balance(station_id), station_id)


@app.get(
    "/stations/{station_id}/trip-stats",
    response_model=TripStats,
    summary="Mean duration of trips departing from and arriving at a station",
)
def trip_stats(station_id: str, store: StoreDep):
    return or_404(store.trip_stats(station_id), station_id)
