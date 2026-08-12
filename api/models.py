"""API response contract."""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class LastActivity(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "station_id": "6233.04",
                "timestamp": "2026-04-15T20:15:51.012000+00:00",
                "event_type": "trip_end",
            }
        }
    )

    station_id: str
    timestamp: str = Field(description="ISO-8601 timestamp of the most recent event.")
    event_type: str = Field(description="Either trip_start or trip_end.")


class BikeBalance(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"example": {"station_id": "6233.04", "balance": -14}}
    )

    station_id: str
    # The initial dock inventory is unknowable from the stream, so this is a
    # net delta and may be negative. That caveat lives here, in the schema,
    # rather than being echoed back in every response body.
    balance: int = Field(
        description=(
            "Net change since stream start, not an absolute bike count: "
            "-1 per departure, +1 per arrival. May be negative."
        )
    )


class BusiestStation(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"example": {"station_id": "6233.04", "event_count": 2259}}
    )

    station_id: str
    event_count: int = Field(description="Total events seen at this station.")


class TripStats(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "station_id": "5679.08",
                "avg_departure_seconds": 577.95,
                "avg_arrival_seconds": 648.11,
                "departure_trips": 210,
                "arrival_trips": 1714,
            }
        }
    )

    station_id: str
    # Null rather than zero when a station has no trips in that direction —
    # a mean over nothing is undefined, and 0 would read as "instant trips".
    avg_departure_seconds: float | None = Field(
        default=None, description="Mean duration of trips departing here. Null if none."
    )
    avg_arrival_seconds: float | None = Field(
        default=None, description="Mean duration of trips arriving here. Null if none."
    )
    departure_trips: int = Field(description="Completed trips that started here.")
    arrival_trips: int = Field(description="Completed trips that ended here.")


class Health(BaseModel):
    status: str = "ok"
