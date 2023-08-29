from datetime import datetime
from pydantic import BaseModel, Field, AliasChoices, field_validator


def get_station_id_aliases(start_or_end: str) -> tuple[str]:
    return (
        f"{start_or_end}Station Id",
        f"{start_or_end} station number",
        f"{start_or_end} Station Id",
        f"{start_or_end}Station Logical Terminal",
    )


def get_station_name_aliases(start_or_end: str) -> tuple[str]:
    return (
        f"{start_or_end}Station Name",
        f"{start_or_end} Station Name",
        f"{start_or_end} station",
    )


def get_time_aliases(start_or_end: str) -> tuple[str]:
    return (
        f"{start_or_end} Date",
        f"{start_or_end} date",
    )


class Station(BaseModel):
    station_id: int
    station_name: str
    lat: float | None = None
    lng: float | None = None


class Ride(BaseModel):
    rental_id: int = Field(validation_alias=AliasChoices("Rental Id", "Number"))
    start_station_id: int = Field(validation_alias=AliasChoices(*get_station_id_aliases("Start")))
    start_station_name: str = Field(validation_alias=AliasChoices(*get_station_name_aliases("Start")))
    end_station_id: int | None = Field(validation_alias=AliasChoices(*get_station_id_aliases("End")))
    end_station_name: str | None = Field(validation_alias=AliasChoices(*get_station_name_aliases("End")))
    bike_id: int | None = Field(validation_alias=AliasChoices("Bike Id", "Bike number"))
    start_time: datetime = Field(validation_alias=AliasChoices(*get_time_aliases("Start")))
    end_time: datetime | None = Field(validation_alias=AliasChoices(*get_time_aliases("End")))
    duration: int | None = Field(validation_alias=AliasChoices("Duration", "Duration_Seconds"), gte=0)

    @field_validator("duration", mode="before")
    @classmethod
    def validate_duration(cls, v: str | None) -> int:
        if v is None:
            return None
        v = int(v)
        return v if v >= 0 else None

    @field_validator("start_time", "end_time", mode="before")
    @classmethod
    def parse_datetime(cls, v: str | None) -> datetime:
        if v is None:
            return None
        for fmt in ("%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M:%S"):
            try:
                return datetime.strptime(v, fmt)
            except ValueError:
                pass

    @field_validator("start_station_id", "end_station_id", mode="before")
    @classmethod
    def parse_station_id(cls, v: str | None) -> int | str | None:
        if v is None:
            return None
        return {
            "200217old2": -1,
            "300006-1": -2,
            "Tabletop1": -3,
            "001057_old": -4,
        }.get(v, v)

    @field_validator("end_station_id", mode="after")
    @classmethod
    def zero_to_none_station_id(cls, v: str | None) -> datetime:
        if v == 0:
            return None
        return v

    def to_stations(self) -> list[Station]:
        start = Station(
            station_id=self.start_station_id,
            station_name=self.start_station_name,
        )
        if self.end_station_id:
            end = Station(
                station_id=self.end_station_id,
                station_name=self.end_station_name,
            )
            return [start, end]
        return [start]
