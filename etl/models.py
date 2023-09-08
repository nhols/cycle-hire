from datetime import datetime
import logging
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
    terminal_id: str
    station_name: str
    lat: float
    lng: float
    n_docks: int
    install_date: datetime | None = None
    removal_date: datetime | None = None

    @field_validator("station_id", mode="before")
    def parse_station_id(cls, v: str) -> int:
        return int(v.removeprefix("BikePoints_"))

    @field_validator("install_date", "removal_date", mode="before")
    def parse_dates(cls, v: str) -> int:
        return v or None


class Ride(BaseModel):
    rental_id: int = Field(validation_alias=AliasChoices("Rental Id", "Number"))
    start_station_id: str = Field(validation_alias=AliasChoices(*get_station_id_aliases("Start")))
    start_station_name: str = Field(validation_alias=AliasChoices(*get_station_name_aliases("Start")))
    end_station_id: str | None = Field(validation_alias=AliasChoices(*get_station_id_aliases("End")))
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
        for fmt in ("%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(v, fmt)
            except ValueError:
                pass

    @field_validator("end_station_id", mode="after")
    @classmethod
    def zero_to_none_station_id(cls, v: str | None) -> datetime:
        if str(v) == "0":
            return None
        return v

    def repair_stations(
        self,
        station_ids: set[str],
        stations_terminal: dict[str, int],
        stations_name: dict[str, int],
        manual_id_map: dict[str, str] = None,
    ) -> None:
        self.start_station_id = self.repair_station_id(
            self.start_station_id, self.start_station_name, station_ids, stations_terminal, stations_name, manual_id_map
        )
        if self.end_station_id is not None:
            self.end_station_id = self.repair_station_id(
                self.end_station_id, self.end_station_name, station_ids, stations_terminal, stations_name, manual_id_map
            )

    @staticmethod
    def repair_station_id(
        id_: int,
        name: str,
        station_ids: set[str],
        stations_terminal: dict[str, Station],
        stations_name: dict[str, Station],
        manual_id_map: dict[str, str] = None,
    ) -> int:
        if id_ in station_ids:
            return id_
        if station_id := stations_terminal.get(id_):
            return station_id
        if station_id := stations_name.get(name):
            return station_id
        if manual_id_map and (station_id := manual_id_map.get(id_)):
            return station_id
        raise ValueError(f"Could not repair station {id_} {name}")

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
