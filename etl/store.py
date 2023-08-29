from abc import ABC, abstractmethod
import psycopg2

from etl.models import Ride, Station


class Store(ABC):
    @abstractmethod
    def persist_ride_data(self, data: list[Ride]) -> int:
        raise NotImplementedError

    @abstractmethod
    def persist_station_data(self, data: list[Station]) -> int:
        raise NotImplementedError

    @abstractmethod
    def persist_file_hash(self, filename: str, filehash: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def commit(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def rollback(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_file_hashes(self) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def ride_ids(self) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def station_ids(self) -> None:
        raise NotImplementedError


class PGStore(Store):
    def __init__(self, **kwargs):
        self.conn = psycopg2.connect(**kwargs)

    def persist_ride_data(self, data: list[Ride]) -> int:
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO rides (rental_id, duration, bike_id, end_station_id, start_time, start_station_id) VALUES 
                (%(rental_id)s, %(duration)s, %(bike_id)s, %(end_station_id)s, %(start_time)s, %(start_station_id)s)
                ON CONFLICT (rental_id) DO NOTHING
            """,
                [
                    ride.model_dump(
                        include=["rental_id", "duration", "bike_id", "end_station_id", "start_time", "start_station_id"]
                    )
                    for ride in data
                ],
            )
            return cur.rowcount

    def persist_station_data(self, data: list[Station]) -> int:
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                    INSERT INTO stations (station_id, station_name, lat, lng) VALUES
                    (%(station_id)s, %(station_name)s, %(lat)s, %(lng)s) ON CONFLICT (station_id) DO NOTHING
            """,
                [station.model_dump() for station in data],
            )
            return cur.rowcount

    def persist_file_hash(self, filename: str, filehash: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO processed_ride_files (file_name, file_hash) VALUES (%s, %s) ON CONFLICT (file_name) DO NOTHING",
                (filename, filehash),
            )

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def get_file_hashes(self) -> dict[str, str]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT file_name, file_hash FROM processed_ride_files")
            return {filename: filehash for filename, filehash in cur.fetchall()}

    @property
    def station_ids(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT station_id FROM stations")
            return {station_id for station_id, in cur.fetchall()}

    @property
    def ride_ids(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT rental_id FROM rides")
            return {rental_id for rental_id, in cur.fetchall()}
