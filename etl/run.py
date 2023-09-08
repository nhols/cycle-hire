import itertools
import json
import logging
import os
from turtle import st

from pandas import DataFrame
from etl.models import Ride, Station, Station2, get_station_id_aliases, get_station_name_aliases

from etl.read import load_ride, process_ride_df, sha256sum
from etl.store import Store

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(levelname)s]%(asctime)s: %(message)s",
    handlers=[logging.FileHandler(".log3"), logging.StreamHandler()],
)

RIDE_DATA_DIR = "data/ride_data/"
STATION_DATA_JSON = "data/docking_stations.json"
STATION_DATA_JSON2 = "data/docking_stations2.json"


def read_stations1() -> list[Station]:
    return [
        Station(
            station_id=station["stationId"],
            terminal_id=station["siteId"],
            station_name=station["stationName"],
            lat=station["location"]["lat"],
            lng=station["location"]["lng"],
            n_docks=station["totalBikesAvailable"] + station["bikeDocksAvailable"],
        )
        for station in json.loads(open(STATION_DATA_JSON).read())["data"]["supply"]["stations"]
    ]


def read_stations2() -> list[Station]:
    return [
        Station(
            station_id=station["id"],
            terminal_id=next(
                prop["value"] for prop in station["additionalProperties"] if prop["key"] == "TerminalName"
            ),
            station_name=station["commonName"],
            lat=station["lat"],
            lng=station["lon"],
            install_date=next(
                prop["value"] for prop in station["additionalProperties"] if prop["key"] == "InstallDate"
            ),
            removal_date=next(
                prop["value"] for prop in station["additionalProperties"] if prop["key"] == "RemovalDate"
            ),
            n_docks=next(prop["value"] for prop in station["additionalProperties"] if prop["key"] == "NbDocks"),
        )
        for station in json.loads(open(STATION_DATA_JSON2).read())
    ]


def list_files(store: Store):
    hash_dict = store.get_file_hashes()
    hashes = set(hash_dict.values())
    for file in os.listdir(RIDE_DATA_DIR):
        file_hash = sha256sum(RIDE_DATA_DIR + file)
        if file_hash in hashes:
            logging.info(f"Skipping {file} as it has already been processed")
            continue
        if file in hash_dict and hash_dict[file] != file_hash:
            logging.warning(f"Hash for {file} has changed")
        yield file


def df_to_rides(
    df: DataFrame, stations_ids, stations_terminals, stations_names, manual_id_map
) -> tuple[list[Ride], list[dict]]:
    rides = []
    exceptions = []
    for record in df.to_dict(orient="records"):
        try:
            ride = Ride(**record)
            ride.repair_stations(stations_ids, stations_terminals, stations_names, manual_id_map)
            rides.append(ride)
        except Exception as exc:
            # logging.error(str(exc))
            # logging.error(record)
            exceptions.append(record)

    return rides, exceptions


def run_stations(store: Store):
    stations1 = read_stations1()
    stations2 = read_stations2()
    stations = list({station.station_id: station for station in itertools.chain(stations1, stations2)}.values())
    store.persist_station_data(stations)
    with store.conn.cursor() as cur:
        cur.execute(open("data/manual_stations.sql").read())
    store.commit()


def run(store: Store):
    run_stations(store)
    stations_ids = store.station_ids
    stations_terminals = store.station_terminal_id_map
    stations_names = store.station_name_id_map
    manual_id_map = {
        "137": "259",
        "300006-1": "852",
        "639": "852",
    }
    for file in list_files(store):
        if file == "325JourneyDataExtract06Jul2022-12Jul2022.csv":
            continue
        filehash = sha256sum(RIDE_DATA_DIR + file)
        logging.info(f"Processing {file}")
        df = load_ride(RIDE_DATA_DIR + file)
        df = process_ride_df(df)
        rides, exceptions = df_to_rides(df, stations_ids, stations_terminals, stations_names, manual_id_map)
        try:
            n_rides = store.persist_ride_data(rides, file)
            if exceptions:
                logging.warning(f"{len(exceptions)} exceptions occurred for file {file}")
                store.persist_exceptions(exceptions, file)
            store.persist_file_hash(file, filehash)
        except Exception as exc:
            logging.error(str(exc))
            store.rollback()
        else:
            store.commit()
            logging.info(f"Successfully processed {file}")
            logging.info(f"{n_rides} new rides added")


if __name__ == "__main__":
    from etl.store import PGStore

    store = PGStore(dbname="cyclehire")
    run(store)
    store.conn.close()
    logging.info("Finished processing all files")
