import itertools
import logging
import os

from pandas import DataFrame
from etl.models import Ride, Station

from etl.read import load_ride, process_ride_df, sha256sum
from etl.store import Store

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(levelname)s]%(asctime)s: %(message)s",
    handlers=[logging.FileHandler(".log2"), logging.StreamHandler()],
)

RIDE_DATA_DIR = "data/ride_data/"


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


def df_to_rides_stations(df: DataFrame) -> tuple[list[Ride], list[Station], int]:
    rides = []
    exceptions = 0
    for record in df.to_dict(orient="records"):
        try:
            ride = Ride(**record)
            rides.append(ride)
        except Exception as exc:
            logging.error(str(exc))
            logging.error(record)
            exceptions += 1

    stations = list(itertools.chain(*(ride.to_stations() for ride in rides)))
    return rides, stations, exceptions


def run(store: Store):
    # seen_rides = store.ride_ids
    # seen_stations = store.station_ids

    for file in list_files(store):
        filehash = sha256sum(RIDE_DATA_DIR + file)
        logging.info(f"Processing {file}")
        df = load_ride(RIDE_DATA_DIR + file)
        df = process_ride_df(df)
        rides, stations, exceptions = df_to_rides_stations(df)
        try:
            # rides = [ride for ride in rides if ride.rental_id not in seen_rides]
            # stations = [station for station in stations if station.station_id not in seen_stations]
            stations = list({station.station_id: station for station in stations}.values())
            n_stations = store.persist_station_data(stations)
            n_rides = store.persist_ride_data(rides)
            if exceptions:
                logging.warning(f"{exceptions} exceptions occurred for file {file}")
            else:
                store.persist_file_hash(file, filehash)
        except Exception as exc:
            logging.error(str(exc))
            store.rollback()
        else:
            store.commit()
            # seen_rides.update(ride.rental_id for ride in rides)
            # seen_stations.update(station.station_id for station in stations)
            logging.info(f"Successfully processed {file}")
            logging.info(f"{n_rides} new rides added")
            logging.info(f"{n_stations} new stations added")


if __name__ == "__main__":
    from etl.store import PGStore

    store = PGStore(dbname="cyclehire")
    run(store)
    store.conn.close()
    logging.info("Finished processing all files")
