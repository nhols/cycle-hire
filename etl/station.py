from collections import defaultdict
import psycopg2
import json

from etl.models import Station


conn = psycopg2.connect(dbname="cyclehire")

stations_ride = {}
with conn.cursor() as cur:
    cur.execute("SELECT station_id, station_name FROM stations")
    for id_, name in cur.fetchall():
        stations_ride[name] = id_

stations = {}
for station in json.loads(open("data/docking_stations.json").read())["data"]["supply"]["stations"]:
    stations[station["stationName"]] = Station_(
        station_id=station["stationId"],
        station_name=station["stationName"],
        lat=station["location"]["lat"],
        lng=station["location"]["lng"],
        alt_ids=[],
    )


for name, id_ in stations_ride.items():
    if name in stations:
        station = stations[name]
        if id_ != station.station_id:
            station.alt_ids.append(id_)
