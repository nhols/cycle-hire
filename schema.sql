CREATE TABLE stations (
    id SERIAL PRIMARY KEY,
    station_id INTEGER NOT NULL UNIQUE,
    station_name TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION
);

CREATE TABLE rides (
    id SERIAL PRIMARY KEY,
    rental_id TEXT NOT NULL UNIQUE,
    start_station_id INTEGER REFERENCES stations (station_id),
    end_station_id INTEGER REFERENCES stations (station_id),
    bike_id INTEGER,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration INTEGER
);

CREATE TABLE processed_ride_files (
    id SERIAL PRIMARY KEY,
    file_name TEXT NOT NULL UNIQUE,
    file_hash TEXT NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);