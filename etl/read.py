from numpy import dtype
import pandas as pd

from etl.models import Ride
import hashlib


def load_ride(ride_file: str) -> pd.DataFrame:
    try:
        if ride_file.endswith(".csv"):
            return pd.read_csv(ride_file, dtype=str, skip_blank_lines=True)
        elif ride_file.endswith(".xlsx"):
            return pd.read_excel(ride_file, dtype=str)
    except UnicodeDecodeError:
        return pd.read_csv(ride_file, dtype=str, skip_blank_lines=True, encoding="cp1252")


def process_ride_df(df: pd.DataFrame):
    if "Total duration (ms)" in df.columns:
        df["Duration"] = (df["Total duration (ms)"].astype(int) / 1000).astype(int)
    df.dropna(how="all", inplace=True)
    return df.where(df.notnull(), None)


def sha256sum(filename):
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, "rb", buffering=0) as f:
        while n := f.readinto(mv):
            h.update(mv[:n])
    return h.hexdigest()
