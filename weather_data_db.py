import argparse
import logging
import sqlite3
from pathlib import Path

import pandas as pd


STATIONS_DDL = """
               CREATE TABLE IF NOT EXISTS stations (
                                                       stationid      INTEGER PRIMARY KEY,
                                                       stationname    TEXT,
                                                       lat            REAL,
                                                       lon            REAL,
                                                       regio          TEXT
               ); \
               """

MEASUREMENTS_DDL = """
                   CREATE TABLE IF NOT EXISTS measurements (
                                                               measurementid      TEXT PRIMARY KEY,
                                                               timestamp          TEXT,            
                                                               temperature        REAL,
                                                               groundtemperature  REAL,
                                                               feeltemperature    REAL,
                                                               windgusts          REAL,
                                                               windspeedBft       INTEGER,
                                                               humidity           REAL,
                                                               precipitation      REAL,
                                                               sunpower           REAL,
                                                               stationid          INTEGER NOT NULL,
                                                               FOREIGN KEY(stationid) REFERENCES stations(stationid)
                       ); \
                   """

# Extra performance helpers
INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_measurements_stationid   ON measurements(stationid);",
    "CREATE INDEX IF NOT EXISTS idx_measurements_timestamp   ON measurements(timestamp DESC);",
]

class WeatherDatabaseBuilder:
    """Build and populate the SQLite database"""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys = ON;") # Enforce FK constraints

    def create_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(STATIONS_DDL)
        cur.executescript(MEASUREMENTS_DDL)
        for sql in INDEXES:
            cur.execute(sql)
        self.conn.commit()
        logging.info("Schema ensured (tables + indexes)")


    def insert_stations(self, df: pd.DataFrame) -> None:
        sql = (
            "INSERT OR IGNORE INTO stations (stationid, stationname, lat, lon, regio) "
            "VALUES (?, ?, ?, ?, ?);"
        )
        rows = df[["stationid", "stationname", "lat", "lon", "regio"]].itertuples(index=False)
        self.conn.executemany(sql, rows)
        self.conn.commit()
        logging.info("Inserted/ignored %d station rows", len(df))

    def insert_measurements(self, df: pd.DataFrame) -> None:
        # Normalize the timestamp to UTC to avoid timezone issues
        df = df.copy()
        if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        cols = [
            "measurementid",
            "timestamp",
            "temperature",
            "groundtemperature",
            "feeltemperature",
            "windgusts",
            "windspeedBft",
            "humidity",
            "precipitation",
            "sunpower",
            "stationid",
        ]
        sql = (
                "INSERT OR REPLACE INTO measurements (" + ",".join(cols) + ") "
                                                                           "VALUES (" + ",".join(["?"] * len(cols)) + ");"
        )
        rows = df[cols].itertuples(index=False)
        self.conn.executemany(sql, rows)
        self.conn.commit()
        logging.info("Inserted %d measurement rows", len(df))


    def populate(self, stations_df: pd.DataFrame, measurements_df: pd.DataFrame) -> None:
        self.create_schema()
        self.insert_stations(stations_df)
        self.insert_measurements(measurements_df)
        logging.info("Database ready → %s", self.db_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build weather.sqlite from CSVs")
    parser.add_argument("--stations-csv", default="data/stations.csv")
    parser.add_argument("--measurements-csv", default="data/measurements.csv")
    parser.add_argument("--db", default="data/weather.sqlite", help="SQLite destination file")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Load the CSVs
    logging.info("Reading CSVs...")
    stations_df = pd.read_csv(args.stations_csv)
    meas_df = pd.read_csv(args.measurements_csv, parse_dates=["timestamp"])

    # Build DB
    builder = WeatherDatabaseBuilder(Path(args.db))
    builder.populate(stations_df, meas_df)


if __name__ == "__main__":
    main()
