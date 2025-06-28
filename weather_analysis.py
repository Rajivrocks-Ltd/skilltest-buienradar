import argparse
import logging
import sqlite3
from pathlib import Path

import pandas as pd

SQL_HIGHEST_TEMP = """
                   SELECT s.stationid,
                          s.stationname,
                          MAX(m.temperature) AS max_temperature
                   FROM   measurements m
                              JOIN   stations s USING (stationid); \
                   """

SQL_AVG_TEMP = "SELECT ROUND(AVG(temperature), 2) AS average_temperature FROM measurements;"

SQL_BIGGEST_GAP = """
                  WITH diffs AS (
                      SELECT m.stationid,
                             s.stationname,
                             MAX(ABS(m.feeltemperature - m.temperature)) AS max_gap
                      FROM   measurements m
                                 JOIN   stations s USING (stationid)
                      GROUP  BY m.stationid
                  )
                  SELECT stationid, stationname, max_gap
                  FROM   diffs
                  ORDER  BY max_gap DESC
                      LIMIT  1; \
                  """

SQL_NORTH_SEA = """
                SELECT stationid, stationname, lat, lon
                FROM   stations
                WHERE  LOWER(regio) LIKE '%noordzee%' OR LOWER(regio) LIKE '%north sea%'; \
                """

def run_query(conn: sqlite3.Connection, sql: str) -> pd.DataFrame:
    """Return the result of "sql" as a Pandas DataFrame for nice printing."""
    return pd.read_sql_query(sql, conn)

def main() -> None:
    parser = argparse.ArgumentParser(description="Answer Part‑2 questions via SQL")
    parser.add_argument("--db", default="data/weather.sqlite", help="Path to the SQLite file")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}. Have you run build_weather_db.py yet?")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")

        logging.info("Highest recorded temperature:")
        res = run_query(conn, SQL_HIGHEST_TEMP).to_string(index=False)
        logging.info(res)

        logging.info("Average temperature across all measurements:")
        res = run_query(conn, SQL_AVG_TEMP).to_string(index=False)
        logging.info(res)

        logging.info("Station with biggest feel‑temp gap:")
        res = run_query(conn, SQL_BIGGEST_GAP).to_string(index=False)
        logging.info(res)

        logging.info("Station located in the North Sea:")
        res = run_query(conn, SQL_NORTH_SEA).to_string(index=False)
        logging.info(res)


if __name__ == "__main__":
    main()
