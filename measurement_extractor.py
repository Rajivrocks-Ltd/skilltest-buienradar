import argparse
import logging
import sys
import uuid
from pathlib import Path

import pandas as pd
import requests

API_URL: str = "https://data.buienradar.nl/2.0/feed/json"
DATA_DIR: Path = Path(__file__).with_suffix("").parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def fetch_json(url: str = API_URL) -> dict:
    """Download the raw Buienradar feed and return it as a Python dict."""
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.json()

def extract_measurements(payload: dict) -> pd.DataFrame:
    """Return a DataFrame with one row per measurement (Question 1)."""
    recs: list[dict] = []
    for m in payload["actual"]["stationmeasurements"]:
        recs.append(
            {
                "measurementid": str(uuid.uuid4()),
                "timestamp": m.get("timestamp"),
                "temperature": m.get("temperature"),
                "groundtemperature": m.get("groundtemperature"),
                "feeltemperature": m.get("feeltemperature"),
                "windgusts": m.get("windgusts"),
                "windspeedBft": m.get("windspeedBft"),
                "humidity": m.get("humidity"),
                "precipitation": m.get("precipitation"),
                "sunpower": m.get("sunpower"),
                "stationid": m.get("stationid"),
            }
        )

    df = pd.DataFrame.from_records(recs)

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    numeric_cols = [
        "temperature",
        "groundtemperature",
        "feeltemperature",
        "windgusts",
        "windspeedBft",
        "humidity",
        "precipitation",
        "sunpower",
    ]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    return df.sort_values("timestamp", ascending=False).reset_index(drop=True)


def extract_stations(payload: dict) -> pd.DataFrame:
    """Return a DataFrame with one row per weather‑station (Question 2)."""
    recs: list[dict] = []
    for s in payload["actual"]["stationmeasurements"]:
        recs.append(
            {
                "stationid": s.get("stationid"),
                "stationname": s.get("stationname"),
                "lat": s.get("lat"),
                "lon": s.get("lon"),
                "regio": s.get("regio"),
            }
        )

    df = pd.DataFrame.from_records(recs)

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df.drop_duplicates("stationid").sort_values("stationid").reset_index(drop=True)
    return df


def save_dataset(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    logging.info("Saved %d rows → %s", len(df), out_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract Buienradar measurements & station metadata -> CSV")
    parser.add_argument(
        "--measurements-out",
        default=str(DATA_DIR / "measurements.csv"),
        help="Destination for measurements.csv",
    )
    parser.add_argument(
        "--stations-out",
        default=str(DATA_DIR / "stations.csv"),
        help="Destination for stations.csv",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        logging.info("Fetching Buienradar JSON feed …")
        raw = fetch_json()

        logging.info("Extracting measurement rows...")
        df_meas = extract_measurements(raw)
        save_dataset(df_meas, Path(args.measurements_out))

        logging.info("Extracting station rows...")
        df_stat = extract_stations(raw)
        save_dataset(df_stat, Path(args.stations_out))

        logging.info("All done - %d measurements from %d unique stations",
                     len(df_meas), df_stat.shape[0])
    except Exception as exc:
        logging.error("Failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
