"""
Simple ETL Script that runs all functionality, this is intended to be run by a cron job or the likes.
"""

import logging, subprocess, sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parent
LOGFILE = PROJECT / "logs" / "etl.log"
LOGFILE.parent.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOGFILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

def main() -> None:
    try:
        subprocess.check_call(
            [sys.executable, PROJECT / "measurement_extractor.py"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        subprocess.check_call(
            [
                sys.executable,
                PROJECT / "weather_data_db.py",
                "--db", PROJECT / "data" / "weather.sqlite",
                ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        logging.info("ETL cycle succeeded")
    except subprocess.CalledProcessError as exc:
        logging.error("ETL cycle FAILED: %s", exc)
        sys.exit(exc.returncode)

if __name__ == "__main__":
    main()
