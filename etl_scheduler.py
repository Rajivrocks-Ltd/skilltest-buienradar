""""
ETL Scheduler Module
Couldn't figure out how to make it run only on a specific day.
"""

import logging
import os
import signal
import sys
from pathlib import Path
from typing import Callable

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from etl import main as job_func  # local import to avoid cycles

def _get_cron_expr() -> str:
    """Return the 5‑field crontab expression from env"""
    return os.getenv("ETL_CRON", "*/1 * * * *")


def register_cron_jobs(job: Callable[..., None] | None = None):
    """
    Register the ETL job with APScheduler.
    """

    cron_expr = _get_cron_expr()
    trigger = CronTrigger.from_crontab(cron_expr, timezone="UTC")

    schedule = BlockingScheduler(timezone="UTC")

    schedule.add_job(
        func=job,
        trigger=trigger,
        id="etl_cycle",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    schedule.start()
    logging.getLogger(__name__).info("APScheduler started with cron: %s", cron_expr)

    try:
        signal.pause()
    except (KeyboardInterrupt, SystemExit):
        schedule.shutdown()
        sys.exit(0)

if __name__ == "__main__":
    LOGDIR = Path(__file__).resolve().parent / "logs"
    LOGDIR.mkdir(exist_ok=True)

    logging.basicConfig(
        filename=LOGDIR / "etl.log",
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    register_cron_jobs(job_func)
