from dagster import job, op, ScheduleDefinition, DefaultScheduleStatus, repository
from fetch_and_load import run_pipeline_for_symbol
import logging
import os

SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "AAPL").split(",") if s.strip()]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def make_symbol_op(symbol):
    @op(name=f"fetch_parse_store_{symbol}")
    def _op():
        run_pipeline_for_symbol(symbol)
    return _op

symbol_jobs = {}
for sym in SYMBOLS:
    symbol_op = make_symbol_op(sym)

    @job(name=f"{sym}_job")
    def _job():
        symbol_op()

    symbol_jobs[sym] = _job

schedules = [
    ScheduleDefinition(
        name=f"every_5_minutes_{sym}",
        job=job,
        cron_schedule="*/5 * * * *",
        default_status=DefaultScheduleStatus.RUNNING,
    )
    for sym, job in symbol_jobs.items()
]


@repository
def stock_repo():
    return list(symbol_jobs.values()) + schedules
