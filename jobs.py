from dagster import job, op, ScheduleDefinition, DefaultScheduleStatus, repository
from fetch_and_load import run_pipeline_once
import logging 

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

@op
def fetch_parse_store_op():
    run_pipeline_once()
    logging.info("Click this link for data, http://localhost:3001")

@job
def stock_pipeline_job():
    fetch_parse_store_op()

every_5_minutes_schedule = ScheduleDefinition(
    name="every_5_minutes_pipeline",
    job=stock_pipeline_job,
    cron_schedule="*/5 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)

@repository
def stock_repo():
    return [stock_pipeline_job, every_5_minutes_schedule]
