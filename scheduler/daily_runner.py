import sys
import json
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from analytics.district_insights import build_daily_summary_report
from config.config import (
    DAILY_REPORT_PATH,
    PIPELINE_RUN_EVERY_MINUTES,
    PIPELINE_SCHEDULE_HOUR,
    RETENTION_DAYS,
)
from database.news_store import ensure_data_store_ready, load_recent_articles, upsert_pipeline_status
from processing.geo_resolver import normalize_location_name
from scheduler.pipeline import run_pipeline


def export_daily_summary_report(state=None, limit=100):

    normalized_state = None if state is None else normalize_location_name(state)
    recent_df = load_recent_articles(RETENTION_DAYS, normalized_state)
    report = build_daily_summary_report(recent_df, RETENTION_DAYS, normalized_state, limit)
    report_path = Path(DAILY_REPORT_PATH)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    return report_path


def run_daily_job():

    ensure_data_store_ready()
    result = run_pipeline()
    report_path = export_daily_summary_report()
    upsert_pipeline_status(
        service="scheduler",
        last_successful_run_at=datetime.now(timezone.utc).isoformat(),
        last_inserted_article_count=int(result.get("inserted", 0)),
        last_collected_count=int(result.get("collected", 0)),
        last_unique_count=int(result.get("unique", 0)),
        last_backfilled_count=int(result.get("backfilled", 0)),
        last_run_result=result,
    )
    print(f"Daily pipeline completed: {result}; report written to {report_path}")


def start_scheduler():

    scheduler = BlockingScheduler(timezone="UTC")

    if PIPELINE_RUN_EVERY_MINUTES > 0:
        scheduler.add_job(run_daily_job, "interval", minutes=PIPELINE_RUN_EVERY_MINUTES)
        print(f"Scheduler started. Pipeline will run every {PIPELINE_RUN_EVERY_MINUTES} minutes (UTC).")
    else:
        scheduler.add_job(run_daily_job, "cron", hour=PIPELINE_SCHEDULE_HOUR, minute=0)
        print(f"Scheduler started. Daily pipeline will run at {PIPELINE_SCHEDULE_HOUR:02d}:00 UTC")

    scheduler.start()


if __name__ == "__main__":
    run_daily_job()
    start_scheduler()