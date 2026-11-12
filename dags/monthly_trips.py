"""Airflow DAG: one run per month. All logic lives in the `dpp` package; tasks are thin wrappers.

    fetch -> land -> write_and_audit -> publish -> report -> outcome
                                      (skipped if quarantined)    (fails loudly if quarantined)

TLC publishes each month about two months late, so the run for logical month M processes M-2.
Trigger manually with {"month": "2025-01"} to process a specific month.
"""

from __future__ import annotations

import pendulum
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import dag, get_current_context, task


def _target_month() -> str:
    from dpp import months

    ctx = get_current_context()
    if ctx["params"].get("month"):
        return ctx["params"]["month"]
    return months.add(ctx["logical_date"].strftime("%Y-%m"), -2)


@dag(
    dag_id="monthly_trips_wap",
    schedule="0 6 15 * *",  # 15th of each month, 06:00 UTC
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # plus the file lock inside dpp: never two publishers
    params={"month": ""},
    tags=["write-audit-publish"],
)
def monthly_trips_wap():
    @task(retries=3, retry_delay=pendulum.duration(minutes=30))
    def fetch() -> dict:
        from dpp.fetch import fetch_month
        from dpp.settings import default_settings

        month = _target_month()
        return {"month": month, "path": str(fetch_month(default_settings().raw_dir, month))}

    @task
    def land(f: dict) -> dict:
        from pathlib import Path

        from dpp.pipeline import land_batch
        from dpp.settings import default_settings

        r = land_batch(default_settings(), Path(f["path"]), f["month"])
        return {"month": f["month"], "batch_id": r.batch_id, "status": r.status}

    @task
    def write_and_audit(b: dict) -> dict:
        from dpp.pipeline import audit_batch
        from dpp.settings import default_settings

        if b["status"] == "skipped":
            return b
        r = audit_batch(default_settings(), b["batch_id"])
        return {**b, "status": r.status, "failing": [f.message for f in r.failing]}

    @task
    def publish(b: dict) -> dict:
        from dpp.pipeline import publish_batch
        from dpp.settings import default_settings

        if b["status"] != "audited":
            raise AirflowSkipException(f"batch {b['batch_id']} is {b['status']}; nothing to publish")
        r = publish_batch(default_settings(), b["batch_id"])
        return {**b, "status": r.status, "rows": r.rows_published}

    @task(trigger_rule="all_done")
    def report() -> str:
        from dpp.report import build_report
        from dpp.settings import default_settings

        return str(build_report(default_settings()))

    @task
    def outcome(b: dict, report_path: str) -> None:
        if b["status"] == "quarantined":
            # Quarantine is the pipeline working correctly, but a human must decide: make it visible.
            raise AirflowFailException(
                f"{b['month']} batch {b['batch_id']} QUARANTINED: {b['failing']}. "
                f"See {report_path}; resolve with `pipeline resolve {b['batch_id']} --discard|--publish`."
            )

    audited = write_and_audit(land(fetch()))
    published = publish(audited)
    rep = report()
    published >> rep
    outcome(audited, rep)


monthly_trips_wap()
