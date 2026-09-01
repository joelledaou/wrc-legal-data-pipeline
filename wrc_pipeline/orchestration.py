"""Dagster orchestration: ingestion -> transformation as dependent tasks.

The job runs the same CLI entrypoints used for manual runs, each in its own
subprocess. Scrapy's Twisted reactor cannot be restarted inside a long-lived
process, so a subprocess per run is the reliable way to orchestrate it; it
also means the orchestrated path and the manual CLI path are identical.

Run it from the Dagster UI (http://localhost:3000 -> wrc_decisions_job ->
Launchpad) or via:
    dagster job execute -m wrc_pipeline.orchestration -j wrc_decisions_job
"""

# NOTE: no `from __future__ import annotations` here — Dagster resolves the
# `config: DateRangeConfig` annotation at runtime and needs the real class.
import subprocess
import sys

from dagster import Config, Definitions, OpExecutionContext, job, op


class DateRangeConfig(Config):
    """Date range for the run; end_date is exclusive (YYYY-MM-DD)."""

    start_date: str = "2024-01-01"
    end_date: str = "2024-02-01"
    bodies: str = ""      # comma-separated subset; empty = all four bodies
    force_refetch: bool = False


def _run_module(context: OpExecutionContext, module: str, arguments: list[str]) -> None:
    command = [sys.executable, "-m", module, *arguments]
    context.log.info("running: %s", " ".join(command))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in process.stdout:
        context.log.info(line.rstrip())
    if process.wait() != 0:
        raise RuntimeError(f"{module} exited with code {process.returncode}")


@op
def ingest_decisions(context: OpExecutionContext, config: DateRangeConfig) -> dict:
    """Scrape the configured date range into the landing zone (Mongo + MinIO)."""
    arguments = ["--start-date", config.start_date, "--end-date", config.end_date]
    if config.bodies:
        arguments += ["--bodies", config.bodies]
    if config.force_refetch:
        arguments += ["--force"]
    _run_module(context, "wrc_pipeline.ingest", arguments)
    return {"start_date": config.start_date, "end_date": config.end_date}


@op
def transform_decisions(context: OpExecutionContext, date_range: dict) -> None:
    """Transform the landing zone for the range the ingestion just covered."""
    _run_module(
        context,
        "wrc_pipeline.transform",
        ["--start-date", date_range["start_date"], "--end-date", date_range["end_date"]],
    )


@job
def wrc_decisions_job():
    transform_decisions(ingest_decisions())


defs = Definitions(jobs=[wrc_decisions_job])
