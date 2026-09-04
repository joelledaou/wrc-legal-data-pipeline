"""Dagster job: ingestion followed by transformation.

Each op runs the matching CLI entrypoint in a subprocess. Scrapy's Twisted
reactor cannot be restarted inside a long-lived process, and this keeps the
orchestrated path identical to a manual run.

    dagster job execute -m wrc_pipeline.orchestration -j wrc_decisions_job
"""

# No `from __future__ import annotations`: Dagster needs the real config class at runtime.
import subprocess
import sys

from dagster import Config, Definitions, OpExecutionContext, job, op


class DateRangeConfig(Config):
    start_date: str = "2024-01-01"
    end_date: str = "2024-02-01"  # exclusive
    bodies: str = ""  # comma-separated subset, empty = all four
    partition_size: str = ""  # monthly | weekly | daily, empty = PARTITION_SIZE from the environment
    force_refetch: bool = False


@op
def ingest_decisions(context: OpExecutionContext, config: DateRangeConfig) -> dict:
    arguments = ["--start-date", config.start_date, "--end-date", config.end_date]
    if config.bodies:
        arguments += ["--bodies", config.bodies]
    if config.partition_size:
        arguments += ["--partition-size", config.partition_size]
    if config.force_refetch:
        arguments.append("--force-refetch")
    run_module(context, "wrc_pipeline.ingest", arguments)
    return {"start_date": config.start_date, "end_date": config.end_date, "partition_size": config.partition_size}


@op
def transform_decisions(context: OpExecutionContext, date_range: dict) -> None:
    arguments = ["--start-date", date_range["start_date"], "--end-date", date_range["end_date"]]
    if date_range["partition_size"]:
        arguments += ["--partition-size", date_range["partition_size"]]
    run_module(context, "wrc_pipeline.transform", arguments)


@job
def wrc_decisions_job():
    transform_decisions(ingest_decisions())


def run_module(context: OpExecutionContext, module: str, arguments: list[str]) -> None:
    command = [sys.executable, "-m", module, *arguments]
    context.log.info("running: %s", " ".join(command))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in process.stdout:
        context.log.info(line.rstrip())
    if process.wait() != 0:
        raise RuntimeError(f"{module} exited with code {process.returncode}")


defs = Definitions(jobs=[wrc_decisions_job])
