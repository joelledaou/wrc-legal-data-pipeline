# Pipeline image: used by the Dagster service and for one-off CLI runs.
FROM python:3.12-slim

# uv manages the virtual environment and dependency installation.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install dependencies first so this layer is cached across code changes.
COPY pyproject.toml ./
RUN uv venv && uv pip install -r pyproject.toml

ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

COPY . .

CMD ["python", "-m", "wrc_pipeline.ingest", "--help"]
