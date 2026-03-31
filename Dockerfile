FROM python:3.11-slim

WORKDIR /app

# Install uv
RUN pip install uv --no-cache-dir

# Copy dependency files first for layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies (no dev extras, no editable install)
RUN uv sync --frozen --no-dev

# Copy source
COPY contracts/ ./contracts/
COPY create_violation.py ./
COPY outputs/ ./outputs/
COPY generated_contracts/ ./generated_contracts/
COPY schema_snapshots/ ./schema_snapshots/
COPY validation_reports/ ./validation_reports/
COPY violation_log/ ./violation_log/
COPY enforcer_report/ ./enforcer_report/

ENV PYTHONPATH=/app

# Default: run the full pipeline end-to-end
CMD [".venv/bin/python", "contracts/report_generator.py", "--output", "enforcer_report/report_data.json"]
