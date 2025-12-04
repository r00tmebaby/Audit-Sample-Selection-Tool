[![CI Windows](https://github.com/r00tmebaby/Audit-Sample-Selection-Tool/actions/workflows/ci-windows.yml/badge.svg)](https://github.com/r00tmebaby/Audit-Sample-Selection-Tool/actions/workflows/ci-windows.yml)
[![CI (Ubuntu)](https://github.com/r00tmebaby/Audit-Sample-Selection-Tool/actions/workflows/ci-ubuntu.yml/badge.svg)](https://github.com/r00tmebaby/Audit-Sample-Selection-Tool/actions/workflows/ci-ubuntu.yml)
[![CI (macOS)](https://github.com/r00tmebaby/Audit-Sample-Selection-Tool/actions/workflows/ci-macos.yml/badge.svg)](https://github.com/r00tmebaby/Audit-Sample-Selection-Tool/actions/workflows/ci-macos.yml)

# Audit Sample Selection Tool

Implementation of RSM's Random Non-Statistical Sampling
methodology with structured logging, Excel reporting, streaming
performance and automated tests.

## Compliance Snapshot
- Core Sampling Implementation (Part 1)
- Data Quality Handling (Part 2)
- Excel Output Generation (Part 3)
- Testing & Documentation (Part 4)
- Structured Logging (Part 5)

## Architecture Diagrams

Worker 
![Worker Sequence](docs/Worker%20Sequence%20Diagram.png)
![Worker Activity](docs/Worker%20Activity%20Diagram.png)

FastAPI
![REST API Sequence](docs/RestAPI%20Sequence%20Diagram.png)
![REST API Activity](docs/RestAPI%20Activity%20Diagram.png)
![REST API Swagger](docs/RestAPI%20Swagger.png)


## Prerequisites
- Python 3.11+ (tested also on 3.13) 32/64-bit
- pip / virtualenv
- (Optional) Docker

## Quick Start
```bash
# Create virtual environment
python -m venv .venv
.venv\Scripts\activate

# Install dependencies & editable package
pip install -r requirements.txt
pip install -e .
```

### Run Tests
```bash
pytest -q        
pytest -v       
```

#### Additional: Run Tests in Docker (Windows only)
```bash
# Install chocolatey if needed and act -> https://nektosact.com/installation/chocolatey.html
# Install Docker Desktop -> https://docs.docker.com/desktop/install/windows-install/
# Ensure Docker Desktop is running

act push -j lint-and-test
````
### Generate a Sample (Streaming Mode Recommended for Large Files)
```bash
python -m src.main \
  --input data/population_data.csv \
  --output-dir output \
  --tolerable 500000 \
  --expected 50000 \
  --assurance 3.0 \
  --seed 42 \
  --fast \
  --progress
# One liner:
python -m src.main --input data/population_data.csv --output-dir output --tolerable 500000 --expected 50000 --assurance 3.0 --seed 42 --fast --progress
```
Fast/Streaming mode applies the same balance-type and zero-amount filters as in-memory sampling, but it only cleans the columns needed for amount-based logic on the fly (dates, doc types, etc. stay raw).
Flags:
- `--fast` enables two-pass streaming + reservoir sampling (low memory).
- `--progress` adds tqdm progress bars for large populations.

### Outputs Generated
- `output/sample_selection_output.xlsx` (three tabs)
- `output/runs/<uuid>.json` (run summary with timings & metrics)

### Excel Workbook Tabs
1. **Population Summary** – totals, interval (formula if not overridden), seed, data quality.
2. **Sample Selected** – coverage banner, transactions (High Value / Random), formatted RSM colors.
3. **Parameters Used** – all CLI parameters, methodology, version, timestamp.

## Logging
Structured compact JSON, each line prefixed with the run UUID for easy filtering:
```
<run_id> {"event":"RUN_START",...}
```
Event codes: RUN_START, RAW_LOADED, QUALITY_REPORT, CLEANING_DONE, STREAM_PASS1_DONE,
STREAM_PASS2_DONE, SAMPLING_DONE, REPORT_WRITTEN, RUN_SUMMARY.

## CLI Parameters
```bash
python -m src.main \
  --input PATH              # Population CSV (required) \
  --output-dir DIR          # Output directory (required) \
  --tolerable FLOAT         # Tolerable misstatement \
  --expected FLOAT          # Expected misstatement \
  --assurance FLOAT         # Assurance factor \
  --balance-type TYPE       # debit|credit|both (default both) \
  --high-value FLOAT        # Override interval (optional) \
  --seed INT                # Random seed (default 42) \
  --include-zeros           # Include zero-amount rows (off by default) \
  --fast                    # Streaming sampler mode (shares filters with in-memory) \
  --progress                # Show progress bars
```
Fast mode mirrors the same debit/credit/zero filters but only cleans non-amount columns minimally, so descriptions/doc types remain as-is in streaming runs.

## Build and Run via Docker
```bash
# Build Docker image
docker build -t audit-sample-tool .

# Run container with mounted input/output directories
docker run --rm -v /path/to/input:/data/input -v /path/to/output:/data/output audit-sample-tool \
  --input /data/input/population_data.csv \
  --output-dir /data/output \
  --tolerable 500000 \
  --expected 50000 \
  --assurance 3.0 \
  --seed 42 \
  --fast \
  --progress
  
# Windows example (PowerShell/CMD)
docker run --rm -v %cd%\data:/data/input -v %cd%\output:/data/output audit-sampling-tool --input /data/input/population_data.csv --output-dir /data/output --tolerable 500000 --expected 50000 --assurance 3.0 --seed 42 --fast --progress
```

## Project Structure
```
worker/src/
  main.py           # CLI entry
  models.py         # Pydantic + enums
  cleaner.py        # Data quality & normalization
  sampler.py        # In-memory + streaming sampler
  reporter.py       # XlsxWriter Excel generation
  logging_setup.py  # UUID-prefixed structured logging

restapi
   src/
        main.py           # FastAPI app (REST orchestrator)
        jobs.py           # Background job manager using local subprocesses
        storage.py        # Filesystem-based job metadata + artifacts
        schemas.py        # Pydantic models for API contracts
   Dockerfile             # REST API container image
   Redme.md               # REST API documentation

tests/
  conftest.py            # Shared fixtures
  test_cleaner.py        # Cleaning & parsing
  test_sampler.py        # Core sampling logic
  test_streaming_sampler.py  # Streaming/reservoir
  test_reporter.py       # Workbook structure/content
  test_reporter_formula.py  # Interval formula presence
  test_run_summary.py    # JSON summary validation
```

## Design Decisions
### Why XlsxWriter (single engine)?
- Faster formatted writes for 10k+ rows vs cell-by-cell styling.
- Rich formatting (colors, number formats, borders) with low memory.
- Avoids file locking temp artifacts seen with previous approach with openpyxl.

### Why Pure Python CSV Instead of Pandas?
- No native build dependencies; works on constrained environments.
- Lower install footprint and avoids build failures.
- Adequate performance for >1M rows with streaming mode.

### Sampling Method
- High value items: abs(amount) > interval.
- Interval: (tolerable - expected) / assurance (unless overridden).
- Random selection: reservoir sampling in streaming mode; deterministic by seed.
- Coverage computed on absolute amounts.

## Run Summary JSON
To keep track of runs, a JSON summary is saved with timings, parameters,
data quality metrics, and sample statistics. 
This can be used for audit trails, further analysis or passed through RabitMQ for dashboard.

Example (`output/runs/<uuid>.json`):
```json
{
  "run_id": "a6c1ca7a-b5b2-40b2-9fc1-48e7cd2e85d8",
  "started_at_utc": "2025-12-01T23:11:26.394492Z",
  "finished_at_utc": "2025-12-01T23:11:28.885301Z",
  "duration_seconds": 2.49,
  "cleaning_seconds": 0.67,
  "sampling_seconds": 1.14,
  "reporting_seconds": 0.68,
  "parameters": {
    "tolerable_misstatement": 500000.0,
    "expected_misstatement": 50000.0,
    "assurance_factor": 3.0,
    "balance_type": "both",
    "high_value_override": null,
    "random_seed": 42,
    "exclude_zero_amounts": true
  },
  "data_quality": {
    "total_rows_raw": 50000,
    "total_rows_cleaned": 48477,
    "missing_transaction_id": 296,
    "missing_amount": 304,
    "missing_effective_date": 324,
    "missing_document_type": 324,
    "missing_description": 2500,
    "invalid_amount_format": 1219,
    "invalid_date_format": 324,
    "duplicate_transaction_ids": 2319,
    "excluded_due_to_amount": 1523,
    "excluded_due_to_balance": 0,
    "notes": ""
  },
  "sample_statistics": {
    "population_size": 48477,
    "population_balance_abs": 2421512899.720034,
    "sampling_interval": 150000.0,
    "high_value_count": 0,
    "random_sample_count": 16144,
    "coverage_abs": 805211936.1,
    "coverage_percent": 33.252432237428735
  },
  "sample_size": 16144,
  "output_excel": "/data/output/sample_selection_output.xlsx",
  "methodology": "RSM Random Non-Statistical",
  "version": "1.0.0"
}
```

## Known Limitations
- Workbook formula references to depend on sheet naming; renaming sheets breaks formula.
- CSV must contain headers; no header inference.
- Interval formula only inserted when not overridden.

## Adding New Tests
Use fixtures in `tests/conftest.py` and create new `test_*.py` files. Example skeleton:
```python
def test_new_edge_case(cleaned_transactions, sampling_params):
    sample, stats = generate_sample(cleaned_transactions, sampling_params)
    assert stats.population_size > 0
```

## Local REST API (FastAPI) Testing
The repository also includes a FastAPI-based REST API (`restapi/`) that orchestrates sampling jobs
by running the same CLI (`python -m src.main`) as a local subprocess. This mode does **not**
launch Kubernetes Jobs; everything runs inside the API process/container.



### Start the API locally
```bash
# From the project root
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pip install -e .

# Install FastAPI/uvicorn if not already present
pip install "fastapi" "uvicorn"

# Run the API on http://127.0.0.1:8000
uvicorn restapi.main:app --reload --port 8000
```

### Submit a job via the API
With the server running, in a new terminal:
```bash
cd /d F:\Coursework

curl -X POST "http://127.0.0.1:8000/jobs" \
  -F "file=@data/population_data.csv;type=text/csv" \
  -F "tolerable_misstatement=500000" \
  -F "expected_misstatement=50000" \
  -F "assurance_factor=3.0" \
  -F "balance_type=both" \
  -F "random_seed=42" \
  -F "include_zeros=true" \
  -F "fast=true" \
  -F "progress=false"
```
If you want to override the sampling interval explicitly, add for example:
```bash
  -F "high_value_override=150000"
```
Otherwise, omit `high_value_override` to let the tool compute the interval from tolerable,
expected, and assurance values, consistent with the CLI.

### Inspect jobs and download reports
```bash
# List recent jobs
curl "http://127.0.0.1:8000/jobs"

# Get details (status, logs, report path)
curl "http://127.0.0.1:8000/jobs/<job_id>"

# Download the generated Excel report
curl -o sample_selection_output.xlsx "http://127.0.0.1:8000/jobs/<job_id>/report"
```
On disk, artifacts live under `restapi_artifacts/<job_id>/`:
- `input.csv` – the uploaded population
- `sample_selection_output.xlsx` – Excel report
- `metadata.json` – job metadata (status, params, timestamps)
- `logs.jsonl` – structured log lines from the subprocess run

### Notes
- The API spawns a background worker loop that pulls job IDs from an in-memory queue
  and runs `python -m src.main` with job-specific input/output paths.
- The API passes the `job_id` as `--run-id` to the worker, ensuring all logs, run summaries,
  and reports use the same identifier for end-to-end traceability.
- Because this is an in-process/local model, there are **no** Kubernetes `Job` resources
  created, and nothing will appear under `kubectl get jobs -n workers`.
- For containerized/Kubernetes deployment, the same behavior applies inside the
  API pod: jobs are executed as local subprocesses, writing to a shared volume.

## Troubleshooting
- Permission denied writing Excel: ensure file not open in Excel.
- Large file performance: always use `--fast` 
- Unexpected coverage: verify interval calculation (override vs formula).
- Logging missing: check logging setup and run UUID prefix.
- CSV parsing errors: ensure valid CSV format with headers.
- Seeded randomness: use same seed for reproducible samples.
- Excel formatting issues: ensure XlsxWriter installed correctly.

## Continuous Integration (GitHub Actions)
A cross-platform pipeline runs on every push/PR and performs:
- Linting (black 79, isort, pyflakes) on worker/src and restapi/src
- Test suites (worker/tests and restapi/tests)
- Worker smoke tests against the sample dataset (data/population_data.csv)

This is configured in `.github/workflows/ci.yml` and runs on Windows, Ubuntu, and macOS runners. Pip dependencies are cached via `actions/setup-python` using `requirements.txt`.

### Future Enhancements

Even though this tool should meet the core requirements, potential future improvements include:
- Web UI for job submission and monitoring.
- Integration with cloud storage (S3, Azure Blob) for input/output.
  - Support for additional sampling methodologies.  
  - Enhanced data validation and cleaning options.
- Support for additional sampling methodologies.  
- Enhanced data validation and cleaning options.
- Integration with audit management systems for seamless workflows.
- Docker Compose setup for local multi-container orchestration (API + worker).
- Kubernetes Job orchestration mode for distributed processing. For this mode,
  the API would create Kubernetes `Job` resources instead of local subprocesses,
  allowing scaling across a cluster. 
* I created helm charts to test that but did not extend further to keep the scope focused.
- Shared libraries for common functionality between CLI and REST API and models.
- Authentication and authorization for API access.
- Rate limiting and job prioritization.
- Metrics and monitoring (Prometheus, Grafana).
- CI/CD pipelines for automated testing and deployment.
