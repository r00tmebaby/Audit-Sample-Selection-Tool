### REST API Orchestrator
This service wraps the CLI worker with FastAPI so uploads, status tracking, and report downloads can be automated or scaled across pods.

**What it does**
- Accepts multipart CSV uploads plus sampling parameters via `POST /jobs` and enqueues work.
- Streams logs/status transitions (`pending → processing → done/failed`) and stores them under `restapi_artifacts/<job_id>`.
- Provides `GET /jobs` for paginated listings, `GET /jobs/{id}` for detailed status/logs, and `GET /jobs/{id}/report` to download the Excel file once finished.
- Spawns the existing worker (`python -m src.main ...`) so no sampling logic was duplicated.

**Run locally**
```cmd
set PYTHONPATH=%CD%
uvicorn restapi.main:app --reload
```

**Build & run container**
```cmd
docker build -f restapi/Dockerfile -t audit-sampling-api .
docker run --rm -p 8000:8000 -v %cd%\data:/data audit-sampling-api
```
(Adjust the volume path for non-Windows shells.)

**Submit a job (PowerShell example)**
```powershell
Invoke-RestMethod -Uri http://localhost:8000/jobs -Method Post -Form @{
  file = Get-Item .\data\population_data.csv;
  tolerable_misstatement = 500000;
  expected_misstatement = 50000;
  assurance_factor = 3;
  random_seed = 42;
  include_zeros = $true;
}
```
Retrieve job info with `GET /jobs/{job_id}` or list everything with `GET /jobs?limit=20&offset=0&order=desc`; once status is `done`, download `/jobs/{job_id}/report` to obtain the Excel output.
