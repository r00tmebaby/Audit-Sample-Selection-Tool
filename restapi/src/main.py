"""FastAPI service orchestrating the audit sampling worker."""

from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
for noisy in ("uvicorn", "uvicorn.access"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import ValidationError

from worker.src.models import BalanceType

from .jobs import JobManager
from .schemas import (
    JobCreateResponse,
    JobDetail,
    JobListResponse,
    JobStatus,
    Order,
    SamplingParams,
)
from .storage import JobStorage

app = FastAPI(
    title="Audit Sampling API",
    version="1.0.0",
    description="API for submitting and managing audit sampling jobs.",
    author="r00tmebaby",
)
api_logger = logging.getLogger("restapi.api")
storage = JobStorage()
manager = JobManager(storage)


@asynccontextmanager
async def lifespan(app: FastAPI):
    api_logger.info("Starting job manager")
    await manager.start()
    try:
        yield
    finally:
        api_logger.info("Stopping job manager")
        await manager.stop()


app.router.lifespan_context = lifespan


@app.get("/", include_in_schema=False)
async def docs_redirect():
    return RedirectResponse(url="/docs")


@app.post(
    "/jobs", response_model=JobCreateResponse, tags=["Audit Job Sampling"]
)
async def submit_job(
    file: UploadFile = File(...),
    tolerable_misstatement: float = Form(..., gt=0),
    expected_misstatement: float = Form(..., ge=0),
    assurance_factor: float = Form(..., gt=0),
    balance_type: BalanceType = Form("both"),
    high_value_override: float | None = Form(None, gt=0.0),
    random_seed: int = Form(42, ge=0),
    include_zeros: bool = Form(True),
    fast: bool = Form(True),
    progress: bool = Form(False),
) -> JobCreateResponse:
    try:
        # Construct SamplingParams so Pydantic can enforce CLI-consistent rules
        params = SamplingParams(
            tolerable_misstatement=tolerable_misstatement,
            expected_misstatement=expected_misstatement,
            assurance_factor=assurance_factor,
            balance_type=balance_type,
            high_value_override=high_value_override,
            random_seed=random_seed,
            include_zeros=include_zeros,
            fast=fast,
            progress=progress,
        )
    except ValidationError as exc:
        api_logger.warning("Invalid job parameters: %s", exc)
        # Surface a simple 422 message when parameters are not worker-compliant
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    job_id = await manager.enqueue_job(file, params)
    return JobCreateResponse(job_id=job_id, status=JobStatus.PENDING)


@app.get(
    "/jobs/{job_id}", response_model=JobDetail, tags=["Audit Job Sampling"]
)
async def get_job(job_id: str) -> JobDetail:
    """Retrieve detailed information about a specific job."""
    try:
        return storage.load_job(job_id)
    except FileNotFoundError:
        api_logger.warning("Job %s not found", job_id)
        raise HTTPException(status_code=404, detail="Job not found")


@app.get("/jobs", response_model=JobListResponse, tags=["Audit Job Sampling"])
async def list_jobs(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    order: Order = Query(Order.DESC),
) -> JobListResponse:
    """List jobs with pagination and ordering."""
    jobs = storage.list_jobs()
    jobs.sort(key=lambda job: job.created_at, reverse=(order == Order.DESC))
    total = len(jobs)
    sliced = jobs[offset : offset + limit]
    return JobListResponse(
        items=sliced, total=total, limit=limit, offset=offset
    )


@app.get("/jobs/{job_id}/report", tags=["Audit Job Sampling"])
async def download_report(job_id: str) -> FileResponse:
    """Download the Excel report for a completed job."""
    try:
        job = storage.load_job(job_id)
    except FileNotFoundError:
        api_logger.warning("Report requested for missing job %s", job_id)
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != JobStatus.DONE or not job.report_path:
        api_logger.warning("Report not ready for job %s", job_id)
        raise HTTPException(status_code=400, detail="Report not ready")

    # Resolve path - handle both relative and absolute paths
    path = Path(job.report_path)
    if not path.is_absolute():
        path = Path.cwd() / path

    if not path.exists():
        api_logger.error("Report file missing for job %s: %s", job_id, path)
        raise HTTPException(
            status_code=404, detail=f"Report file missing: {path}"
        )
    return FileResponse(
        path=path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"sample_selection_{job_id}.xlsx",
    )


@app.middleware("http")
async def log_requests(request, call_next):
    api_logger.info("HTTP %s %s started", request.method, request.url.path)
    response = await call_next(request)
    api_logger.info(
        "HTTP %s %s completed -> %s",
        request.method,
        request.url.path,
        response.status_code,
    )
    return response
