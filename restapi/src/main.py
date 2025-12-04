"""FastAPI service orchestrating the audit sampling worker."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

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
storage = JobStorage()
manager = JobManager(storage)


@app.get("/", include_in_schema=False)
async def docs_redirect():
    return RedirectResponse(url="/docs")


@app.on_event("startup")
async def startup_event() -> None:
    """Initialize and start the job manager on application startup."""
    await manager.start()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """Gracefully stop the job manager on application shutdown."""
    await manager.stop()


@app.post("/jobs", response_model=JobCreateResponse, tags=["jobs"])
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
        # Surface a simple 422 message when parameters are not worker-compliant
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    job_id = await manager.enqueue_job(file, params)
    return JobCreateResponse(job_id=job_id, status=JobStatus.PENDING)


@app.get("/jobs/{job_id}", response_model=JobDetail, tags=["jobs"])
async def get_job(job_id: str) -> JobDetail:
    """Retrieve detailed information about a specific job.

    @param job_id: Identifier of the job
    @return: JobDetail object with job information
    """
    try:
        return storage.load_job(job_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Job not found")


@app.get("/jobs", response_model=JobListResponse, tags=["jobs"])
async def list_jobs(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    order: Order = Query(Order.DESC),
) -> JobListResponse:
    """List jobs with pagination and ordering.

    :param limit: Maximum number of jobs to return
    :param offset: Number of jobs to skip
    :param order: Sort order (ascending or descending by creation time)
    :return: JobListResponse containing the paginated jobs
    """
    jobs = storage.list_jobs()
    jobs.sort(key=lambda job: job.created_at, reverse=(order == Order.DESC))
    total = len(jobs)
    sliced = jobs[offset : offset + limit]
    return JobListResponse(
        items=sliced, total=total, limit=limit, offset=offset
    )


@app.get("/jobs/{job_id}/report", tags=["jobs"])
async def download_report(job_id: str) -> FileResponse:
    """Download the Excel report for a completed job.

    :param job_id: Identifier of the job
    :return: FileResponse with the report file
    """
    try:
        job = storage.load_job(job_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != JobStatus.DONE or not job.report_path:
        raise HTTPException(status_code=400, detail="Report not ready")

    # Resolve path - handle both relative and absolute paths
    path = Path(job.report_path)
    if not path.is_absolute():
        path = Path.cwd() / path

    if not path.exists():
        raise HTTPException(
            status_code=404, detail=f"Report file missing: {path}"
        )

    # Return file with proper headers for download
    return FileResponse(
        path=path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"sample_selection_{job_id}.xlsx",
    )
