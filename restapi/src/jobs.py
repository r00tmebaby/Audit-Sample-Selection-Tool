"""Background job orchestration for the REST API."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

from fastapi import UploadFile

from .schemas import JobLogEntry, JobStatus, SamplingParams
from .storage import JobStorage

logger = logging.getLogger("restapi.jobs")


def _build_cli_args(
    job_id: str, detail_params: SamplingParams, storage: JobStorage
) -> list[str]:
    input_csv = storage.input_path(job_id)
    output_dir = storage.job_dir(job_id)
    args = [
        sys.executable,
        "-m",
        "src.main",
        "--input",
        str(input_csv),
        "--output-dir",
        str(output_dir),
        "--tolerable",
        str(detail_params.tolerable_misstatement),
        "--expected",
        str(detail_params.expected_misstatement),
        "--assurance",
        str(detail_params.assurance_factor),
        "--balance-type",
        detail_params.balance_type,
        "--seed",
        str(detail_params.random_seed),
        "--run-id",
        job_id,  # Pass job_id as run_id for traceability
    ]
    if detail_params.high_value_override:
        args.extend(["--high-value", str(detail_params.high_value_override)])
    if detail_params.include_zeros:
        args.append("--include-zeros")
    if detail_params.fast:
        args.append("--fast")
    if detail_params.progress:
        args.append("--progress")
    return args


class JobManager:
    """In-memory queue + worker loop to run CLI jobs."""

    def __init__(self, storage: JobStorage | None = None) -> None:
        self.storage = storage or JobStorage()
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker())

    async def stop(self) -> None:
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:  # pragma: no cover - shutdown path
                pass

    async def enqueue_job(
        self,
        file: UploadFile,
        params: SamplingParams,
    ) -> str:
        job_id = uuid.uuid4().hex
        input_path = self.storage.input_path(job_id)
        with input_path.open("wb") as handle:
            shutil.copyfileobj(file.file, handle)
        logger.info("Queued job %s (%s)", job_id, file.filename)
        self.storage.create_job_record(
            job_id, file.filename or "input.csv", params
        )
        await self.queue.put(job_id)
        return job_id

    async def _worker(self) -> None:
        """Background worker loop to process jobs from the queue."""
        while True:
            job_id = await self.queue.get()
            logger.info("Starting job %s", job_id)
            try:
                # Run the job in a background thread so we don't block the event loop
                await asyncio.to_thread(self._run_job_sync, job_id)
                logger.info("Finished job %s", job_id)
            except Exception as exc:  # pragma: no cover - guard rail
                import traceback

                error_detail = (
                    f"{type(exc).__name__}: {exc}"
                    if str(exc)
                    else type(exc).__name__
                )
                full_traceback = traceback.format_exc()
                self.storage.append_logs(
                    job_id,
                    [
                        JobLogEntry(
                            level="error",
                            message=f"Worker crashed: {error_detail}",
                        ),
                        JobLogEntry(
                            level="error",
                            message=full_traceback,
                        ),
                    ],
                )
                self.storage.update_job(
                    job_id, status=JobStatus.FAILED, error_message=error_detail
                )
            finally:
                self.queue.task_done()

    def _run_job_sync(self, job_id: str) -> None:
        """Synchronous job runner using subprocess.run (works on Windows).

        :param job_id: Identifier of the job to run
        :return: None
        """

        detail = self.storage.load_job(job_id)
        self.storage.update_job(job_id, status=JobStatus.PROCESSING)
        cmd = _build_cli_args(job_id, detail.params, self.storage)
        logger.info("Launching worker for job %s", job_id)

        proc = subprocess.run(
            cmd,
            cwd=os.path.join(Path.cwd(), "worker"),  # Run in worker context
            capture_output=True,
            text=True,
        )
        logger.info(
            "Worker exited for job %s with code %s", job_id, proc.returncode
        )

        # Capture stdout
        stdout_lines = (proc.stdout or "").splitlines()
        if stdout_lines:
            stdout_entries = [
                JobLogEntry(message=line, level="info")
                for line in stdout_lines
                if line.strip()
            ]
            if stdout_entries:
                self.storage.append_logs(job_id, stdout_entries)

        # Capture stderr
        stderr_lines = (proc.stderr or "").splitlines()
        if stderr_lines:
            stderr_entries = [
                JobLogEntry(message=line, level="error")
                for line in stderr_lines
                if line.strip()
            ]
            if stderr_entries:
                self.storage.append_logs(job_id, stderr_entries)

        if proc.returncode != 0:
            stderr_summary = (
                " | ".join(stderr_lines[-5:])
                if stderr_lines
                else "No stderr output"
            )
            logger.error(
                "Job %s failed with code %s: %s",
                job_id,
                proc.returncode,
                stderr_summary,
            )
            self.storage.update_job(
                job_id,
                status=JobStatus.FAILED,
                error_message=f"Worker exited with code {proc.returncode}. Last errors: {stderr_summary}",
            )
            return

        report_path = self.storage.report_path(job_id)
        rel_path = str(report_path)
        self.storage.update_job(
            job_id, status=JobStatus.DONE, report_path=rel_path
        )
        logger.info("Job %s succeeded: %s", job_id, rel_path)
