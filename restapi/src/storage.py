"""Utilities for persisting job artifacts on disk."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

from .schemas import JobDetail, JobLogEntry, JobStatus, SamplingParams

ARTIFACT_ROOT = Path("restapi_artifacts")


class JobStorage:
    """Filesystem-based job persistence helper."""

    def __init__(self, root: Path | None = None) -> None:
        self.root = root or ARTIFACT_ROOT
        self.root.mkdir(parents=True, exist_ok=True)

    def job_dir(self, job_id: str) -> Path:
        path = self.root / job_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def input_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "input.csv"

    def metadata_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "metadata.json"

    def log_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "logs.jsonl"

    def report_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "sample_selection_output.xlsx"

    def create_job_record(
        self,
        job_id: str,
        file_name: str,
        params: SamplingParams,
        status: JobStatus = JobStatus.PENDING,
    ) -> None:
        payload = {
            "job_id": job_id,
            "status": status.value,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "file_name": file_name,
            "params": params.model_dump(),
            "report_path": None,
            "error_message": None,
        }
        self._write_metadata(job_id, payload)

    def append_logs(self, job_id: str, entries: Iterable[JobLogEntry]) -> None:
        log_file = self.log_path(job_id)
        with log_file.open("a", encoding="utf-8") as handle:
            for entry in entries:
                handle.write(entry.model_dump_json())
                handle.write("\n")

    def update_job(
        self,
        job_id: str,
        *,
        status: JobStatus | None = None,
        report_path: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """
        Update job metadata fields.
        :param job_id:
        :param status:
        :param report_path:
        :param error_message:
        :return:
        """
        meta = self._read_metadata(job_id)
        if status is not None:
            meta["status"] = status.value
        meta["updated_at"] = datetime.now(timezone.utc).isoformat()
        if report_path is not None:
            meta["report_path"] = report_path
        if error_message is not None:
            meta["error_message"] = error_message
        self._write_metadata(job_id, meta)

    def load_job(self, job_id: str) -> JobDetail:
        """
        Load job details and logs.
        :param job_id:
        :return:  JobDetail object
        """
        meta = self._read_metadata(job_id)
        logs = self._read_logs(job_id)
        return self._build_detail(meta, logs)

    def list_jobs(self) -> list[JobDetail]:
        """
        List all jobs with their details.
        :return: List of JobDetail objects
        """
        items: List[JobDetail] = []
        for child in sorted(self.root.iterdir()):
            if not child.is_dir():
                continue
            metadata_file = child / "metadata.json"
            if not metadata_file.exists():
                continue
            meta = json.loads(metadata_file.read_text(encoding="utf-8"))
            logs = self._read_logs(child.name)
            items.append(self._build_detail(meta, logs))
        return items

    def _read_logs(self, job_id: str) -> list[JobLogEntry]:
        """
        Read and normalize job log entries.
        :param job_id:
        :return: job log entries
        """
        logs: list[JobLogEntry] = []
        log_file = self.log_path(job_id)
        if log_file.exists():
            for line in log_file.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                raw = JobLogEntry.model_validate_json(line)
                logs.append(self._normalize_log_message(raw))
        return logs

    @staticmethod
    def _normalize_log_message(entry: JobLogEntry) -> JobLogEntry:
        """Normalize worker log messages for clearer API responses.

        :param entry: JobLogEntry
        :return: Normalized JobLogEntry

        Our worker writes lines like:
            "<run_id> {json_payload}"
        where the JSON has keys: event, level, run_id, etc.

        Here we:
        - Parse the JSON part when present.
        - Derive the outer level from the inner JSON level.
        - Build a concise, human-readable message summarizing the event.
        - Keep non-JSON messages unchanged.
        """

        text = entry.message
        brace_idx = text.find("{")
        if brace_idx <= 0:
            # No JSON payload, return as-is
            return entry

        prefix = text[:brace_idx].strip()  # usually the run_id
        json_part = text[brace_idx:].strip()

        try:
            payload = json.loads(json_part)
        except json.JSONDecodeError:
            return entry

        inner_level = payload.get("level")
        level = (
            inner_level
            if inner_level in ("info", "warning", "error")
            else entry.level
        )

        event = payload.get("event") or "EVENT"
        msg_parts: list[str] = [event]

        if event == "RUN_START":
            params = payload.get("parameters") or {}
            tol = params.get("tolerable_misstatement")
            exp = params.get("expected_misstatement")
            ass = params.get("assurance_factor")
            msg_parts.append(
                f"tolerable={tol}, expected={exp}, assurance={ass}"
            )
        elif event == "RAW_LOADED":
            rows = payload.get("rows")
            path = payload.get("path")
            msg_parts.append(f"rows={rows}, path={path}")
        elif event == "QUALITY_REPORT":
            report = payload.get("report") or {}
            rows_raw = report.get("total_rows_raw")
            rows_clean = report.get("total_rows_cleaned")
            msg_parts.append(f"quality: raw={rows_raw}, cleaned={rows_clean}")
        elif event == "SAMPLING_DONE":
            pop = payload.get("population_size")
            cov = payload.get("coverage")
            msg_parts.append(f"population={pop}, coverage={cov}")
        elif event in ("STREAM_PASS1_DONE", "STREAM_PASS2_DONE"):
            pop = payload.get("population_size")
            cov = payload.get("coverage")
            if pop is not None:
                msg_parts.append(f"population={pop}")
            if cov is not None:
                msg_parts.append(f"coverage={cov}")
        elif event in ("REPORT_WRITTEN", "RUN_SUMMARY"):
            path = payload.get("path")
            msg_parts.append(f"path={path}")

        run_id = payload.get("run_id") or prefix
        summary = f"{run_id}: " + " | ".join(str(p) for p in msg_parts if p)

        return JobLogEntry(
            timestamp=entry.timestamp,
            level=level,
            message=summary,
        )

    def _read_metadata(self, job_id: str) -> dict:
        """
        Read job metadata from disk.
        :param job_id:
        :return: Dictionary of metadata
        """
        metadata_file = self.metadata_path(job_id)
        if not metadata_file.exists():
            raise FileNotFoundError(job_id)
        return json.loads(metadata_file.read_text(encoding="utf-8"))

    def _write_metadata(self, job_id: str, payload: dict) -> None:
        """
        Write job metadata to disk.
        :param job_id:
        :param payload: Payload dictionary
        :return: None
        """
        metadata_file = self.metadata_path(job_id)
        metadata_file.write_text(
            json.dumps(payload, indent=2), encoding="utf-8"
        )

    @staticmethod
    def _build_detail(meta: dict, logs: list[JobLogEntry]) -> JobDetail:
        """
        Build JobDetail from metadata and logs.
        :param meta:
        :param logs:
        :return: JobDetail object
        """
        return JobDetail(
            job_id=meta["job_id"],
            status=JobStatus(meta["status"]),
            created_at=datetime.fromisoformat(meta["created_at"]),
            updated_at=datetime.fromisoformat(meta["updated_at"]),
            file_name=meta["file_name"],
            params=SamplingParams.model_validate(meta["params"]),
            logs=logs,
            report_path=meta.get("report_path"),
            error_message=meta.get("error_message"),
        )
