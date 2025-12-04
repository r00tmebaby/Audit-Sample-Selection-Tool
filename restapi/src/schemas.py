"""Pydantic models shared across the REST API."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from worker.src.models import BalanceType  # reuse core balance type


class JobStatus(str, Enum):
    """Lifecycle states for submitted jobs."""

    PENDING = "pending"
    PROCESSING = "processing"
    FAILED = "failed"
    DONE = "done"


class Order(str, Enum):
    """Sort order for job listings."""

    ASC = "asc"
    DESC = "desc"


class JobLogEntry(BaseModel):
    """Structured event recorded for a job."""

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    level: Literal["info", "warning", "error"] = "info"
    message: str


class SamplingParams(BaseModel):
    """User-supplied sampling parameters mirroring CLI options."""

    tolerable_misstatement: float = Field(
        gt=0, description="Tolerable misstatement amount"
    )
    expected_misstatement: float = Field(
        ge=0, description="Expected misstatement amount"
    )
    assurance_factor: float = Field(gt=0, description="Assurance factor")
    balance_type: BalanceType = Field(default="both")
    high_value_override: float | None = Field(default=None, gt=0)
    random_seed: int = Field(default=42, ge=0)
    include_zeros: bool = Field(default=True)
    fast: bool = Field(default=True)
    progress: bool = Field(default=False)

    @model_validator(mode="after")
    def validate_relationships(self) -> "SamplingParams":
        """Ensure parameters are consistent with CLI expectations."""

        if self.expected_misstatement >= self.tolerable_misstatement:
            raise ValueError(
                "expected_misstatement must be less than tolerable_misstatement"
            )
        return self


class JobCreateResponse(BaseModel):
    """Response model for job submission."""

    job_id: str
    status: JobStatus


class JobDetail(BaseModel):
    """Detailed information about a submitted job."""

    job_id: str
    status: JobStatus
    created_at: datetime
    updated_at: datetime
    file_name: str
    params: SamplingParams
    logs: list[JobLogEntry] = Field(default_factory=list)
    report_path: str | None = None
    error_message: str | None = None


class JobListResponse(BaseModel):
    """Paginated list of jobs."""

    items: list[JobDetail]
    total: int
    limit: int
    offset: int
