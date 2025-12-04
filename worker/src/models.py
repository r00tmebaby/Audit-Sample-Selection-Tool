"""Core data models for the audit sampling tool."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

BalanceType = Literal["debit", "credit", "both"]


class CleanedTransaction(BaseModel):
    """Canonical transaction schema after cleaning."""

    transaction_id: str | None = None
    amount_signed: float | None = None
    amount_abs: float | None = None
    effective_date: datetime | None = None
    document_type: str | None = None
    description: str | None = None
    balance_category: Literal["debit", "credit", "zero"] | None = None
    source_row_index: int
    selection_type: Literal["High Value", "Random"] | None = None

    @field_validator("amount_abs")
    @classmethod
    def validate_amount_abs(cls, value: float | None) -> float | None:
        """Ensure the absolute amount is never negative."""

        if value is not None and value < 0:
            raise ValueError("amount_abs must be non-negative")
        return value


class SamplingParameters(BaseModel):
    """Sampling configuration derived from CLI inputs."""

    tolerable_misstatement: float = Field(gt=0)
    expected_misstatement: float = Field(ge=0)
    assurance_factor: float = Field(gt=0)
    balance_type: BalanceType = "both"
    high_value_override: float | None = Field(default=None, gt=0)
    random_seed: int = Field(default=42, ge=0)
    exclude_zero_amounts: bool = True

    @model_validator(mode="after")
    def validate_relationships(self) -> "SamplingParameters":
        """Validate tolerable vs expected misstatement values."""

        if self.expected_misstatement >= self.tolerable_misstatement:
            raise ValueError(
                "expected_misstatement must be less than tolerable"
            )
        return self

    def sampling_interval(self) -> float:
        """Compute the sampling interval per methodology.

        Returns:
            float: Sampling interval based on tolerable vs expected misstatement.
        """

        if self.high_value_override is not None:
            return self.high_value_override
        span = self.tolerable_misstatement - self.expected_misstatement
        return span / self.assurance_factor


class SampleStatistics(BaseModel):
    """Summary metrics describing the final sample."""

    population_size: int
    population_balance_abs: float
    sampling_interval: float
    high_value_count: int
    random_sample_count: int
    coverage_abs: float
    coverage_percent: float
    excluded_zero_amounts: int = 0
    excluded_due_to_balance: int = 0


class DataQualityReport(BaseModel):
    """Data quality metrics tracked during cleaning."""

    total_rows_raw: int
    total_rows_cleaned: int
    missing_transaction_id: int
    missing_amount: int
    missing_effective_date: int
    missing_document_type: int
    missing_description: int
    invalid_amount_format: int
    invalid_date_format: int
    duplicate_transaction_ids: int
    excluded_due_to_amount: int
    excluded_due_to_balance: int
    excluded_zero_amounts: int = 0
    notes: str = ""


class EventCode(str, Enum):
    """Enumeration of structured logging event codes."""

    RUN_START = "RUN_START"
    RAW_LOADED = "RAW_LOADED"
    QUALITY_REPORT = "QUALITY_REPORT"
    CLEANING_DONE = "CLEANING_DONE"
    STREAM_PASS1_START = "STREAM_PASS1_START"
    STREAM_PASS1_DONE = "STREAM_PASS1_DONE"
    STREAM_PASS2_START = "STREAM_PASS2_START"
    STREAM_PASS2_DONE = "STREAM_PASS2_DONE"
    SAMPLING_DONE = "SAMPLING_DONE"
    REPORT_WRITTEN = "REPORT_WRITTEN"
    RUN_SUMMARY = "RUN_SUMMARY"


class RunSummary(BaseModel):
    """Aggregate run results and timings persisted as JSON."""

    run_id: str
    started_at_utc: datetime
    finished_at_utc: datetime
    duration_seconds: float
    cleaning_seconds: float
    sampling_seconds: float
    reporting_seconds: float
    parameters: dict
    data_quality: dict
    sample_statistics: dict
    sample_size: int
    output_excel: str
    methodology: str = "RSM Random Non-Statistical"
    version: str = "1.0.0"
