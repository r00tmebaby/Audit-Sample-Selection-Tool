"""Shared pytest fixtures for sampling tool tests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from worker.src.cleaner import clean_data
from worker.src.models import (
    CleanedTransaction,
    DataQualityReport,
    SampleStatistics,
    SamplingParameters,
)


@pytest.fixture(scope="session")
def sample_csv(tmp_path_factory) -> Path:
    """Create a reusable CSV with mixed debit/credit and edge cases."""
    p = tmp_path_factory.mktemp("data") / "population.csv"
    rows = [
        "transaction_id,amount,effective_date,document_type,description",
        "T1,100,01/01/2024,INV,Positive",
        "T2,-250,01/02/2024,CM,Negative",
        "T3,0,01/03/2024,INV,Zero Amount",
        "T4,999999,01/04/2024,INV,Very Large",
        "T5,bad,01/05/2024,INV,Invalid Amount",
        "T6,50,13/31/2024,INV,Bad Date",
        "T7,75,01/06/2024,INV,Normal",
        "T8,80,01/07/2024,INV,Normal",
        "T9,90,01/08/2024,INV,Normal",
        "T10,110,01/09/2024,INV,Normal",
    ]
    p.write_text("\n".join(rows))
    return p


@pytest.fixture()
def sampling_params() -> SamplingParameters:
    """Default parameters for tests."""
    return SamplingParameters(
        tolerable_misstatement=1000.0,
        expected_misstatement=100.0,
        assurance_factor=3.0,
        random_seed=123,
    )


@pytest.fixture()
def cleaned_transactions(sample_csv: Path) -> list[CleanedTransaction]:
    """Return cleaned transactions from sample CSV (excluding invalid)."""
    cleaned, _ = clean_data(sample_csv)
    return cleaned


@pytest.fixture()
def data_quality(sample_csv: Path) -> DataQualityReport:
    """Data quality report from sample CSV."""
    _, report = clean_data(sample_csv)
    return report


@pytest.fixture()
def sample_stats() -> SampleStatistics:
    """Basic sample statistics object for reporter tests."""
    return SampleStatistics(
        population_size=10,
        population_balance_abs=1000.0,
        sampling_interval=100.0,
        high_value_count=2,
        random_sample_count=3,
        coverage_abs=500.0,
        coverage_percent=50.0,
    )


@pytest.fixture()
def minimal_sample() -> list[CleanedTransaction]:
    """Small in-memory sample list."""
    return [
        CleanedTransaction(
            transaction_id="A",
            amount_signed=100.0,
            amount_abs=100.0,
            balance_category="credit",
            source_row_index=0,
            selection_type="High Value",
        ),
        CleanedTransaction(
            transaction_id="B",
            amount_signed=-50.0,
            amount_abs=50.0,
            balance_category="debit",
            source_row_index=1,
            selection_type="Random",
        ),
    ]


@pytest.fixture()
def timestamp() -> datetime:
    return datetime.now(timezone.utc)


@pytest.fixture()
def run_id() -> str:
    return "test-run-id"


@pytest.fixture()
def clean_output(tmp_path: Path) -> Path:
    out_dir = Path("output")
    if out_dir.exists():
        for child in out_dir.iterdir():
            if child.is_file():
                child.unlink()
            else:
                import shutil

                shutil.rmtree(child)
    return out_dir
