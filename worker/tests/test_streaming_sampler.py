"""Tests for streaming sampler performance and correctness."""

from __future__ import annotations

from pathlib import Path

import pytest

from worker.src.models import SamplingParameters
from worker.src.sampler import generate_sample_streaming


def test_streaming_reservoir_size(sample_csv: Path) -> None:
    params = SamplingParameters(
        tolerable_misstatement=1000.0,
        expected_misstatement=100.0,
        assurance_factor=2.0,
        random_seed=11,
    )
    sample, stats = generate_sample_streaming(sample_csv, params)
    assert stats.population_size > 0
    assert stats.random_sample_count == len(
        [t for t in sample if t.selection_type == "Random"]
    )  # consistency


def test_streaming_empty_file(tmp_path: Path) -> None:
    empty = tmp_path / "empty.csv"
    empty.write_text(
        "transaction_id,amount,effective_date,document_type,description\n"
    )
    params = SamplingParameters(
        tolerable_misstatement=100.0,
        expected_misstatement=10.0,
        assurance_factor=2.0,
        random_seed=1,
    )
    with pytest.raises(ValueError):
        generate_sample_streaming(empty, params)
