"""Unit tests for the sampling engine."""

from __future__ import annotations

import pytest

from worker.src.models import CleanedTransaction, SamplingParameters
from worker.src.sampler import generate_sample


@pytest.fixture()
def sample_population() -> list[CleanedTransaction]:
    """Provide a toy population for sampling tests."""
    return [
        CleanedTransaction(
            transaction_id="A",
            amount_signed=100.0,
            amount_abs=100.0,
            balance_category="credit",
            source_row_index=0,
        ),
        CleanedTransaction(
            transaction_id="B",
            amount_signed=50.0,
            amount_abs=50.0,
            balance_category="credit",
            source_row_index=1,
        ),
        CleanedTransaction(
            transaction_id="C",
            amount_signed=25.0,
            amount_abs=25.0,
            balance_category="credit",
            source_row_index=2,
        ),
        CleanedTransaction(
            transaction_id="D",
            amount_signed=10.0,
            amount_abs=10.0,
            balance_category="credit",
            source_row_index=3,
        ),
    ]


def test_high_value_selection(
    sample_population: list[CleanedTransaction],
) -> None:
    """High value items exceeding interval are always selected."""
    params = SamplingParameters(
        tolerable_misstatement=180.0,  # interval will be 90.0
        expected_misstatement=0.0,
        assurance_factor=2.0,
        random_seed=1,
    )
    sample, stats = generate_sample(sample_population, params)

    sample_ids = {t.transaction_id for t in sample}
    # Transaction A (100) > interval (90), so it's high value
    assert "A" in sample_ids
    assert stats.high_value_count == 1


def test_random_selection_reproducible(
    sample_population: list[CleanedTransaction],
) -> None:
    """Random selection is deterministic for a given seed."""
    params = SamplingParameters(
        tolerable_misstatement=200.0,
        expected_misstatement=0.0,
        assurance_factor=10.0,
        random_seed=123,
    )
    sample1, _ = generate_sample(sample_population, params)
    sample2, _ = generate_sample(sample_population, params)

    ids1 = [t.transaction_id for t in sample1]
    ids2 = [t.transaction_id for t in sample2]
    assert ids1 == ids2


def test_all_high_value(
    sample_population: list[CleanedTransaction],
) -> None:
    """If all items are high value, random sample count is 0."""
    params = SamplingParameters(
        tolerable_misstatement=9.0,  # interval will be 9.0
        expected_misstatement=0.0,
        assurance_factor=1.0,
    )
    sample, stats = generate_sample(sample_population, params)
    # All amounts (100, 50, 25, 10) > 9.0, so all are high value
    assert stats.high_value_count == 4
    assert stats.random_sample_count == 0


def test_empty_population_error() -> None:
    """Empty population raises ValueError."""
    params = SamplingParameters(
        tolerable_misstatement=100.0,
        expected_misstatement=0.0,
        assurance_factor=1.0,
    )
    with pytest.raises(ValueError):
        generate_sample([], params)


def test_balance_type_filtering() -> None:
    """Filter by debit/credit balance type."""
    population = [
        CleanedTransaction(
            transaction_id="A",
            amount_signed=100.0,
            amount_abs=100.0,
            balance_category="credit",
            source_row_index=0,
        ),
        CleanedTransaction(
            transaction_id="B",
            amount_signed=-50.0,
            amount_abs=50.0,
            balance_category="debit",
            source_row_index=1,
        ),
    ]

    params_credit = SamplingParameters(
        tolerable_misstatement=200.0,
        expected_misstatement=0.0,
        assurance_factor=2.0,
        balance_type="credit",
    )

    sample, stats = generate_sample(population, params_credit)
    assert stats.population_size == 1
    assert all(t.balance_category == "credit" for t in sample)
