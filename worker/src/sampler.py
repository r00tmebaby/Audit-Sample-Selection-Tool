"""Sampling engine implementing the random non-statistical method."""

from __future__ import annotations

import csv
import random
from pathlib import Path
from typing import Literal

from tqdm import tqdm

from cleaner import (
    _clean_string,
    _derive_balance,
    _normalize_row,
    _parse_amount,
    _parse_date,
)
from logging_setup import get_logger
from models import (
    CleanedTransaction,
    EventCode,
    SampleStatistics,
    SamplingParameters,
)

log = get_logger("sampler")


def generate_sample(
    cleaned: list[CleanedTransaction],
    params: SamplingParameters,
) -> tuple[list[CleanedTransaction], SampleStatistics]:
    """Generate an audit-ready sample per methodology.

    Args:
        cleaned (list[CleanedTransaction]): List of cleaned transactions.
        params (SamplingParameters): Sampling parameters validated via Pydantic.

    Returns:
        tuple[list[CleanedTransaction], SampleStatistics]: Sample selections and summary statistics.
    """
    filtered, zero_filtered, balance_filtered = _filter_population(
        cleaned,
        params,
    )

    if not filtered:
        msg = "Population is empty after applying balance filters."
        raise ValueError(msg)

    pop_balance = sum(t.amount_abs for t in filtered)
    log.info(
        "population_prepared",
        size=len(filtered),
        balance=pop_balance,
    )

    interval = params.sampling_interval()
    high_value = _select_high_value(filtered, interval)
    log.info("high_value_selected", count=len(high_value))

    remaining = _exclude_transactions(filtered, high_value)
    random_sample = _select_random_sample(
        remaining,
        interval,
        params.random_seed,
    )
    log.info("random_sample_selected", count=len(random_sample))

    sample = _combine_samples(high_value, random_sample)
    stats = _build_statistics(
        filtered,
        sample,
        interval,
        zero_filtered,
        balance_filtered,
    )

    log.info(
        EventCode.SAMPLING_DONE.value,
        population=stats.population_size,
        coverage=stats.coverage_percent,
    )

    return sample, stats


def generate_sample_streaming(
    input_csv: Path,
    params: SamplingParameters,
    show_progress: bool = False,
) -> tuple[list[CleanedTransaction], SampleStatistics]:
    """High-performance streaming sampler over the input CSV.

    Two passes over the file:
    - Pass 1: Clean on the fly to compute population totals and collect
      high-value selections (without retaining all rows).
    - Pass 2: Reservoir sampling over the remaining population to select
      the random items.

    Args:
        input_csv (Path): Population CSV file path.
        params (SamplingParameters): Sampling parameters validated via Pydantic.
        show_progress (bool): Whether to show tqdm progress indicators.

    Returns:
        tuple[list[CleanedTransaction], SampleStatistics]: Sampled transactions and statistics.
    """
    interval = params.sampling_interval()
    log.info("stream_pass1_start", interval=interval)

    total_abs = 0.0
    population_size = 0
    high_value: list[CleanedTransaction] = []
    excluded_zero = 0
    excluded_balance = 0

    # Pass 1: compute totals and collect high value
    with open(input_csv, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        iterator = reader
        if show_progress:
            iterator = tqdm(
                reader, desc="Pass 1: scanning population", unit="row"
            )
        for idx, raw in enumerate(iterator):
            norm = _normalize_row(raw)
            amt = _parse_amount(norm.get("amount", ""))
            if amt["value"] is None:
                continue
            signed = amt["value"]
            abs_val = abs(signed)
            balance_cat = _derive_balance(signed)
            include, reason = _apply_balance_filters(
                abs_val, balance_cat, params
            )
            if not include:
                if reason == "zero":
                    excluded_zero += 1
                else:
                    excluded_balance += 1
                continue
            population_size += 1
            total_abs += abs_val
            date = _parse_date(norm.get("effective_date", ""))["value"]
            if abs_val > interval:
                txn = CleanedTransaction(
                    transaction_id=_clean_string(norm.get("transaction_id")),
                    amount_signed=signed,
                    amount_abs=abs_val,
                    effective_date=date,
                    document_type=_clean_string(norm.get("document_type")),
                    description=_clean_string(norm.get("description")),
                    balance_category=balance_cat,
                    selection_type="High Value",
                    source_row_index=idx,
                )
                high_value.append(txn)

    if population_size == 0:
        raise ValueError("Population is empty after applying balance filters.")

    # Remaining balance excludes high value
    remaining_abs = total_abs - sum(t.amount_abs for t in high_value)
    tentative_size = remaining_abs / interval if interval > 0 else 0
    random_size = int(tentative_size + 0.9999)
    log.info(
        EventCode.STREAM_PASS1_DONE.value,
        population_size=population_size,
        total_abs=total_abs,
        high_value_count=len(high_value),
        random_target=random_size,
        zero_filtered=excluded_zero,
        balance_filtered=excluded_balance,
    )

    # Pass 2: reservoir sampling over non-high-value items
    log.info(EventCode.STREAM_PASS2_START.value)
    reservoir: list[CleanedTransaction] = []
    k = max(0, random_size)
    seen = 0

    if k > 0:
        with open(input_csv, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            rng = random.Random(params.random_seed)
            iterator = reader
            if show_progress:
                iterator = tqdm(
                    reader, desc="Pass 2: selecting random", unit="row"
                )
            for idx, raw in enumerate(iterator):
                norm = _normalize_row(raw)
                amt = _parse_amount(norm.get("amount", ""))
                if amt["value"] is None:
                    continue
                signed = amt["value"]
                abs_val = abs(signed)
                balance_cat = _derive_balance(signed)
                include, _ = _apply_balance_filters(
                    abs_val, balance_cat, params
                )
                if not include:
                    continue
                if abs_val > interval:
                    continue
                seen += 1
                if len(reservoir) < k:
                    reservoir.append(
                        CleanedTransaction(
                            transaction_id=_clean_string(
                                norm.get("transaction_id")
                            ),
                            amount_signed=signed,
                            amount_abs=abs_val,
                            effective_date=_parse_date(
                                norm.get("effective_date", "")
                            )["value"],
                            document_type=_clean_string(
                                norm.get("document_type")
                            ),
                            description=_clean_string(norm.get("description")),
                            balance_category=balance_cat,
                            selection_type="Random",
                            source_row_index=idx,
                        )
                    )
                else:
                    j = rng.randint(0, seen - 1)
                    if j < k:
                        reservoir[j] = CleanedTransaction(
                            transaction_id=_clean_string(
                                norm.get("transaction_id")
                            ),
                            amount_signed=signed,
                            amount_abs=abs_val,
                            effective_date=_parse_date(
                                norm.get("effective_date", "")
                            )["value"],
                            document_type=_clean_string(
                                norm.get("document_type")
                            ),
                            description=_clean_string(norm.get("description")),
                            balance_category=balance_cat,
                            selection_type="Random",
                            source_row_index=idx,
                        )

    sample = high_value + reservoir
    coverage_abs = sum(t.amount_abs for t in sample)
    coverage_percent = coverage_abs / total_abs * 100 if total_abs > 0 else 0.0

    stats = SampleStatistics(
        population_size=population_size,
        population_balance_abs=total_abs,
        sampling_interval=interval,
        high_value_count=len(high_value),
        random_sample_count=len(reservoir),
        coverage_abs=coverage_abs,
        coverage_percent=coverage_percent,
        excluded_zero_amounts=excluded_zero,
        excluded_due_to_balance=excluded_balance,
    )

    log.info(
        EventCode.STREAM_PASS2_DONE.value,
        random_selected=len(reservoir),
        coverage=coverage_percent,
    )
    return sample, stats


def _filter_population(
    transactions: list[CleanedTransaction],
    params: SamplingParameters,
) -> tuple[list[CleanedTransaction], int, int]:
    """Filter by balance type and remove invalid rows."""

    filtered = []
    zero_filtered = 0
    balance_filtered = 0

    for txn in transactions:
        include, reason = _apply_balance_filters(
            txn.amount_abs,
            txn.balance_category,
            params,
        )
        if include:
            filtered.append(txn)
            continue
        if reason == "zero":
            zero_filtered += 1
        else:
            balance_filtered += 1

    return filtered, zero_filtered, balance_filtered


def _apply_balance_filters(
    amount_abs: float | None,
    balance_category: Literal["debit", "credit", "zero"] | None,
    params: SamplingParameters,
) -> tuple[bool, Literal["zero", "balance"] | None]:
    """Determine if a row should remain in the sampling population."""

    abs_value = amount_abs or 0.0
    if params.exclude_zero_amounts and abs_value == 0:
        return False, "zero"

    if params.balance_type == "both":
        return True, None

    if balance_category is None or balance_category != params.balance_type:
        return False, "balance"

    return True, None


def _select_high_value(
    transactions: list[CleanedTransaction],
    interval: float,
) -> list[CleanedTransaction]:
    """Return transactions exceeding the sampling interval.

    Args:
        transactions (list[CleanedTransaction]): Candidate population items.
        interval (float): Sampling interval threshold.

    Returns:
        list[CleanedTransaction]: Transactions classified as high value.
    """
    high_value = [
        _mark_transaction(t, "High Value")
        for t in transactions
        if t.amount_abs > interval
    ]
    return high_value


def _exclude_transactions(
    population: list[CleanedTransaction],
    to_exclude: list[CleanedTransaction],
) -> list[CleanedTransaction]:
    """Remove excluded transactions from population.

    Args:
        population (list[CleanedTransaction]): Full population.
        to_exclude (list[CleanedTransaction]): Transactions already selected.

    Returns:
        list[CleanedTransaction]: Population items remaining for random selection.
    """
    exclude_indices = {t.source_row_index for t in to_exclude}
    return [t for t in population if t.source_row_index not in exclude_indices]


def _select_random_sample(
    transactions: list[CleanedTransaction],
    interval: float,
    seed: int,
) -> list[CleanedTransaction]:
    """Select random items for the remaining population.

    Args:
        transactions (list[CleanedTransaction]): Remaining population candidates.
        interval (float): Sampling interval guiding sample size.
        seed (int): Random seed for deterministic selection.

    Returns:
        list[CleanedTransaction]: Randomly selected transactions.
    """
    if not transactions:
        return []

    remaining_balance = sum(t.amount_abs for t in transactions)

    if remaining_balance == 0:
        return []

    sample_size = int((remaining_balance / interval) + 0.9999)
    sample_size = min(sample_size, len(transactions))

    if sample_size == 0:
        return []

    rng = random.Random(seed)
    sampled_txns = rng.sample(transactions, sample_size)

    return [_mark_transaction(t, "Random") for t in sampled_txns]


def _mark_transaction(
    txn: CleanedTransaction,
    selection_type: Literal["High Value", "Random"],
) -> CleanedTransaction:
    """Create a copy of transaction with selection_type marked.

    Args:
        txn (CleanedTransaction): Transaction to clone.
        selection_type (Literal["High Value", "Random"]): Selection label to embed.

    Returns:
        CleanedTransaction: Cloned transaction with updated selection type.
    """
    data = txn.model_dump()
    data["selection_type"] = selection_type
    return CleanedTransaction(**data)


def _combine_samples(
    high_value: list[CleanedTransaction],
    random_sample: list[CleanedTransaction],
) -> list[CleanedTransaction]:
    """Concatenate high-value and random selections.

    Args:
        high_value (list[CleanedTransaction]): High-value selections.
        random_sample (list[CleanedTransaction]): Random selections.

    Returns:
        list[CleanedTransaction]: Combined sample list preserving order.
    """
    return high_value + random_sample


def _build_statistics(
    population: list[CleanedTransaction],
    sample: list[CleanedTransaction],
    interval: float,
    zero_filtered: int,
    balance_filtered: int,
) -> SampleStatistics:
    """Compute coverage and count metrics for reporting."""

    pop_balance = sum(t.amount_abs for t in population)
    coverage_abs = sum(t.amount_abs for t in sample)

    coverage_percent = (
        (coverage_abs / pop_balance * 100) if pop_balance > 0 else 0.0
    )

    high_value_count = sum(
        1 for t in sample if t.selection_type == "High Value"
    )
    random_count = sum(1 for t in sample if t.selection_type == "Random")

    stats = SampleStatistics(
        population_size=len(population),
        population_balance_abs=pop_balance,
        sampling_interval=interval,
        high_value_count=high_value_count,
        random_sample_count=random_count,
        coverage_abs=coverage_abs,
        coverage_percent=coverage_percent,
        excluded_zero_amounts=zero_filtered,
        excluded_due_to_balance=balance_filtered,
    )

    return stats
