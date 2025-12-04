"""Data cleaning logic for transactions."""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from .logging_setup import get_logger
from .models import CleanedTransaction, DataQualityReport, EventCode

log = get_logger("cleaner")

DATE_FORMATS = [
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M",
    "%d/%m/%Y",
    "%m/%d/%Y",
]

COLUMN_ALIASES = {
    "transactionid": "transaction_id",
    "transaction_id": "transaction_id",
    "trx_id": "transaction_id",
    "amount": "amount",
    "value": "amount",
    "amount_value": "amount",
    "amountvalue": "amount",
    "effective_date": "effective_date",
    "effectivedate": "effective_date",
    "date": "effective_date",
    "document_type": "document_type",
    "documenttype": "document_type",
    "doctype": "document_type",
    "description": "description",
}


def load_raw_data(file_path: Path) -> list[dict[str, str]]:
    """Load raw CSV data into a list of dictionaries.

    Args:
        file_path (Path): Absolute or relative path to the population CSV file.

    Returns:
        list[dict[str, str]]: Raw CSV rows keyed by column header.

    Raises:
        FileNotFoundError: If the provided file path does not exist.
        csv.Error: If the CSV reader encounters malformed input.
    """
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    log.info(EventCode.RAW_LOADED.value, rows=len(rows), path=str(file_path))
    return rows


def clean_data(
    input_path: Path,
) -> tuple[list[CleanedTransaction], DataQualityReport]:
    """Clean population data and produce a quality report.

    Args:
        input_path (Path): Path to the population CSV file.

    Returns:
        tuple[list[CleanedTransaction], DataQualityReport]: Cleaned transactions and associated quality metrics.
    """
    raw_rows = load_raw_data(input_path)
    metrics = _initialize_metrics()
    cleaned = _process_rows(raw_rows, metrics)
    duplicate_count = _count_duplicates(cleaned)
    report = _build_quality_report(
        len(raw_rows),
        len(cleaned),
        metrics,
        duplicate_count,
    )

    log.info(
        EventCode.CLEANING_DONE.value,
        raw_rows=len(raw_rows),
        cleaned_rows=len(cleaned),
        duplicates=duplicate_count,
    )

    return cleaned, report


def _initialize_metrics() -> dict[str, int]:
    """Initialize quality metrics dictionary.

    Returns:
        dict[str, int]: Zero-initialized counters for every quality metric.
    """
    return {
        "missing_amount": 0,
        "invalid_amount": 0,
        "invalid_dates": 0,
        "missing_txn_id": 0,
        "missing_date": 0,
        "missing_doc_type": 0,
        "missing_desc": 0,
    }


def _process_rows(
    raw_rows: list[dict[str, str]], metrics: dict[str, int]
) -> list[CleanedTransaction]:
    """Process raw rows into cleaned transactions.

    Args:
        raw_rows (list[dict[str, str]]): Raw CSV dictionaries.
        metrics (dict[str, int]): Mutable metrics accumulator.

    Returns:
        list[CleanedTransaction]: Validated transactions ready for sampling.
    """
    cleaned: list[CleanedTransaction] = []

    for idx, raw_row in enumerate(raw_rows):
        transaction = _process_single_row(idx, raw_row, metrics)
        if transaction:
            cleaned.append(transaction)

    return cleaned


def _process_single_row(
    idx: int,
    raw_row: dict[str, str],
    metrics: dict[str, int],
) -> CleanedTransaction | None:
    """Process a single raw row into a cleaned transaction.

    Args:
        idx (int): Row index within the population file.
        raw_row (dict[str, str]): Raw CSV row dictionary.
        metrics (dict[str, int]): Mutable metrics accumulator.

    Returns:
        CleanedTransaction | None: Cleaned transaction when valid, otherwise ``None``.
    """
    normalized = _normalize_row(raw_row)
    parsed_data = _parse_row_fields(normalized)
    _update_metrics(parsed_data, metrics)

    if parsed_data["amount_result"]["value"] is None:
        return None

    return _create_transaction(idx, parsed_data)


def _parse_row_fields(normalized: dict[str, str]) -> dict[str, Any]:
    """Parse and validate each field from a normalized row.

    Args:
        normalized (dict[str, str]): Row dictionary keyed by canonical column names.

    Returns:
        dict[str, Any]: Parsed values along with validation metadata.
    """
    return {
        "txn_id": _clean_string(normalized.get("transaction_id")),
        "amount_result": _parse_amount(normalized.get("amount", "")),
        "date_result": _parse_date(normalized.get("effective_date", "")),
        "doc_type": _clean_string(normalized.get("document_type")),
        "desc": _clean_string(normalized.get("description")),
    }


def _update_metrics(
    parsed_data: dict[str, Any], metrics: dict[str, int]
) -> None:
    """Update quality metrics based on parsed data.

    Args:
        parsed_data (dict[str, Any]): Validated field values and statuses.
        metrics (dict[str, int]): Mutable metrics accumulator.
    """
    if parsed_data["txn_id"] is None:
        metrics["missing_txn_id"] += 1
    if parsed_data["amount_result"]["status"] == "missing":
        metrics["missing_amount"] += 1
    elif parsed_data["amount_result"]["status"] == "invalid":
        metrics["invalid_amount"] += 1
    if not parsed_data["date_result"]["valid"]:
        metrics["invalid_dates"] += 1
    if parsed_data["date_result"]["value"] is None:
        metrics["missing_date"] += 1
    if parsed_data["doc_type"] is None:
        metrics["missing_doc_type"] += 1
    if parsed_data["desc"] is None:
        metrics["missing_desc"] += 1


def _create_transaction(
    idx: int, parsed_data: dict[str, Any]
) -> CleanedTransaction | None:
    """Create a ``CleanedTransaction`` from parsed data.

    Args:
        idx (int): Row index within the CSV file.
        parsed_data (dict[str, Any]): Parsed values for the row.

    Returns:
        CleanedTransaction | None: Transaction when valid, otherwise ``None`` if schema validation fails.
    """
    amount = parsed_data["amount_result"]["value"]
    balance_cat = _derive_balance(amount)

    try:
        return CleanedTransaction(
            transaction_id=parsed_data["txn_id"],
            amount_signed=amount,
            amount_abs=abs(amount),
            effective_date=parsed_data["date_result"]["value"],
            document_type=parsed_data["doc_type"],
            description=parsed_data["desc"],
            balance_category=balance_cat,
            source_row_index=idx,
        )
    except Exception as e:
        log.warning("VALIDATION_FAILED", row=idx, error=str(e))
        return None


def _count_duplicates(
    cleaned: list[CleanedTransaction],
) -> int:
    """Count duplicate transaction IDs.

    Args:
        cleaned (list[CleanedTransaction]): Cleaned transactions.

    Returns:
        int: Duplicate transaction identifier count.
    """
    duplicate_count = 0
    seen_ids: set[str] = set()
    for txn in cleaned:
        if txn.transaction_id and txn.transaction_id in seen_ids:
            duplicate_count += 1
        if txn.transaction_id:
            seen_ids.add(txn.transaction_id)
    return duplicate_count


def _normalize_row(row: dict[str, str]) -> dict[str, str]:
    """Normalize column names using configured aliases.

    Args:
        row (dict[str, str]): Raw CSV row with arbitrary headers.

    Returns:
        dict[str, str]: Row keyed by canonical column names.
    """
    normalized = {}
    for key, value in row.items():
        canonical = _canonical_name(key)
        mapped_name = COLUMN_ALIASES.get(canonical, canonical)
        normalized[mapped_name] = value
    return normalized


def _canonical_name(value: str) -> str:
    """Convert arbitrary header text to snake-like form.

    Args:
        value (str): Raw column header string.

    Returns:
        str: Normalized header string using snake-like characters.
    """
    key = value.strip().lower()
    return "".join("_" if not ch.isalnum() else ch for ch in key)


def _clean_string(value: str | None) -> str | None:
    """Trim whitespace and normalize sentinel null strings.

    Args:
        value (str | None): Raw string value.

    Returns:
        str | None: Cleaned string or ``None`` when empty/sentinel.
    """
    if value is None:
        return None
    cleaned = value.strip()
    if cleaned == "" or cleaned.lower() == "none":
        return None
    return cleaned


def _parse_amount(value: str) -> dict[str, Any]:
    """Parse amount text, returning value and status.

    Args:
        value (str): String representation of the transaction amount.

    Returns:
        dict[str, Any]: Parsed amount result including status flag.
    """
    cleaned = value.replace(",", "").strip()

    if not cleaned:
        return {"value": None, "status": "missing"}

    try:
        amount = float(cleaned)
        return {"value": amount, "status": "valid"}
    except ValueError:
        return {"value": None, "status": "invalid"}


def _parse_date(value: str) -> dict[str, Any]:
    """Parse date using ordered formats.

    Args:
        value (str): Source date string.

    Returns:
        dict[str, Any]: Parsed datetime and validity flag.
    """
    if not value or not value.strip():
        return {"value": None, "valid": False}

    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(value.strip(), fmt)
            return {"value": dt, "valid": True}
        except ValueError:
            continue

    return {"value": None, "valid": False}


def _derive_balance(
    amount: float,
) -> Literal["debit", "credit", "zero"] | None:
    """Return debit/credit/zero classification per signed amount.

    Args:
        amount (float): Signed transaction amount.

    Returns:
        Literal["debit", "credit", "zero"] | None: Balance classification when determinable.
    """
    if amount > 0:
        return "credit"
    elif amount < 0:
        return "debit"
    elif amount == 0:
        return "zero"
    return None


def _build_quality_report(
    total_raw: int,
    total_cleaned: int,
    metrics: dict[str, int],
    duplicate_count: int,
    zero_filtered: int = 0,
    balance_filtered: int = 0,
) -> DataQualityReport:
    """Assemble the ``DataQualityReport`` value object.

    Args:
        total_raw (int): Total raw rows count.
        total_cleaned (int): Total cleaned rows count.
        metrics (dict[str, int]): Quality metrics captured during cleaning.
        duplicate_count (int): Count of duplicate transaction IDs.
        balance_filtered (int, optional): Rows excluded due to balance filtering. Defaults to 0.
        zero_filtered (int, optional): Rows excluded due to zero amounts. Defaults to 0

    Returns:
        DataQualityReport: Immutable report describing population quality.
    """
    excluded = metrics["missing_amount"] + metrics["invalid_amount"]

    ratio = excluded / total_raw if total_raw > 0 else 0
    notes = ""
    if ratio > 0.2:
        notes = "Warning: more than 20% of rows have invalid amounts."

    report = DataQualityReport(
        total_rows_raw=total_raw,
        total_rows_cleaned=total_cleaned,
        missing_transaction_id=metrics["missing_txn_id"],
        missing_amount=metrics["missing_amount"],
        missing_effective_date=metrics["missing_date"],
        missing_document_type=metrics["missing_doc_type"],
        missing_description=metrics["missing_desc"],
        invalid_amount_format=metrics["invalid_amount"],
        invalid_date_format=metrics["invalid_dates"],
        duplicate_transaction_ids=duplicate_count,
        excluded_due_to_amount=excluded,
        excluded_due_to_balance=balance_filtered,
        excluded_zero_amounts=zero_filtered,
        notes=notes,
    )

    log.info(EventCode.QUALITY_REPORT.value, report=report.model_dump())
    return report
