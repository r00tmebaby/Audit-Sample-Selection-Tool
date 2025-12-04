"""Unit tests for the cleaning pipeline."""

from __future__ import annotations

from pathlib import Path

from worker.src.cleaner import (
    _clean_string,
    _derive_balance,
    _parse_amount,
    _parse_date,
    clean_data,
)


def test_parse_amount_handles_commas() -> None:
    """Comma separated strings convert to floats."""
    result = _parse_amount("1,000")
    assert result["value"] == 1000.0
    assert result["status"] == "valid"


def test_parse_amount_handles_negative() -> None:
    """Negative amounts are parsed correctly."""
    result = _parse_amount("-200")
    assert result["value"] == -200.0
    assert result["status"] == "valid"


def test_parse_amount_handles_empty() -> None:
    """Empty strings are marked as missing."""
    result = _parse_amount("")
    assert result["value"] is None
    assert result["status"] == "missing"


def test_parse_amount_handles_invalid() -> None:
    """Invalid strings are marked as invalid."""
    result = _parse_amount("not_a_number")
    assert result["value"] is None
    assert result["status"] == "invalid"


def test_parse_date_multiple_formats() -> None:
    """Supported formats are parsed correctly."""
    result1 = _parse_date("01/02/2024")
    assert result1["valid"] is True
    assert result1["value"].day == 1

    result2 = _parse_date("12/31/2024")
    assert result2["valid"] is True
    assert result2["value"].month == 12

    result3 = _parse_date("bad")
    assert result3["valid"] is False


def test_clean_string_normalizes() -> None:
    """Clean string trims and normalizes null values."""
    assert _clean_string("  test  ") == "test"
    assert _clean_string("") is None
    assert _clean_string("none") is None
    assert _clean_string("NONE") is None


def test_derive_balance_categories() -> None:
    """Balance categories are derived correctly."""
    assert _derive_balance(100.0) == "credit"
    assert _derive_balance(-100.0) == "debit"
    assert _derive_balance(0.0) == "zero"


def test_clean_data_reports_quality(tmp_path: Path) -> None:
    """Cleaning produces a DataQualityReport instance."""
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(
        "transaction_id,amount,effective_date,document_type,description\n"
        "1,100,01/01/2024,INV,Test\n"
    )
    cleaned, report = clean_data(csv_path)
    assert len(cleaned) == 1
    assert report.total_rows_raw == 1
    assert report.total_rows_cleaned == 1


def test_clean_data_handles_missing_amounts(tmp_path: Path) -> None:
    """Rows with missing amounts are excluded."""
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(
        "transaction_id,amount,effective_date,document_type,description\n"
        "1,,01/01/2024,INV,Test\n"
        "2,200,01/01/2024,INV,Test2\n"
    )
    cleaned, report = clean_data(csv_path)
    assert len(cleaned) == 1
    assert report.missing_amount == 1


def test_clean_data_handles_invalid_amounts(tmp_path: Path) -> None:
    """Rows with invalid amounts are excluded."""
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(
        "transaction_id,amount,effective_date,document_type,description\n"
        "1,bad_value,01/01/2024,INV,Test\n"
        "2,200,01/01/2024,INV,Test2\n"
    )
    cleaned, report = clean_data(csv_path)
    assert len(cleaned) == 1
    assert report.invalid_amount_format == 1
