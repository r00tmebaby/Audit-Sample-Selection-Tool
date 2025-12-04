"""Report generation utilities for audit sampling results."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import xlsxwriter
from logging_setup import get_logger
from models import (
    CleanedTransaction,
    DataQualityReport,
    EventCode,
    SampleStatistics,
    SamplingParameters,
)
from tqdm import tqdm

# RSM corporate palette (from Style Appendix)
RSM_BLUE_100 = "009cde"
RSM_GREEN_100 = "3f9c35"
RSM_MID_GREY_50 = "c3c5c6"
RSM_MID_GREY_10 = "f3f3f4"
RSM_DARK_GREY_100 = "63666a"
RSM_MIDNIGHT_BLUE = "00153d"
RSM_LIGHT_BLUE = "e5f5fc"

REPORT_FILENAME = "sample_selection_output.xlsx"

log = get_logger("reporter")


def generate_reports(
    output_dir: Path,
    sample: list[CleanedTransaction],
    quality_report: DataQualityReport,
    sample_stats: SampleStatistics,
    params: SamplingParameters,
    timestamp: datetime,
    run_id: str,
    show_progress: bool = False,
) -> Path:
    """Generate Excel report per methodology requirements.

    Args:
        output_dir (Path): Directory that will receive the Excel report.
        sample (list[CleanedTransaction]): Selected sample transactions.
        quality_report (DataQualityReport): Data quality statistics.
        sample_stats (SampleStatistics): Calculated sampling metrics.
        params (SamplingParameters): Parameters used to drive sampling.
        timestamp (datetime): Timestamp applied to workbook metadata.
        run_id (str): Unique identifier for the execution run.
        show_progress (bool): Whether to display progress bars while writing.

    Returns:
        Path: Filesystem path to the generated Excel workbook.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / REPORT_FILENAME

    _write_excel_report(
        output_path,
        sample,
        quality_report,
        sample_stats,
        params,
        timestamp,
        run_id,
        show_progress=show_progress,
    )

    log.info(EventCode.REPORT_WRITTEN.value, path=str(output_path))
    return output_path


def _write_excel_report(
    output_path: Path,
    sample: list[CleanedTransaction],
    quality_report: DataQualityReport,
    sample_stats: SampleStatistics,
    params: SamplingParameters,
    timestamp: datetime,
    run_id: str,
    show_progress: bool = False,
) -> None:
    """Write complete Excel report with all sheets.

    Args:
        output_path (Path): Path for output Excel file.
        sample (list[CleanedTransaction]): Selected sample transactions.
        quality_report (DataQualityReport): Data quality metrics.
        sample_stats (SampleStatistics): Sample statistics.
        params (SamplingParameters): Sampling parameters.
        timestamp (datetime): Generation timestamp.
        run_id (str): Unique run identifier.
        show_progress (bool): Whether to show progress bars.
    """
    workbook = xlsxwriter.Workbook(str(output_path), {"constant_memory": True})
    formats = _create_workbook_formats(workbook)

    _write_population_summary_sheet(
        workbook,
        formats,
        sample_stats,
        params,
        quality_report,
        run_id,
        timestamp,
    )
    _write_sample_selected_sheet(
        workbook, formats, sample, sample_stats, show_progress
    )
    _write_parameters_used_sheet(workbook, formats, params, timestamp, run_id)

    workbook.close()


def _create_workbook_formats(workbook: xlsxwriter.Workbook) -> dict[str, Any]:
    """Create all formatting styles for the workbook.

    Args:
        workbook (xlsxwriter.Workbook): Workbook instance that needs formatting definitions.

    Returns:
        dict[str, Any]: Dictionary of named format objects for reuse across sheets.
    """
    return {
        "header_blue": workbook.add_format(
            {
                "bold": True,
                "font_color": "#FFFFFF",
                "bg_color": RSM_BLUE_100,
                "border": 1,
                "align": "center",
            }
        ),
        "header_green": workbook.add_format(
            {
                "bold": True,
                "font_color": "#FFFFFF",
                "bg_color": RSM_GREEN_100,
                "border": 1,
                "align": "center",
            }
        ),
        "label": workbook.add_format(
            {"font_color": RSM_DARK_GREY_100, "border": 1}
        ),
        "value_wrap": workbook.add_format({"text_wrap": True, "border": 1}),
        "number": workbook.add_format({"num_format": "#,##0.00", "border": 1}),
        "integer": workbook.add_format({"num_format": "#,##0", "border": 1}),
        "percent": workbook.add_format({"num_format": "0.00%", "border": 1}),
        "banner": workbook.add_format(
            {
                "bold": True,
                "font_color": "#FFFFFF",
                "bg_color": RSM_MIDNIGHT_BLUE,
                "border": 1,
                "align": "center",
            }
        ),
        "alt_row": workbook.add_format(
            {"bg_color": RSM_LIGHT_BLUE, "border": 1}
        ),
        "normal_row": workbook.add_format({"border": 1}),
    }


def _write_population_summary_sheet(
    workbook: xlsxwriter.Workbook,
    formats: dict[str, Any],
    sample_stats: SampleStatistics,
    params: SamplingParameters,
    quality_report: DataQualityReport,
    run_id: str,
    timestamp: datetime,
) -> None:
    """Write the Population Summary sheet.

    Args:
        workbook (xlsxwriter.Workbook): Workbook currently being authored.
        formats (dict[str, Any]): Dictionary of reusable formats.
        sample_stats (SampleStatistics): Sample statistics to summarize.
        params (SamplingParameters): Parameters used in the run.
        quality_report (DataQualityReport): Data quality findings.
        run_id (str): Unique run identifier to display.
        timestamp (datetime): Generation timestamp for metadata.
    """
    ws = workbook.add_worksheet("Population Summary")
    ws.set_column("A:A", 30)
    ws.set_column("B:B", 70)

    ws.write(0, 0, "Metric", formats["header_blue"])
    ws.write(0, 1, "Value", formats["header_blue"])

    quality_summary = (
        f"Missing Amounts: {quality_report.missing_amount}; "
        f"Invalid Amounts: {quality_report.invalid_amount_format}; "
        f"Invalid Dates: {quality_report.invalid_date_format}; "
        f"Zero Excluded: {quality_report.excluded_zero_amounts}; "
        f"Balance Excluded: {quality_report.excluded_due_to_balance}"
    )

    rows = [
        (
            "Total Population Value",
            sample_stats.population_balance_abs,
            "number",
        ),
        ("Number of Items", sample_stats.population_size, "integer"),
        ("High Value Threshold", sample_stats.sampling_interval, "number"),
        (
            "Sample Size Calculated",
            sample_stats.random_sample_count,
            "integer",
        ),
        ("Random Seed Used", params.random_seed, "integer"),
        ("Data Quality Issues", quality_summary, "value_wrap"),
        ("Run Identifier", run_id, "value_wrap"),
        ("Generated At (UTC)", timestamp.isoformat(), "value_wrap"),
    ]

    for r, (label, value, fmt_name) in enumerate(rows, start=1):
        ws.write(r, 0, label, formats["label"])
        if (
            label == "High Value Threshold"
            and params.high_value_override is None
        ):
            formula = (
                "=(INDIRECT(\"'Parameters Used'!B2\")"
                "-INDIRECT(\"'Parameters Used'!B3\"))"
                "/INDIRECT(\"'Parameters Used'!B4\")"
            )
            ws.write_formula(r, 1, formula, formats["number"])
        else:
            ws.write(r, 1, value, formats[fmt_name])


def _write_sample_selected_sheet(
    workbook: xlsxwriter.Workbook,
    formats: dict[str, Any],
    sample: list[CleanedTransaction],
    sample_stats: SampleStatistics,
    show_progress: bool,
) -> None:
    """Write the Sample Selected sheet.

    Args:
        workbook (xlsxwriter.Workbook): Workbook being written.
        formats (dict[str, Any]): Formatting dictionary for styles.
        sample (list[CleanedTransaction]): Sampled transactions to tabulate.
        sample_stats (SampleStatistics): Summary stats for banner sections.
        show_progress (bool): Whether to display tqdm progress bars.
    """
    ws = workbook.add_worksheet("Sample Selected")
    ws.set_column("A:A", 18)
    ws.set_column("B:C", 14)
    ws.set_column("D:D", 22)
    ws.set_column("E:E", 18)
    ws.set_column("F:F", 40)
    ws.set_column("G:I", 14)

    # Banner
    ws.write(0, 0, "Coverage %", formats["banner"])
    ws.write_number(
        0, 1, sample_stats.coverage_percent / 100.0, formats["percent"]
    )

    # Spacer
    ws.write(1, 0, "")

    # Headers
    headers = [
        "Transaction ID",
        "Amount Signed",
        "Amount Abs",
        "Effective Date",
        "Document Type",
        "Description",
        "Balance Category",
        "Selection Type",
        "Source Row Index",
    ]
    for c, h in enumerate(headers):
        ws.write(2, c, h, formats["header_blue"])

    _write_sample_rows(ws, formats, sample, show_progress)


def _write_sample_rows(
    ws: Any,
    formats: dict[str, Any],
    sample: list[CleanedTransaction],
    show_progress: bool,
) -> None:
    """Write sample transaction rows to worksheet.

    Args:
        ws (Any): Worksheet object to mutate.
        formats (dict[str, Any]): Formatting map for alternating rows.
        sample (list[CleanedTransaction]): Sample transactions to write.
        show_progress (bool): Whether to show progress bars.
    """
    iterator = (
        sample
        if not show_progress
        else tqdm(sample, desc="Writing sample rows", unit="row")
    )

    for idx, txn in enumerate(iterator, start=3):
        row_fmt = (
            formats["alt_row"] if (idx - 3) % 2 == 0 else formats["normal_row"]
        )
        ws.write(idx, 0, txn.transaction_id or "", row_fmt)
        ws.write_number(idx, 1, txn.amount_signed or 0.0, formats["number"])
        ws.write_number(idx, 2, txn.amount_abs or 0.0, formats["number"])
        ws.write(
            idx,
            3,
            txn.effective_date.isoformat() if txn.effective_date else "",
            row_fmt,
        )
        ws.write(idx, 4, txn.document_type or "", row_fmt)
        ws.write(idx, 5, txn.description or "", row_fmt)
        ws.write(idx, 6, txn.balance_category or "", row_fmt)
        ws.write(idx, 7, txn.selection_type or "", row_fmt)
        ws.write_number(idx, 8, txn.source_row_index, formats["integer"])


def _write_parameters_used_sheet(
    workbook: xlsxwriter.Workbook,
    formats: dict[str, Any],
    params: SamplingParameters,
    timestamp: datetime,
    run_id: str,
) -> None:
    """Write the Parameters Used sheet.

    Args:
        workbook (xlsxwriter.Workbook): Workbook being populated.
        formats (dict[str, Any]): Formatting dictionary.
        params (SamplingParameters): Input parameters guiding sampling.
        timestamp (datetime): Timestamp to display for traceability.
        run_id (str): Unique run identifier for reference.
    """
    ws = workbook.add_worksheet("Parameters Used")
    ws.set_column("A:A", 30)
    ws.set_column("B:B", 45)

    ws.write(0, 0, "Metric", formats["header_green"])
    ws.write(0, 1, "Value", formats["header_green"])

    param_rows = [
        ("Tolerable Misstatement", params.tolerable_misstatement, "number"),
        ("Expected Misstatement", params.expected_misstatement, "number"),
        ("Assurance Factor", params.assurance_factor, "number"),
        ("Balance Type", params.balance_type, "value_wrap"),
        (
            "High Value Override",
            params.high_value_override or "Not Specified",
            "value_wrap",
        ),
        (
            "Exclude Zero Amounts",
            str(params.exclude_zero_amounts),
            "value_wrap",
        ),
        ("Random Seed", params.random_seed, "integer"),
        ("Timestamp (UTC)", timestamp.isoformat(), "value_wrap"),
        ("Run Identifier", run_id, "value_wrap"),
        ("Methodology", "RSM Random Non-Statistical", "value_wrap"),
        ("Version", "1.0.0", "value_wrap"),
    ]

    for r, (label, value, fmt_name) in enumerate(param_rows, start=1):
        ws.write(r, 0, label, formats["label"])
        ws.write(r, 1, value, formats[fmt_name])
