"""CLI entry point for the audit sampling tool."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from cleaner import clean_data
from logging_setup import configure_logging, get_logger
from models import EventCode, SamplingParameters
from reporter import generate_reports
from sampler import generate_sample, generate_sample_streaming


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the audit sampling CLI.

    Returns:
        argparse.Namespace: Parsed command-line namespace populated from CLI input.
    """

    parser = argparse.ArgumentParser(
        description="Audit sampling tool (random non-statistical)",
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to population CSV file",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory where the Excel report will be saved",
    )
    parser.add_argument(
        "--tolerable",
        type=float,
        required=True,
        help="Tolerable misstatement amount",
    )
    parser.add_argument(
        "--expected",
        type=float,
        required=True,
        help="Expected misstatement amount",
    )
    parser.add_argument(
        "--assurance",
        type=float,
        required=True,
        help="Assurance factor",
    )
    parser.add_argument(
        "--balance-type",
        choices=["debit", "credit", "both"],
        default="both",
        help="Balance category to include",
    )
    parser.add_argument(
        "--high-value",
        type=float,
        default=None,
        help="Override high-value threshold (sampling interval)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Deterministic random seed",
    )
    parser.add_argument(
        "--include-zeros",
        action="store_true",
        help="Include zero-amount transactions in the population",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help=(
            "Enable streaming mode for large CSVs (1M+ rows). "
            "Reads input twice with reservoir sampling."
        ),
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Show progress bars for streaming operations",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Optional run identifier; if omitted a UUID is generated",
    )
    return parser.parse_args()


def main() -> int:
    """Run the full sampling workflow from CLI parameters to report output.

    Returns:
        int: Process exit status code (0 indicates success).
    """

    args = parse_args()
    params = SamplingParameters(
        tolerable_misstatement=args.tolerable,
        expected_misstatement=args.expected,
        assurance_factor=args.assurance,
        balance_type=args.balance_type,
        high_value_override=args.high_value,
        random_seed=args.seed,
        exclude_zero_amounts=not args.include_zeros,
    )
    # Use provided run_id from API, or generate a new UUID
    run_id = args.run_id if args.run_id else str(uuid4())
    configure_logging(run_id)
    log = get_logger("main")
    log.info(EventCode.RUN_START.value, parameters=params.model_dump())
    started = time.perf_counter()
    started_dt = datetime.now(timezone.utc)
    cleaned, quality_report = clean_data(args.input)
    log.info(
        EventCode.CLEANING_DONE.value,
        total_rows=len(cleaned),
        quality=quality_report.model_dump(),
    )
    cleaning_end = time.perf_counter()
    cleaning_seconds = cleaning_end - started

    sampling_start = time.perf_counter()
    if args.fast:
        sample, stats = generate_sample_streaming(
            args.input, params, show_progress=args.progress
        )
    else:
        sample, stats = generate_sample(cleaned, params)

    quality_report = quality_report.model_copy(
        update={
            "excluded_zero_amounts": stats.excluded_zero_amounts,
            "excluded_due_to_balance": stats.excluded_due_to_balance,
        }
    )
    sampling_end = time.perf_counter()
    sampling_seconds = sampling_end - sampling_start
    log.info(
        EventCode.SAMPLING_DONE.value,
        population_size=stats.population_size,
        coverage=stats.coverage_percent,
    )
    timestamp = datetime.now(timezone.utc)
    report_start = time.perf_counter()
    report_path = generate_reports(
        args.output_dir,
        sample,
        quality_report,
        stats,
        params,
        timestamp,
        run_id,
    )
    report_end = time.perf_counter()
    reporting_seconds = report_end - report_start
    log.info(EventCode.REPORT_WRITTEN.value, path=str(report_path))
    print(f"Report generated at: {report_path}")
    finished_dt = datetime.now(timezone.utc)
    total_duration = time.perf_counter() - started
    # Round durations to 2 decimals
    cleaning_seconds = round(cleaning_seconds, 2)
    sampling_seconds = round(sampling_seconds, 2)
    reporting_seconds = round(reporting_seconds, 2)
    total_duration = round(total_duration, 2)
    from models import RunSummary

    summary = RunSummary(
        run_id=run_id,
        started_at_utc=started_dt,
        finished_at_utc=finished_dt,
        duration_seconds=total_duration,
        cleaning_seconds=cleaning_seconds,
        sampling_seconds=sampling_seconds,
        reporting_seconds=reporting_seconds,
        parameters=params.model_dump(),
        data_quality=quality_report.model_dump(),
        sample_statistics=stats.model_dump(),
        sample_size=len(sample),
        output_excel=str(report_path),
    )
    runs_dir = args.output_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    summary_path = runs_dir / f"{run_id}.json"
    summary_json = summary.model_dump(mode="json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_json, f, indent=2)
    log.info(EventCode.RUN_SUMMARY.value, path=str(summary_path))
    print(f"Summary written to: {summary_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
