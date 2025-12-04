"""Tests for run summary JSON generation."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.usefixtures("clean_output")
def test_run_summary_created(tmp_path: Path) -> None:
    csv = tmp_path / "data.csv"
    csv.write_text(
        "transaction_id,amount,effective_date,document_type,description\n"
        "A,100,01/01/2024,INV,Test\n"
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    env = os.environ.copy()
    # Ensure worker/src is on PYTHONPATH, mirroring production usage
    worker_src = Path.cwd() / "worker" / "src"
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [env.get("PYTHONPATH"), str(worker_src)])
    )
    cmd = [
        sys.executable,
        "-m",
        "src.main",
        "--input",
        str(csv),
        "--output-dir",
        str(out_dir),
        "--tolerable",
        "1000",
        "--expected",
        "100",
        "--assurance",
        "3",
        "--seed",
        "5",
        "--fast",
    ]
    # Run from the worker directory to match runtime expectations
    subprocess.run(cmd, check=True, env=env, cwd=str(Path.cwd() / "worker"))
    runs_dir = out_dir / "runs"
    summaries = list(runs_dir.glob("*.json"))
    assert summaries, "No run summary JSON generated"
    data = json.loads(summaries[0].read_text())
    assert data["run_id"] in summaries[0].name
    assert "duration_seconds" in data
    assert isinstance(data["duration_seconds"], float)
    assert round(data["duration_seconds"], 2) == data["duration_seconds"]
