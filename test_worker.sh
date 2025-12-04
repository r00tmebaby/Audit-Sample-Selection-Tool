#!/usr/bin/env bash
set -euo pipefail

printf 'Running worker end-to-end smoke tests (macOS/Linux)...\n'

VENV_DIR=".venv"
if [[ ! -f "$VENV_DIR/bin/activate" ]]; then
  printf 'Creating virtual environment...\n'
  python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"

if ! python -m pip show worker >/dev/null 2>&1; then
  printf 'Installing worker package in editable mode...\n'
  python -m pip install -q -e worker
else
  printf 'Worker package already installed.\n'
fi

DATA_FILE="data/population_data.csv"
if [[ ! -f "$DATA_FILE" ]]; then
  printf 'Sample data file not found at %s\n' "$DATA_FILE"
  exit 1
fi

OUTPUT_DIR="worker_test_output"
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

COMMON_ARGS=(
  --input "$PWD/$DATA_FILE"
  --output-dir "$PWD/$OUTPUT_DIR"
  --tolerable 1000
  --expected 100
  --assurance 2
  --seed 42
)

run_test() {
  local label="$1"
  shift
  printf '\n%s\n' "$label"
  python -m worker.src.main "${COMMON_ARGS[@]}" "$@"
  assert_outputs "$label"
}

assert_outputs() {
  local label="$1"
  local output_file="$OUTPUT_DIR/sample_selection_output.xlsx"
  local runs_dir="$OUTPUT_DIR/runs"
  [[ -f "$output_file" ]] || { printf '%s failed: output file missing.\n' "$label"; exit 1; }
  [[ -d "$runs_dir" ]] || { printf '%s failed: runs directory missing.\n' "$label"; exit 1; }
  local summary
  summary=$(ls "$runs_dir"/*.json 2>/dev/null | head -n 1)
  [[ -n "$summary" ]] || { printf '%s failed: no run summary JSON found.\n' "$label"; exit 1; }
  grep -qi "balance_type" "$summary" || { printf '%s failed: balance_type missing in summary.\n' "$label"; exit 1; }
}

run_test "Test 1: default parameters (balance both, exclude zeros)"
run_test "Test 2: debit-only with zeros included" --balance-type debit --include-zeros
run_test "Test 3: fast streaming mode" --fast

printf '\nWorker smoke tests passed.\n'

