#!/usr/bin/env bash
set -euo pipefail

printf 'Running static checks and tests (macOS/Linux)...\n'

VENV_DIR=".venv"
if [[ ! -f "$VENV_DIR/bin/activate" ]]; then
  printf 'Virtual environment not found. Creating one...\n'
  python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"

python -m pip install --upgrade pip >/dev/null
python -m pip install black==24.10.0 pyflakes==3.2.0 isort==5.13.2 pytest==8.3.3 httpx==0.27.2 >/dev/null

TARGETS=()
[[ -d "worker/src" ]] && TARGETS+=("worker/src")
[[ -d "restapi/src" ]] && TARGETS+=("restapi/src")

if [[ ${#TARGETS[@]} -eq 0 ]]; then
  printf 'No source folders found.\n'
  exit 1
fi

printf '\nRunning black (line length 79)...\n'
python -m black -l 79 "${TARGETS[@]}"

printf '\nRunning isort...\n'
python -m isort "${TARGETS[@]}"

printf '\nRunning pyflakes...\n'
for target in "${TARGETS[@]}"; do
  python -m pyflakes "$target"
done

PYTEST_TARGETS=()
[[ -d "worker/tests" ]] && PYTEST_TARGETS+=("worker/tests")
[[ -d "restapi/tests" ]] && PYTEST_TARGETS+=("restapi/tests")

if [[ ${#PYTEST_TARGETS[@]} -eq 0 ]]; then
  printf 'No pytest targets found.\n'
  exit 1
fi

printf '\nRunning pytest (verbose)...\n'
python -m pytest -vv "${PYTEST_TARGETS[@]}"

printf '\nAll static checks and tests passed successfully.\n'
