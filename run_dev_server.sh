#!/usr/bin/env bash
set -euo pipefail

if ! python3 -m ensurepip --version >/dev/null 2>&1; then
  printf 'Python venv tooling (ensurepip) is missing.\n' >&2
  printf 'On Debian/Ubuntu, run: sudo apt install python3-venv\n' >&2
  exit 1
fi

printf 'Starting Audit Sampling REST API...\n\n'
printf 'Working directory: %s\n\n' "$PWD"

VENV_DIR=.venv
NEED_DEPS=0

if [[ ! -f "$VENV_DIR/bin/activate" ]]; then
  printf 'Creating virtual environment...\n'
  python3 -m venv "$VENV_DIR"
  NEED_DEPS=1
fi

source "$VENV_DIR/bin/activate"

if [[ $NEED_DEPS -eq 1 ]]; then
  if [[ -f requirements.txt ]]; then
    printf 'Installing Python requirements...\n'
    python -m pip install --upgrade pip >/dev/null
    python -m pip install -r requirements.txt >/dev/null
  else
    printf 'requirements.txt not found; skipping dependency install.\n'
  fi
fi

if [[ -f worker/setup.py ]]; then
  if ! python -m pip show worker >/dev/null 2>&1; then
    printf 'Installing worker package in editable mode...\n'
    python -m pip install -e worker >/dev/null
  else
    printf 'Worker package already installed.\n'
  fi
else
  export PYTHONPATH="$PWD/worker:${PYTHONPATH:-}"
fi

printf 'Starting uvicorn on http://127.0.0.1:8888 (Reload enabled)\n'
exec python -m uvicorn restapi.src.main:app \
  --host 127.0.0.1 \
  --port 8888 \
  --reload \
  --log-level warning \
  --no-access-log
