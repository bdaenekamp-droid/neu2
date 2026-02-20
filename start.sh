#!/usr/bin/env bash
set -e

# start using venv python to ensure deps available
exec .venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
