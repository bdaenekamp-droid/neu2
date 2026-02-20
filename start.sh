#!/usr/bin/env bash
set -e

# optional: print versions for debugging
python --version
node --version || true

exec python -m uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
