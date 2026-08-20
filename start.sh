#!/usr/bin/env bash
set -e

# The project API and PostgreSQL repository live in server.js.  Starting the
# former FastAPI-only entry point made every /api/projects request fall through
# to its SPA guard and return 404.
exec node server.js
