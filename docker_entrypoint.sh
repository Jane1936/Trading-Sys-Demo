#!/usr/bin/env sh
set -eu

MODE="${1:-worker}"

if [ "$MODE" = "worker" ]; then
  exec python app.py
fi

if [ "$MODE" = "web" ]; then
  exec gunicorn -w 2 -b 0.0.0.0:5000 web_app:app
fi

echo "Unknown mode: $MODE (expected: worker | web)" >&2
exit 1
