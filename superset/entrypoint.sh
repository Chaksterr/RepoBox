#!/bin/bash

# Install database drivers
pip install --no-cache-dir psycopg2-binary pymongo 2>/dev/null || true

# Run the original Superset command
exec "$@"
