FROM apache/superset:latest

USER root

# Install PostgreSQL client libraries
RUN apt-get update && apt-get install -y libpq-dev && rm -rf /var/lib/apt/lists/*

USER superset

# Install database drivers directly
RUN pip install --user psycopg2-binary pymongo

# Copy custom configuration
COPY --chown=superset:superset superset_config.py /app/pythonpath/superset_config.py

# Set Python path to include user site-packages and config
ENV PYTHONPATH="/app/pythonpath:/app/superset_home/.local/lib/python3.10/site-packages:${PYTHONPATH}"
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

CMD ["bash", "-c", "superset db upgrade && superset fab create-admin --username admin --firstname Admin --lastname User --email admin@repobox.local --password admin || true && superset init && superset run -h 0.0.0.0 -p 8088 --with-threads --reload"]
