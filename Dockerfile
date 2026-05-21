FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY collector.py data_processor.py pre_safety_module.py app.py web_app.py ./
COPY templates ./templates
COPY docker_entrypoint.sh ./docker_entrypoint.sh

RUN chmod +x /app/docker_entrypoint.sh \
    && mkdir -p /app/data /app/logs \
    && addgroup --system appgroup \
    && adduser --system --ingroup appgroup appuser \
    && chown -R appuser:appgroup /app

USER appuser

VOLUME ["/app/data", "/app/logs"]

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:5000/ || exit 1

ENTRYPOINT ["/app/docker_entrypoint.sh"]
CMD ["worker"]
