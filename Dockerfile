FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY collector.py data_processor.py pre_safety_module.py app.py web_app.py ./
COPY templates ./templates
COPY docker_entrypoint.sh ./docker_entrypoint.sh

RUN chmod +x /app/docker_entrypoint.sh && mkdir -p /app/data

VOLUME ["/app/data"]

EXPOSE 5000

ENTRYPOINT ["/app/docker_entrypoint.sh"]
CMD ["worker"]
