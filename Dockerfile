FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY collector.py data_processor.py app.py ./

RUN mkdir -p /app/data

VOLUME ["/app/data"]

CMD ["python", "app.py"]
