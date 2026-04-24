FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libffi-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY news_hunter/ ./news_hunter/
COPY news_hunter_service.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "news_hunter_service.py"]
