FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /app/.dagster

COPY . .
EXPOSE 3000 8501

CMD ["dagster", "dev", "-m", "orchestration.defs", "-h", "0.0.0.0", "-p", "3000"]
