FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /srv

# Runtime dependencies only (tests, Locust and fakeredis live in requirements-dev.txt).
COPY requirements.txt .
# hiredis: C parser for Redis replies (large monitoring reads are otherwise slow in pure Python).
RUN pip install --no-cache-dir -r requirements.txt hiredis

COPY app ./app

# Run as an unprivileged user.
RUN useradd --system --no-create-home appuser
USER appuser

# Overridden per service in docker-compose.yml
CMD ["uvicorn", "app.gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]
