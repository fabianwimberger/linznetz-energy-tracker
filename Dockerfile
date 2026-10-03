FROM python:3.14-alpine AS builder

WORKDIR /app

COPY . .
RUN python -m venv /opt/venv \
    && /opt/venv/bin/python -m pip install --no-cache-dir .
RUN python download_vendors.py

FROM python:3.14-alpine

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY . .
COPY --from=builder /app/static/vendor /app/static/vendor

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/latest-date')" || exit 1

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
