FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Aplikácia sa nespúšťa ako root
RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8000

# Health check bez curl (v slim obraze ho nie je) – cez Python
HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=4)" || exit 1

# Jeden proces = jeden uzol (dôležité: zámok Lamportových hodín v rámci procesu)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
