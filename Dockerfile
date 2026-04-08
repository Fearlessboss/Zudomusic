FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AUTO_INSTALL_DEPS=false \
    LOG_LEVEL=INFO \
    RUNTIME_DIR=/app/runtime \
    ENV_FILE=/app/.env

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg gcc \
    && rm -rf /var/lib/apt/lists/*

COPY music.py /app/music.py
COPY .env /app/.env

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
        pyrogram>=2.0.106 \
        tgcrypto>=1.2.5 \
        py-tgcalls>=2.1.0 \
        yt-dlp>=2025.3.31

CMD ["python", "music.py"]
