# ═══════════════════════════════════════════════════════════════════
# ZUDO X MUSIC v10 — Dockerfile
# ═══════════════════════════════════════════════════════════════════
FROM python:3.11-slim

WORKDIR /app

# Environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AUTO_INSTALL_DEPS=false \
    LOG_LEVEL=INFO \
    RUNTIME_DIR=/app/runtime \
    ENV_FILE=/app/.env \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# ─── Install ffmpeg + build essentials ───
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        gcc \
        g++ \
        make \
        curl \
        ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ─── Upgrade pip ───
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# ─── Install bot dependencies ───
RUN pip install --no-cache-dir \
        "pyrogram>=2.0.106" \
        "tgcrypto>=1.2.5" \
        "py-tgcalls>=2.2.0" \
        "yt-dlp>=2025.3.31"

# ─── Copy app files ───
COPY music.py /app/music.py

# .env optional — agar nahi hai to skip
COPY .en[v] /app/.env

# ─── Create runtime directories ───
RUN mkdir -p \
    /app/runtime/clones \
    /app/runtime/logs \
    /app/runtime/pids \
    /app/runtime/states \
    /app/runtime/control \
    /app/runtime/shared

# ─── Run bot ───
CMD ["python", "-u", "music.py"]
