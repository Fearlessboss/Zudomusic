FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
PYTHONUNBUFFERED=1 \
AUTO_INSTALL_DEPS=false \
LOG_LEVEL=INFO \
RUNTIME_DIR=/app/runtime \
ENV_FILE=/app/.env

Install ffmpeg + build essentials

RUN apt-get update \
&& apt-get install -y --no-install-recommends \
ffmpeg \
gcc \
g++ \
make \
curl \
ca-certificates \
&& rm -rf /var/lib/apt/lists/*

Upgrade pip first

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

Install bot dependencies (pinned versions for stability)

RUN pip install --no-cache-dir \
"pyrogram>=2.0.106" \
"tgcrypto>=1.2.5" \
"py-tgcalls>=2.2.0" \
"yt-dlp>=2025.3.31"

Copy app files

COPY music.py /app/music.py
COPY .env /app/.env

Create runtime directory

RUN mkdir -p /app/runtime/clones /app/runtime/logs /app/runtime/pids /app/runtime/states /app/runtime/control

Run as exec form so SIGTERM reaches the Python process directly

CMD ["python", "-u", "music.py"]
