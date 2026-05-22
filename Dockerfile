FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AUTO_INSTALL_DEPS=false \
    LOG_LEVEL=INFO \
    RUNTIME_DIR=/app/runtime \
    ENV_FILE=/app/.env

# Install ffmpeg + build essentials + useful libs
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        gcc \
        g++ \
        make \
        curl \
        git \
        wget \
        unzip \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip first
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install bot dependencies
RUN pip install --no-cache-dir \
        "aiohttp>=3.11.0" \
        "aiofiles>=24.1.0" \
        "pillow>=10.0.0" \
        "youtube-search-python>=1.6.6" \
        "pyrogram>=2.0.106" \
        "tgcrypto>=1.2.5" \
        "py-tgcalls>=2.2.0" \
        "yt-dlp>=2025.3.31"

# Copy all project files
COPY . /app

# Create runtime directories
RUN mkdir -p \
    /app/runtime \
    /app/runtime/clones \
    /app/runtime/logs \
    /app/runtime/pids \
    /app/runtime/states \
    /app/runtime/control \
    /app/downloads \
    /app/cache \
    /app/temp

# Give permissions
RUN chmod -R 777 /app/runtime /app/downloads /app/cache /app/temp

# Expose optional port
EXPOSE 8080

# Run bot
CMD ["python", "-u", "music.py"]
