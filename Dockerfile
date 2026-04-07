# ================================================
# ZUDO X MUSIC BOT - Python Version (Full VC Bot)
# Pyrogram + PyTgCalls + yt-dlp + Clone Mode
# Script Name: music.py
# ================================================

FROM python:3.11-slim

# System dependencies (ffmpeg compulsory hai VC streaming ke liye)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    git \
    && rm -rf /var/lib/apt/lists/*

# Working directory
WORKDIR /app

# Script ko music.py naam se copy karo
COPY music.py /app/music.py

# Data directories create kar do (clones, logs, pids ke liye)
RUN mkdir -p /app/musicbot_runtime/clones \
             /app/musicbot_runtime/logs \
             /app/musicbot_runtime/pids \
    && chmod -R 777 /app/musicbot_runtime

# Environment variables
ENV API_ID=123456
ENV API_HASH=PASTE_API_HASH_HERE
ENV MAIN_BOT_TOKEN=PASTE_MAIN_BOT_TOKEN_HERE
ENV OWNER_ID=123456789
ENV DEFAULT_ASSISTANT_SESSION=PASTE_DEFAULT_STRING_SESSION_HERE
ENV MASTER_SUPPORT_CHAT=@userbotsupportchat
ENV MASTER_OWNER_USERNAME=@ITZ_ME_ADITYA_02
ENV BOT_BRAND_NAME="AURA X MUSIC"
ENV BOT_BRAND_TAGLINE="Ultra Fast • No Lag • Voice Chat Player"

# Required packages install (script khud bhi install karega)
RUN python -m pip install --no-cache-dir --upgrade \
    pyrogram \
    tgcrypto \
    py-tgcalls \
    yt-dlp \
    aiohttp

# Non-root user for security
RUN useradd -m -u 1001 botuser
USER botuser

# Volume for persistence
VOLUME /app/musicbot_runtime

# Bot start (music.py se)
CMD ["python", "music.py"]
