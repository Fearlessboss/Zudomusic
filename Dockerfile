# ================================================
# ZUDO X MUSIC BOT - Docker Setup (Node.js)
# ================================================

FROM node:20-alpine

# System dependencies install (yt-dlp + ffmpeg)
RUN apk add --no-cache \
    ffmpeg \
    python3 \
    py3-pip \
    && pip3 install --no-cache-dir --upgrade yt-dlp

# Working directory
WORKDIR /app

# Pehle package files copy karo (cache better ke liye)
COPY package*.json ./

# Dependencies install (production only)
RUN npm ci --only=production && npm cache clean --force

# Baaki code copy karo
COPY index.js ./

# Downloads folder (audio files ke liye)
RUN mkdir -p downloads && chmod 777 downloads

# Environment variables (optional - .env se bhi le sakta hai)
ENV BOT_TOKEN=""
ENV OWNER_ID="7661825494"
ENV BOT_BRAND_NAME="ZUDO X MUSIC"
ENV BOT_BRAND_TAGLINE="Ultra Fast • No Lag"

# Bot ko non-root user se chalao (security ke liye)
RUN addgroup -g 1001 botgroup && \
    adduser -D -u 1001 -G botgroup botuser
USER botuser

# Volume for downloads (optional)
VOLUME /app/downloads

# Bot start
CMD ["python", "music.py"]
