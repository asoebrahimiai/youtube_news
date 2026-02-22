FROM python:3.11-slim

# نصب ffmpeg و ابزارهای ضروری
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        curl \
        ca-certificates \
        && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/lib/python3.11/site-packages/openruntimes

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir --upgrade yt-dlp

COPY . .