FROM openruntimes/python:v4-3.11

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install/Update yt-dlp to latest version
RUN pip install --upgrade pip && \
    pip install --upgrade yt-dlp && \
    yt-dlp --version

# Verify installation
RUN python3 -c "import yt_dlp; print('yt-dlp version:', yt_dlp.version.__version__)"

WORKDIR /usr/local/server