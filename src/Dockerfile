FROM openruntimes/python:v4-3.11

WORKDIR /usr/local/server/src/function

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install "yt-dlp>=2024.11.18" && \
    pip install -r requirements.txt

COPY . .
