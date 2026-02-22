import os
import sys
import glob
import logging
import requests
import random
from datetime import datetime, timedelta
from contextlib import contextmanager

import yt_dlp
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from appwrite.id import ID
from googleapiclient.discovery import build


# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────

SEARCH_QUERIES = [
    "مهندسی مکانیک",
    "Mechanical Engineering shorts",
    "Mechanical mechanisms",
    "Engineering gears animation",
    "CNC machining process",
    "Thermodynamics experiment",
    "Fluid mechanics shorts",
    "Robotics mechanical design",
    "manufacturing process satisfying",
    "hydraulic press machine",
]

CAPTION_TEMPLATE = (
    "🎥 **{title}**\n\n"
    "🔗 [مشاهده در یوتیوب]({url})\n\n"
    "#مهندسی\\_مکانیک #MechanicalEngineering"
)

MAX_DURATION_SECONDS = 179
MAX_POSTS_PER_RUN    = 2
SEARCH_LOOKBACK_DAYS = 180
MAX_FILE_SIZE_BYTES  = 50 * 1024 * 1024  # 50MB


# ─────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────

class QuietLogger:
    def debug(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass
    def info(self, msg): pass


@contextmanager
def suppress_stderr():
    old_stderr = sys.stderr
    try:
        with open(os.devnull, 'w') as devnull:
            sys.stderr = devnull
            yield
    finally:
        sys.stderr = old_stderr


def cleanup_files(file_list: list) -> None:
    for f in file_list:
        try:
            if os.path.exists(f):
                os.remove(f)
        except OSError as e:
            logging.warning(f"Could not remove file {f}: {e}")


def get_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return value


# ─────────────────────────────────────────────
# Appwrite Helpers
# ─────────────────────────────────────────────

def is_video_duplicate(databases, db_id: str, col_id: str, video_id: str) -> bool:
    with suppress_stderr():
        try:
            result = databases.list_documents(
                database_id=db_id,
                collection_id=col_id,
                queries=[Query.equal("videoId", video_id)]
            )
            return result['total'] > 0
        except Exception:
            return False


def register_video(databases, db_id: str, col_id: str, video_id: str) -> bool:
    with suppress_stderr():
        try:
            databases.create_document(
                database_id=db_id,
                collection_id=col_id,
                document_id=ID.unique(),
                data={"videoId": video_id}
            )
            return True
        except Exception as e:
            logging.error(f"Appwrite write failed for {video_id}: {e}")
            return False


# ─────────────────────────────────────────────
# Download Logic (No FFmpeg)
# ─────────────────────────────────────────────

def get_best_merged_format(formats: list) -> str | None:
    """
    فرمت‌هایی را پیدا می‌کند که از پیش audio+video دارند
    و نیازی به FFmpeg برای merge ندارند.
    """
    merged = []
    for f in formats:
        has_video = f.get('vcodec', 'none') != 'none'
        has_audio = f.get('acodec', 'none') != 'none'
        ext_ok    = f.get('ext') in ('mp4', 'webm')
        size      = f.get('filesize') or f.get('filesize_approx') or 0
        size_ok   = 0 < size < MAX_FILE_SIZE_BYTES

        if has_video and has_audio and ext_ok and size_ok:
            merged.append(f)

    if not merged:
        return None

    # بالاترین bitrate را انتخاب می‌کند
    best = max(merged, key=lambda f: f.get('tbr') or f.get('vbr') or 0)
    return best['format_id']


def build_base_ydl_opts(cookie_path: str) -> dict:
    opts = {
        'quiet': True,
        'no_warnings': True,
        'logger': QuietLogger(),
        'noplaylist': True,
        'extractor_args': {
            'youtube': {'player_client': ['android', 'web']}
        },
    }
    if cookie_path and os.path.exists(cookie_path):
        opts['cookiefile'] = cookie_path
    return opts


def download_video(video_url: str, video_id: str, base_opts: dict, context) -> str | None:
    try:
        # مرحله ۱: فقط اطلاعات را بگیر (بدون دانلود)
        info_opts = {**base_opts, 'outtmpl': '/tmp/%(id)s.%(ext)s'}
        with yt_dlp.YoutubeDL(info_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)

        if not info:
            return None

        # مرحله ۲: بررسی مدت زمان
        duration = info.get('duration', 0)
        if duration == 0 or duration > MAX_DURATION_SECONDS:
            return None

        # مرحله ۳: پیدا کردن فرمت یکپارچه
        formats = info.get('formats', [])
        format_id = get_best_merged_format(formats)

        if not format_id:
            context.log(f"⚠️  No merged format for {video_id} (needs FFmpeg)")
            return None

        # مرحله ۴: دانلود با فرمت انتخاب‌شده
        dl_opts = {
            **base_opts,
            'format': format_id,
            'outtmpl': '/tmp/%(id)s.%(ext)s',
        }
        with yt_dlp.YoutubeDL(dl_opts) as ydl:
            ydl.download([video_url])

    except yt_dlp.utils.DownloadError as e:
        context.log(f"⚠️  DownloadError {video_id}: {str(e)[:80]}")
        return None
    except Exception as e:
        context.log(f"⚠️  Unexpected error {video_id}: {str(e)[:80]}")
        return None

    # مرحله ۵: یافتن فایل دانلودشده
    downloaded = glob.glob(f"/tmp/{video_id}.*")
    valid = [f for f in downloaded if not f.endswith('.part')]
    return valid[0] if valid else None


# ─────────────────────────────────────────────
# Telegram
# ─────────────────────────────────────────────

def send_to_telegram(
    token: str,
    channel: str,
    file_path: str,
    title: str,
    video_url: str
) -> bool:
    api_url = f"https://api.telegram.org/bot{token}/sendVideo"
    caption = CAPTION_TEMPLATE.format(title=title, url=video_url)

    try:
        with open(file_path, 'rb') as video_file:
            response = requests.post(
                api_url,
                data={
                    "chat_id": channel,
                    "caption": caption,
                    "parse_mode": "Markdown",
                    "supports_streaming": True,
                },
                files={"video": video_file},
                timeout=120,
            )

        if response.status_code != 200:
            logging.warning(
                f"Telegram error {response.status_code}: {response.text[:200]}"
            )
            return False
        return True

    except requests.RequestException as e:
        logging.error(f"Telegram request failed: {e}")
        return False


# ─────────────────────────────────────────────
# Main Entry Point
# ─────────────────────────────────────────────

def main(context):
    context.log("⏰ Bot execution started...")

    # ── بارگذاری متغیرهای محیطی ──────────────
    try:
        endpoint        = get_env("APPWRITE_ENDPOINT")
        project_id      = get_env("APPWRITE_PROJECT_ID")
        api_key         = get_env("APPWRITE_API_KEY")
        db_id           = get_env("APPWRITE_DATABASE_ID")
        collection_id   = get_env("APPWRITE_COLLECTION_ID")
        youtube_api_key = get_env("YOUTUBE_API_KEY")
        tg_token        = get_env("TELEGRAM_TOKEN")
        tg_channel      = get_env("TELEGRAM_CHANNEL")
    except EnvironmentError as e:
        context.error(str(e))
        return context.res.json({"success": False, "error": str(e)})

    # ── راه‌اندازی Appwrite ───────────────────
    client = Client()
    client.set_endpoint(endpoint)
    client.set_project(project_id)
    client.set_key(api_key)
    databases = Databases(client)

    # ── راه‌اندازی YouTube API ────────────────
    youtube = build('youtube', 'v3', developerKey=youtube_api_key)

    # ── جستجو در یوتیوب ──────────────────────
    search_query = random.choice(SEARCH_QUERIES)
    context.log(f"🔍 Query: {search_query}")

    published_after = (
        datetime.utcnow() - timedelta(days=SEARCH_LOOKBACK_DAYS)
    ).isoformat() + "Z"

    try:
        search_response = youtube.search().list(
            q=search_query,
            part='snippet',
            type='video',
            videoDuration='short',
            order='viewCount',
            publishedAfter=published_after,
            maxResults=50
        ).execute()
    except Exception as e:
        context.error(f"YouTube API Error: {e}")
        return context.res.json({"success": False, "error": "YouTube API Error"})

    # ── آماده‌سازی yt-dlp ─────────────────────
    base_dir    = os.path.dirname(os.path.abspath(__file__))
    cookie_path = os.path.join(base_dir, 'cookies.txt')
    base_opts   = build_base_ydl_opts(cookie_path)

    # ── پردازش ویدیوها ────────────────────────
    videos_posted = 0
    stats = {
        "duplicates":     0,
        "too_long":       0,
        "format_error":   0,
        "telegram_error": 0,
    }

    for item in search_response.get('items', []):
        if videos_posted >= MAX_POSTS_PER_RUN:
            break

        video_id  = item['id']['videoId']
        title     = item['snippet']['title']
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        # ── بررسی تکراری بودن ─────────────────
        if is_video_duplicate(databases, db_id, collection_id, video_id):
            stats["duplicates"] += 1
            continue

        context.log(f"⬇️  Downloading: {video_id}")

        # ── دانلود ────────────────────────────
        file_path = download_video(video_url, video_id, base_opts, context)

        if not file_path:
            stats["format_error"] += 1
            cleanup_files(glob.glob(f"/tmp/{video_id}.*"))
            continue

        # ── ارسال به تلگرام ───────────────────
        success = send_to_telegram(tg_token, tg_channel, file_path, title, video_url)
        cleanup_files([file_path])

        if success:
            register_video(databases, db_id, collection_id, video_id)
            videos_posted += 1
            context.log(f"✅ Posted: {video_id} — {title[:50]}")
        else:
            stats["telegram_error"] += 1

    # ── گزارش نهایی ───────────────────────────
    context.log(
        f"📊 Run complete | "
        f"Posted: {videos_posted} | "
        f"Duplicates: {stats['duplicates']} | "
        f"Format issues: {stats['format_error']} | "
        f"Too long: {stats['too_long']} | "
        f"Telegram errors: {stats['telegram_error']}"
    )

    return context.res.json({
        "success": True,
        "posted_count": videos_posted,
        "stats": stats,
    })
