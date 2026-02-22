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
# Utilities
# ─────────────────────────────────────────────

class QuietLogger:
    """Suppress yt-dlp internal logs."""
    def debug(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass
    def info(self, msg): pass


@contextmanager
def suppress_stderr():
    """Thread-safer alternative to direct sys.stderr manipulation."""
    old_stderr = sys.stderr
    try:
        with open(os.devnull, 'w') as devnull:
            sys.stderr = devnull
            yield
    finally:
        sys.stderr = old_stderr


def cleanup_files(file_list: list[str]) -> None:
    """Remove all temporary files safely."""
    for f in file_list:
        try:
            if os.path.exists(f):
                os.remove(f)
        except OSError as e:
            logging.warning(f"Could not remove file {f}: {e}")


# ─────────────────────────────────────────────
# Core Logic
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

MAX_DURATION_SECONDS = 179   # زیر 3 دقیقه
MAX_POSTS_PER_RUN    = 2
SEARCH_LOOKBACK_DAYS = 180


def get_env(key: str) -> str:
    """Read required env var or raise."""
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return value


def build_ydl_opts(cookie_path: str | None) -> dict:
    opts = {
        # اولویت: فرمت یکپارچه mp4 360p → بهترین فرمت یکپارچه
        'format': '18/b[ext=mp4][vcodec!*=av01]/b[ext=mp4]/b',
        'outtmpl': '/tmp/%(id)s.%(ext)s',
        'quiet': True,
        'no_warnings': True,
        'logger': QuietLogger(),
        'noplaylist': True,
        'extractor_args': {
            'youtube': {'player_client': ['android', 'web']}
        },
        # جلوگیری از دانلود فایل‌های نیازمند merge
        'merge_output_format': None,
    }
    if cookie_path and os.path.exists(cookie_path):
        opts['cookiefile'] = cookie_path
    return opts


def is_video_duplicate(databases: Databases, db_id: str, col_id: str, video_id: str) -> bool:
    """Check if video already exists in Appwrite collection."""
    with suppress_stderr():
        try:
            result = databases.list_documents(
                database_id=db_id,
                collection_id=col_id,
                queries=[Query.equal("videoId", video_id)]
            )
            return result['total'] > 0
        except Exception:
            # در صورت خطای شبکه، محافظه‌کارانه False برمی‌گردانیم
            return False


def register_video(databases: Databases, db_id: str, col_id: str, video_id: str) -> bool:
    """Save video_id to Appwrite to prevent future duplicates."""
    with suppress_stderr():
        try:
            databases.create_document(
                database_id=db_id,
                collection_id=col_id,
                document_id=ID.unique(),          # ✅ صحیح
                data={"videoId": video_id}
            )
            return True
        except Exception as e:
            logging.error(f"Appwrite write failed for {video_id}: {e}")
            return False


def download_video(video_url: str, video_id: str, ydl_opts: dict) -> str | None:
    """
    Download video and return file path.
    Returns None if:
      - Duration is out of range
      - No merged/single-stream format available
      - Any yt-dlp error
    """
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
            if not info:
                return None

            duration = info.get('duration', 0)
            if duration == 0 or duration > MAX_DURATION_SECONDS:
                return None

            # بررسی اینکه آیا فرمت یکپارچه وجود دارد (بدون نیاز به ffmpeg)
            selected_format = info.get('format_id', '')
            requested_formats = info.get('requested_formats')
            if requested_formats and len(requested_formats) > 1:
                # یعنی yt-dlp می‌خواهد audio+video را merge کند → رد کن
                return None

            ydl.download([video_url])

    except yt_dlp.utils.DownloadError:
        return None
    except Exception:
        return None

    # یافتن فایل دانلودشده
    downloaded = glob.glob(f"/tmp/{video_id}.*")
    valid = [f for f in downloaded if not f.endswith('.part')]
    return valid[0] if valid else None


def send_to_telegram(
    token: str,
    channel: str,
    file_path: str,
    title: str,
    video_url: str
) -> bool:
    """Upload video to Telegram channel. Returns True on success."""
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
                timeout=120,          # ✅ timeout اضافه شد
            )
        if response.status_code != 200:
            logging.warning(f"Telegram API error {response.status_code}: {response.text[:200]}")
            return False
        return True

    except requests.RequestException as e:
        logging.error(f"Telegram request failed: {e}")
        return False


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────

def main(context):
    context.log("⏰ Bot execution started...")

    # ── بارگذاری تنظیمات ──────────────────────
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

    # ── راه‌اندازی سرویس‌ها ───────────────────
    client = Client()
    client.set_endpoint(endpoint)
    client.set_project(project_id)
    client.set_key(api_key)
    databases = Databases(client)

    youtube = build('youtube', 'v3', developerKey=youtube_api_key)

    # ── جستجوی یوتیوب ─────────────────────────
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

    # ── آماده‌سازی yt-dlp ──────────────────────
    base_dir    = os.path.dirname(os.path.abspath(__file__))
    cookie_path = os.path.join(base_dir, 'cookies.txt')
    ydl_opts    = build_ydl_opts(cookie_path)

    # ── پردازش ویدیوها ────────────────────────
    videos_posted = 0
    stats = {
        "duplicates":    0,
        "too_long":      0,
        "format_error":  0,
        "telegram_error": 0,
    }

    for item in search_response.get('items', []):
        if videos_posted >= MAX_POSTS_PER_RUN:
            break

        video_id  = item['id']['videoId']
        title     = item['snippet']['title']
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        # ── بررسی تکراری ──────────────────────
        if is_video_duplicate(databases, db_id, collection_id, video_id):
            stats["duplicates"] += 1
            continue

        context.log(f"⬇️  Downloading: {video_id}")

        # ── دانلود ────────────────────────────
        file_path = download_video(video_url, video_id, ydl_opts)
        if not file_path:
            stats["format_error"] += 1
            # پاکسازی فایل‌های ناقص احتمالی
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
    summary = (
        f"📊 Run complete | "
        f"Posted: {videos_posted} | "
        f"Duplicates: {stats['duplicates']} | "
        f"Format issues: {stats['format_error']} | "
        f"Too long: {stats['too_long']} | "
        f"Telegram errors: {stats['telegram_error']}"
    )
    context.log(summary)

    return context.res.json({
        "success": True,
        "posted_count": videos_posted,
        "stats": stats,
    })
