import os
import sys
import glob
import logging
import requests
import random
import subprocess
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
    "Mechanical Engineering shorts",
    "Mechanical mechanisms",
    "Engineering gears animation",
    "CNC machining process",
    "Thermodynamics experiment",
    "Fluid mechanics shorts",
    "Robotics mechanical design",
    "manufacturing process satisfying",
    "hydraulic press machine",
    "مهندسی مکانیک",
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

class VerboseLogger:
    """لاگر که خطاها را نمایش می‌دهد اما debug را مخفی می‌کند."""
    def __init__(self, context):
        self.context = context

    def debug(self, msg):
        # فقط خطاهای مهم را نمایش بده
        if 'ERROR' in msg.upper() or 'WARNING' in msg.upper():
            self.context.log(f"   [ytdlp] {msg[:150]}")

    def warning(self, msg):
        self.context.log(f"   [WARN] {msg[:150]}")

    def error(self, msg):
        self.context.log(f"   [ERR] {msg[:200]}")

    def info(self, msg): pass


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
        raise EnvironmentError(f"Missing required env var: {key}")
    return value


def get_ytdlp_version(context) -> None:
    """نسخه yt-dlp نصب شده را نمایش می‌دهد."""
    try:
        result = subprocess.run(
            ['yt-dlp', '--version'],
            capture_output=True, text=True, timeout=10
        )
        context.log(f"ℹ️  yt-dlp version: {result.stdout.strip()}")
    except Exception:
        context.log("⚠️  Could not get yt-dlp version")


# ─────────────────────────────────────────────
# Appwrite
# ─────────────────────────────────────────────

def is_video_duplicate(databases, db_id, col_id, video_id) -> bool:
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


def register_video(databases, db_id, col_id, video_id) -> bool:
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
# yt-dlp Options — چندین استراتژی
# ─────────────────────────────────────────────

def build_opts_strategies(cookie_path: str, context) -> list[dict]:
    """
    چند استراتژی مختلف برای دور زدن bot detection.
    به ترتیب از ساده به پیچیده امتحان می‌شوند.
    """
    cookie_exists = os.path.exists(cookie_path) if cookie_path else False

    common = {
        'quiet': True,
        'no_warnings': True,
        'noplaylist': True,
        'skip_download': True,
    }

    strategies = []

    # ── استراتژی ۱: Android client (کمترین محدودیت) ──
    s1 = {
        **common,
        'extractor_args': {
            'youtube': {
                'player_client': ['android'],
                'player_skip': ['webpage', 'configs'],
            }
        },
        'http_headers': {
            'User-Agent': 'com.google.android.youtube/19.09.37 (Linux; U; Android 12) gzip',
        },
    }
    if cookie_exists:
        s1['cookiefile'] = cookie_path
    strategies.append(('android_client', s1))

    # ── استراتژی ۲: TV Embedded ──
    s2 = {
        **common,
        'extractor_args': {
            'youtube': {
                'player_client': ['tv_embedded'],
            }
        },
        'http_headers': {
            'User-Agent': (
                'Mozilla/5.0 (SMART-TV; Linux; Tizen 5.0) '
                'AppleWebKit/538.1 (KHTML, like Gecko) '
                'Version/5.0 TV Safari/538.1'
            ),
        },
    }
    if cookie_exists:
        s2['cookiefile'] = cookie_path
    strategies.append(('tv_embedded', s2))

    # ── استراتژی ۳: iOS client ──
    s3 = {
        **common,
        'extractor_args': {
            'youtube': {
                'player_client': ['ios'],
            }
        },
        'http_headers': {
            'User-Agent': (
                'com.google.ios.youtube/19.09.3 '
                '(iPhone16,2; U; CPU iOS 17_4 like Mac OS X)'
            ),
        },
    }
    if cookie_exists:
        s3['cookiefile'] = cookie_path
    strategies.append(('ios_client', s3))

    # ── استراتژی ۴: mweb بدون cookie ──
    s4 = {
        **common,
        'extractor_args': {
            'youtube': {
                'player_client': ['mweb', 'web'],
            }
        },
        'http_headers': {
            'User-Agent': (
                'Mozilla/5.0 (Linux; Android 12; Pixel 6) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Mobile Safari/537.36'
            ),
            'Accept-Language': 'en-US,en;q=0.9',
        },
    }
    strategies.append(('mweb_no_cookie', s4))

    return strategies


# ─────────────────────────────────────────────
# Video Info با چند استراتژی
# ─────────────────────────────────────────────

def get_video_info(video_url: str, strategies: list, context) -> tuple[dict | None, dict | None]:
    """
    چند استراتژی را امتحان می‌کند.
    برمی‌گرداند: (info_dict, working_opts) یا (None, None)
    """
    for strategy_name, opts in strategies:
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(video_url, download=False)
                if info:
                    context.log(f"   ✓ Strategy '{strategy_name}' worked")
                    # opts را برای دانلود آماده کن (بدون skip_download)
                    dl_opts = {k: v for k, v in opts.items() if k != 'skip_download'}
                    return info, dl_opts
        except yt_dlp.utils.DownloadError as e:
            err = str(e)[:120]
            context.log(f"   ✗ '{strategy_name}' failed: {err}")
        except Exception as e:
            context.log(f"   ✗ '{strategy_name}' error: {type(e).__name__}: {str(e)[:80]}")

    return None, None


# ─────────────────────────────────────────────
# Download
# ─────────────────────────────────────────────

def download_video(
    video_url: str,
    video_id: str,
    working_opts: dict,
    context
) -> str | None:

    format_chain = (
        'best[ext=mp4][filesize<?50M]'
        '/best[ext=webm][filesize<?50M]'
        '/best[filesize<?50M]'
        '/best'
    )

    dl_opts = {
        **working_opts,
        'format': format_chain,
        'outtmpl': f'/tmp/{video_id}.%(ext)s',
        'overwrites': True,
        'logger': QuietLogger(),
    }

    try:
        with yt_dlp.YoutubeDL(dl_opts) as ydl:
            ydl.download([video_url])

    except yt_dlp.utils.DownloadError as e:
        err_msg = str(e)[:150]
        if 'ffmpeg' in err_msg.lower() or 'merger' in err_msg.lower():
            context.log(f"   🚫 FFmpeg required — skipping")
        else:
            context.log(f"   ⚠️  DownloadError: {err_msg}")
        cleanup_files(glob.glob(f"/tmp/{video_id}.*"))
        return None

    except Exception as e:
        context.log(f"   ⚠️  Unexpected: {str(e)[:100]}")
        cleanup_files(glob.glob(f"/tmp/{video_id}.*"))
        return None

    downloaded = glob.glob(f"/tmp/{video_id}.*")
    valid = [
        f for f in downloaded
        if not f.endswith('.part')
        and os.path.getsize(f) <= MAX_FILE_SIZE_BYTES
    ]

    if not valid:
        context.log(f"   ⚠️  File missing or too large")
        cleanup_files(downloaded)
        return None

    return valid[0]


# ─────────────────────────────────────────────
# Telegram
# ─────────────────────────────────────────────

def send_to_telegram(token, channel, file_path, title, video_url, context) -> bool:
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

        if response.status_code == 200:
            return True

        context.log(f"⚠️  Telegram HTTP {response.status_code}: {response.text[:200]}")
        return False

    except requests.RequestException as e:
        context.log(f"⚠️  Telegram error: {e}")
        return False


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main(context):
    context.log("⏰ Bot execution started...")

    # ── env vars ──────────────────────────────
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

    # ── نسخه yt-dlp ──────────────────────────
    get_ytdlp_version(context)

    # ── Appwrite ──────────────────────────────
    client = Client()
    client.set_endpoint(endpoint)
    client.set_project(project_id)
    client.set_key(api_key)
    databases = Databases(client)

    # ── YouTube API ───────────────────────────
    youtube = build('youtube', 'v3', developerKey=youtube_api_key)

    # ── جستجو ─────────────────────────────────
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

    # ── آماده‌سازی استراتژی‌ها ─────────────────
    base_dir    = os.path.dirname(os.path.abspath(__file__))
    cookie_path = os.path.join(base_dir, 'cookies.txt')
    strategies  = build_opts_strategies(cookie_path, context)

    if os.path.exists(cookie_path):
        context.log("🍪 cookies.txt found")
    else:
        context.log("⚠️  No cookies.txt")

    context.log(f"🎯 Strategies to try: {[s[0] for s in strategies]}")

    # ── پردازش ────────────────────────────────
    videos_posted = 0
    stats = {
        "duplicates":     0,
        "too_long":       0,
        "no_info":        0,
        "format_error":   0,
        "telegram_error": 0,
    }

    for item in search_response.get('items', []):
        if videos_posted >= MAX_POSTS_PER_RUN:
            break

        video_id  = item['id']['videoId']
        title     = item['snippet']['title']
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        if is_video_duplicate(databases, db_id, collection_id, video_id):
            stats["duplicates"] += 1
            continue

        context.log(f"\n⬇️  {video_id} — {title[:45]}")

        # ── دریافت اطلاعات با چند استراتژی ────
        info, working_opts = get_video_info(video_url, strategies, context)

        if not info:
            context.log("   ❌ All strategies failed")
            stats["no_info"] += 1
            continue

        # ── بررسی مدت زمان ────────────────────
        duration = info.get('duration', 0)
        if duration == 0 or duration > MAX_DURATION_SECONDS:
            context.log(f"   ⏭️  Duration {duration}s — skipping")
            stats["too_long"] += 1
            continue

        context.log(f"   ⏱️  Duration: {duration}s — OK")

        # ── دانلود ────────────────────────────
        file_path = download_video(video_url, video_id, working_opts, context)

        if not file_path:
            stats["format_error"] += 1
            continue

        # ── ارسال تلگرام ──────────────────────
        success = send_to_telegram(
            tg_token, tg_channel, file_path, title, video_url, context
        )
        cleanup_files([file_path])

        if success:
            register_video(databases, db_id, collection_id, video_id)
            videos_posted += 1
            context.log(f"   ✅ Posted!")
        else:
            stats["telegram_error"] += 1

    # ── گزارش نهایی ───────────────────────────
    context.log(
        f"\n📊 Done | Posted: {videos_posted} | "
        f"No info: {stats['no_info']} | "
        f"Duplicates: {stats['duplicates']} | "
        f"Format: {stats['format_error']} | "
        f"Telegram: {stats['telegram_error']}"
    )

    return context.res.json({
        "success": True,
        "posted_count": videos_posted,
        "stats": stats,
    })
