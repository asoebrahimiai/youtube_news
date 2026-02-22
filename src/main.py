import os
import sys
import glob
import logging
import requests
import random
import subprocess
import time
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
    "Mechanical mechanisms animation",
    "Engineering gears satisfying",
    "CNC machining process short",
    "Thermodynamics experiment",
    "Fluid mechanics satisfying",
    "Robotics mechanical design",
    "manufacturing process satisfying",
    "hydraulic press machine",
    "metal lathe machining",
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
DELAY_BETWEEN_VIDEOS = 2  # ثانیه


# ─────────────────────────────────────────────
# Logger
# ─────────────────────────────────────────────

class QuietLogger:
    def debug(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass
    def info(self, msg): pass


# ─────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────

@contextmanager
def suppress_stderr():
    old = sys.stderr
    try:
        with open(os.devnull, 'w') as devnull:
            sys.stderr = devnull
            yield
    finally:
        sys.stderr = old


def cleanup_files(file_list: list) -> None:
    for f in file_list:
        try:
            if os.path.exists(f):
                os.remove(f)
        except OSError:
            pass


def get_env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(f"Missing env var: {key}")
    return val


def get_ytdlp_version(context) -> str:
    try:
        import yt_dlp.version as v
        version = v.__version__
        context.log(f"ℹ️  yt-dlp version: {version}")
        return version
    except Exception:
        try:
            result = subprocess.run(
                [sys.executable, '-m', 'yt_dlp', '--version'],
                capture_output=True, text=True, timeout=10
            )
            version = result.stdout.strip()
            context.log(f"ℹ️  yt-dlp version (subprocess): {version}")
            return version
        except Exception as e:
            context.log(f"⚠️  Cannot get yt-dlp version: {e}")
            return "unknown"


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
            logging.error(f"Appwrite write failed: {e}")
            return False


# ─────────────────────────────────────────────
# yt-dlp Strategies
# ─────────────────────────────────────────────

def build_strategies(cookie_path: str) -> list[tuple[str, dict]]:
    """
    استراتژی‌های مختلف برای دور زدن bot detection.
    هر استراتژی یک نام و یک dict از opts است.
    """
    cookie_exists = os.path.exists(cookie_path) if cookie_path else False

    base = {
        'quiet': True,
        'no_warnings': True,
        'logger': QuietLogger(),
        'noplaylist': True,
        'skip_download': True,
    }

    strategies = []

    # ── استراتژی ۱: WEB با cookie ─────────────
    # اگر cookie معتبر باشد، این بهترین گزینه است
    if cookie_exists:
        s = {
            **base,
            'cookiefile': cookie_path,
            'extractor_args': {
                'youtube': {
                    'player_client': ['web'],
                }
            },
            'http_headers': {
                'User-Agent': (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                ),
                'Accept-Language': 'en-US,en;q=0.9',
            },
        }
        strategies.append(('web_with_cookie', s))

    # ── استراتژی ۲: Android با cookie ──────────
    if cookie_exists:
        s = {
            **base,
            'cookiefile': cookie_path,
            'extractor_args': {
                'youtube': {
                    'player_client': ['android'],
                    'player_skip': ['webpage'],
                }
            },
            'http_headers': {
                'User-Agent': (
                    'com.google.android.youtube/19.09.37 '
                    '(Linux; U; Android 12; Pixel 6) gzip'
                ),
            },
        }
        strategies.append(('android_with_cookie', s))

    # ── استراتژی ۳: iOS با cookie ───────────────
    if cookie_exists:
        s = {
            **base,
            'cookiefile': cookie_path,
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
        strategies.append(('ios_with_cookie', s))

    # ── استراتژی ۴: tv_embedded (نیاز به cookie ندارد) ──
    s = {
        **base,
        'extractor_args': {
            'youtube': {
                'player_client': ['tv_embedded'],
            }
        },
    }
    if cookie_exists:
        s['cookiefile'] = cookie_path
    strategies.append(('tv_embedded', s))

    # ── استراتژی ۵: Android بدون cookie (آخرین تلاش) ──
    s = {
        **base,
        'extractor_args': {
            'youtube': {
                'player_client': ['android'],
                'player_skip': ['webpage', 'configs'],
            }
        },
        'http_headers': {
            'User-Agent': (
                'com.google.android.youtube/19.09.37 '
                '(Linux; U; Android 12) gzip'
            ),
        },
    }
    strategies.append(('android_no_cookie', s))

    return strategies


# ─────────────────────────────────────────────
# Video Info
# ─────────────────────────────────────────────

def get_video_info(
    video_url: str,
    strategies: list,
    context
) -> tuple[dict | None, dict | None]:
    """
    چند استراتژی را امتحان می‌کند.
    returns: (info_dict, working_base_opts) یا (None, None)
    """
    for name, opts in strategies:
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(video_url, download=False)
                if info and info.get('duration'):
                    context.log(f"   ✓ '{name}' OK — duration: {info['duration']}s")
                    # opts پایه برای دانلود (بدون skip_download)
                    base_for_dl = {
                        k: v for k, v in opts.items()
                        if k not in ('skip_download', 'logger')
                    }
                    return info, base_for_dl

        except yt_dlp.utils.DownloadError as e:
            msg = str(e)
            # خلاصه خطا
            if 'Sign in' in msg or 'bot' in msg.lower():
                short = 'IP blocked / bot detected'
            elif 'player response' in msg:
                short = 'outdated yt-dlp (update needed)'
            elif 'not available' in msg:
                short = 'format not available'
            elif 'Private video' in msg:
                short = 'private video'
            else:
                short = msg[:80]
            context.log(f"   ✗ '{name}': {short}")

        except Exception as e:
            context.log(f"   ✗ '{name}' exception: {type(e).__name__}: {str(e)[:60]}")

    return None, None


# ─────────────────────────────────────────────
# Download
# ─────────────────────────────────────────────

def download_video(
    video_url: str,
    video_id: str,
    base_opts: dict,
    context
) -> str | None:
    """دانلود ویدیو با استراتژی موفق."""

    # فرمت: ابتدا pre-merged، سپس best
    format_selector = (
        'bestvideo[ext=mp4]+bestaudio[ext=m4a]'
        '/best[ext=mp4][filesize<?50M]'
        '/best[ext=webm][filesize<?50M]'
        '/best[filesize<?50M]'
        '/best'
    )

    dl_opts = {
        **base_opts,
        'format': format_selector,
        'outtmpl': f'/tmp/{video_id}.%(ext)s',
        'overwrites': True,
        'logger': QuietLogger(),
        'quiet': True,
        'no_warnings': True,
    }

    try:
        with yt_dlp.YoutubeDL(dl_opts) as ydl:
            ydl.download([video_url])

    except yt_dlp.utils.DownloadError as e:
        msg = str(e)
        if 'ffmpeg' in msg.lower() or 'merger' in msg.lower():
            # fallback: فرمت pre-merged بدون ادغام
            context.log("   ↳ FFmpeg needed, trying pre-merged only...")
            return _download_premerged(video_url, video_id, base_opts, context)
        else:
            context.log(f"   ⚠️  Download failed: {msg[:100]}")
            cleanup_files(glob.glob(f"/tmp/{video_id}.*"))
            return None

    except Exception as e:
        context.log(f"   ⚠️  Unexpected: {str(e)[:80]}")
        cleanup_files(glob.glob(f"/tmp/{video_id}.*"))
        return None

    return _find_downloaded_file(video_id, context)


def _download_premerged(
    video_url: str,
    video_id: str,
    base_opts: dict,
    context
) -> str | None:
    """فرمت pre-merged (بدون نیاز به FFmpeg)."""
    dl_opts = {
        **base_opts,
        'format': 'best[ext=mp4][filesize<?50M]/best[filesize<?50M]/best',
        'outtmpl': f'/tmp/{video_id}.%(ext)s',
        'overwrites': True,
        'logger': QuietLogger(),
        'quiet': True,
    }
    try:
        with yt_dlp.YoutubeDL(dl_opts) as ydl:
            ydl.download([video_url])
    except Exception as e:
        context.log(f"   ⚠️  Pre-merged also failed: {str(e)[:80]}")
        cleanup_files(glob.glob(f"/tmp/{video_id}.*"))
        return None

    return _find_downloaded_file(video_id, context)


def _find_downloaded_file(video_id: str, context) -> str | None:
    downloaded = glob.glob(f"/tmp/{video_id}.*")
    valid = [
        f for f in downloaded
        if not f.endswith('.part')
        and os.path.exists(f)
        and os.path.getsize(f) > 0
        and os.path.getsize(f) <= MAX_FILE_SIZE_BYTES
    ]

    if not valid:
        context.log("   ⚠️  No valid file found after download")
        cleanup_files(downloaded)
        return None

    size_mb = os.path.getsize(valid[0]) / (1024 * 1024)
    context.log(f"   📁 File: {os.path.basename(valid[0])} ({size_mb:.1f} MB)")
    return valid[0]


# ─────────────────────────────────────────────
# Telegram
# ─────────────────────────────────────────────

def send_to_telegram(
    token: str,
    channel: str,
    file_path: str,
    title: str,
    video_url: str,
    context
) -> bool:
    api_url = f"https://api.telegram.org/bot{token}/sendVideo"
    caption = CAPTION_TEMPLATE.format(title=title, url=video_url)

    try:
        with open(file_path, 'rb') as vf:
            response = requests.post(
                api_url,
                data={
                    "chat_id": channel,
                    "caption": caption,
                    "parse_mode": "Markdown",
                    "supports_streaming": True,
                },
                files={"video": vf},
                timeout=120,
            )

        if response.status_code == 200:
            return True

        context.log(
            f"   ⚠️  Telegram HTTP {response.status_code}: "
            f"{response.text[:150]}"
        )
        return False

    except requests.RequestException as e:
        context.log(f"   ⚠️  Telegram error: {e}")
        return False


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main(context):
    context.log("⏰ Bot started")

    # ── env ──────────────────────────────────
    try:
        endpoint        = get_env("APPWRITE_ENDPOINT")
        project_id      = get_env("APPWRITE_PROJECT_ID")
        api_key         = get_env("APPWRITE_API_KEY")
        db_id           = get_env("APPWRITE_DATABASE_ID")
        collection_id   = get_env("APPWRITE_COLLECTION_ID")
        yt_api_key      = get_env("YOUTUBE_API_KEY")
        tg_token        = get_env("TELEGRAM_TOKEN")
        tg_channel      = get_env("TELEGRAM_CHANNEL")
    except EnvironmentError as e:
        context.error(str(e))
        return context.res.json({"success": False, "error": str(e)})

    # ── yt-dlp version ───────────────────────
    get_ytdlp_version(context)

    # ── Appwrite ─────────────────────────────
    client = Client()
    client.set_endpoint(endpoint)
    client.set_project(project_id)
    client.set_key(api_key)
    databases = Databases(client)

    # ── YouTube API ──────────────────────────
    youtube = build('youtube', 'v3', developerKey=yt_api_key)

    # ── جستجو ────────────────────────────────
    query = random.choice(SEARCH_QUERIES)
    context.log(f"🔍 Query: {query}")

    pub_after = (
        datetime.utcnow() - timedelta(days=SEARCH_LOOKBACK_DAYS)
    ).isoformat() + "Z"

    try:
        results = youtube.search().list(
            q=query,
            part='snippet',
            type='video',
            videoDuration='short',
            order='viewCount',
            publishedAfter=pub_after,
            maxResults=50
        ).execute()
    except Exception as e:
        context.error(f"YouTube API Error: {e}")
        return context.res.json({"success": False})

    items = results.get('items', [])
    context.log(f"📋 Found {len(items)} videos")

    # ── آماده‌سازی ────────────────────────────
    base_dir    = os.path.dirname(os.path.abspath(__file__))
    cookie_path = os.path.join(base_dir, 'cookies.txt')
    strategies  = build_strategies(cookie_path)

    cookie_exists = os.path.exists(cookie_path)
    context.log(
        f"🍪 cookies.txt: {'found' if cookie_exists else 'NOT FOUND'} | "
        f"Strategies: {[s[0] for s in strategies]}"
    )

    # ── پردازش ───────────────────────────────
    posted = 0
    stats  = {"dup": 0, "long": 0, "no_info": 0, "dl_fail": 0, "tg_fail": 0}

    for item in items:
        if posted >= MAX_POSTS_PER_RUN:
            break

        vid_id    = item['id']['videoId']
        title     = item['snippet']['title']
        video_url = f"https://www.youtube.com/watch?v={vid_id}"

        # ── تکراری؟ ──────────────────────────
        if is_video_duplicate(databases, db_id, collection_id, vid_id):
            stats["dup"] += 1
            continue

        context.log(f"\n▶️  {vid_id} — {title[:50]}")

        # ── اطلاعات ویدیو ─────────────────────
        info, working_opts = get_video_info(video_url, strategies, context)

        if not info:
            context.log("   ❌ All strategies failed — skipping")
            stats["no_info"] += 1
            time.sleep(DELAY_BETWEEN_VIDEOS)
            continue

        # ── مدت زمان ─────────────────────────
        dur = info.get('duration', 0)
        if dur == 0 or dur > MAX_DURATION_SECONDS:
            context.log(f"   ⏭️  Duration {dur}s — too long")
            stats["long"] += 1
            continue

        # ── دانلود ───────────────────────────
        file_path = download_video(video_url, vid_id, working_opts, context)

        if not file_path:
            stats["dl_fail"] += 1
            time.sleep(DELAY_BETWEEN_VIDEOS)
            continue

        # ── ارسال تلگرام ─────────────────────
        ok = send_to_telegram(tg_token, tg_channel, file_path, title, video_url, context)
        cleanup_files([file_path])

        if ok:
            register_video(databases, db_id, collection_id, vid_id)
            posted += 1
            context.log(f"   ✅ Posted ({posted}/{MAX_POSTS_PER_RUN})")
        else:
            stats["tg_fail"] += 1

        time.sleep(DELAY_BETWEEN_VIDEOS)

    # ── گزارش ────────────────────────────────
    context.log(
        f"\n📊 DONE | posted={posted} | "
        f"no_info={stats['no_info']} | dup={stats['dup']} | "
        f"long={stats['long']} | dl_fail={stats['dl_fail']} | "
        f"tg_fail={stats['tg_fail']}"
    )

    return context.res.json({"success": True, "posted": posted, "stats": stats})
