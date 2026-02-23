import os
import json
import base64
import logging
import asyncio
import tempfile
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone

from telegram import Bot
from telegram.error import TelegramError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# 1. CONFIG
# ─────────────────────────────────────────
TELEGRAM_BOT_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "")
YOUTUBE_SEARCH_QUERY = os.environ.get("YOUTUBE_SEARCH_QUERY", "AI news today")
MAX_VIDEOS   = int(os.environ.get("MAX_VIDEOS",   "5"))
MAX_DURATION = int(os.environ.get("MAX_DURATION", "300"))
MIN_DURATION = int(os.environ.get("MIN_DURATION", "30"))

# کوکی از متغیر محیطی (Base64) یا فایل
YOUTUBE_COOKIES_B64  = os.environ.get("YOUTUBE_COOKIES", "")
COOKIES_FILE_PATH    = os.environ.get("COOKIES_FILE_PATH", "/tmp/yt_cookies.txt")


# ─────────────────────────────────────────
# 2. INSTALL FFMPEG (Alpine)
# ─────────────────────────────────────────
def install_ffmpeg():
    """Install ffmpeg at runtime if not present (Alpine Linux)."""
    # Check if already installed
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            version_line = result.stdout.split("\n")[0]
            logger.info(f"✅ ffmpeg already installed: {version_line}")
            return True
    except FileNotFoundError:
        pass

    logger.info("📦 Installing ffmpeg via apk...")
    try:
        result = subprocess.run(
            ["apk", "add", "--no-cache", "ffmpeg"],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0:
            logger.info("✅ ffmpeg installed successfully")
            return True
        else:
            logger.error(f"❌ apk install failed:\n{result.stderr[:300]}")
    except FileNotFoundError:
        logger.warning("⚠️ apk not found — trying apt-get...")
        try:
            subprocess.run(
                ["apt-get", "update", "-qq"],
                capture_output=True, timeout=60
            )
            result = subprocess.run(
                ["apt-get", "install", "-y", "-qq", "ffmpeg"],
                capture_output=True, text=True, timeout=120
            )
            if result.returncode == 0:
                logger.info("✅ ffmpeg installed via apt-get")
                return True
            else:
                logger.error(f"❌ apt-get install failed:\n{result.stderr[:300]}")
        except Exception as e:
            logger.error(f"❌ apt-get error: {e}")
    except Exception as e:
        logger.error(f"❌ ffmpeg install error: {e}")

    return False


# ─────────────────────────────────────────
# 3. ENSURE LATEST yt-dlp
# ─────────────────────────────────────────
def ensure_latest_ytdlp():
    """Upgrade yt-dlp at runtime to avoid outdated errors."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--upgrade", "yt-dlp", "-q"],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0:
            logger.info("✅ yt-dlp upgraded successfully")
        else:
            logger.warning(f"⚠️ yt-dlp upgrade warning: {result.stderr[:200]}")
    except Exception as e:
        logger.warning(f"⚠️ Could not upgrade yt-dlp: {e}")

    try:
        import yt_dlp
        logger.info(f"📦 yt-dlp version: {yt_dlp.version.__version__}")
    except Exception:
        pass


# ─────────────────────────────────────────
# 4. COOKIES HELPER
# ─────────────────────────────────────────
def prepare_cookies() -> str | None:
    """
    Decode cookies from Base64 env var → /tmp/yt_cookies.txt
    یا از مسیر فایل مستقیم استفاده می‌کند.
    Returns path to cookies file or None.
    """
    # روش اول: Base64 از متغیر محیطی
    if YOUTUBE_COOKIES_B64:
        try:
            decoded = base64.b64decode(YOUTUBE_COOKIES_B64).decode("utf-8")
            with open(COOKIES_FILE_PATH, "w", encoding="utf-8") as f:
                f.write(decoded)
            size = os.path.getsize(COOKIES_FILE_PATH)
            logger.info(f"🍪 Cookies decoded from env var → {COOKIES_FILE_PATH} ({size} bytes)")
            return COOKIES_FILE_PATH
        except Exception as e:
            logger.error(f"❌ Failed to decode YOUTUBE_COOKIES: {e}")

    # روش دوم: فایل مستقیم
    if os.path.exists(COOKIES_FILE_PATH):
        size = os.path.getsize(COOKIES_FILE_PATH)
        if size > 100:
            logger.info(f"🍪 Using cookies file: {COOKIES_FILE_PATH} ({size} bytes)")
            return COOKIES_FILE_PATH
        else:
            logger.warning(f"⚠️ Cookies file too small ({size} bytes) — skipping")

    logger.warning("⚠️ No valid cookies found — proceeding without cookies")
    return None


def get_cookie_opts(cookie_path: str | None) -> dict:
    if cookie_path:
        return {"cookiefile": cookie_path}
    return {}


# ─────────────────────────────────────────
# 5. SEARCH YOUTUBE
# ─────────────────────────────────────────
def search_youtube(query: str, cookie_path: str | None, max_results: int = 30) -> list[str]:
    """Search YouTube and return list of video IDs."""
    import yt_dlp

    cookie_opts = get_cookie_opts(cookie_path)

    ydl_opts = {
        "quiet": True,
        "no_warnings": False,
        "extract_flat": True,
        "playlistend": max_results,
        **cookie_opts,
        "extractor_args": {
            "youtube": {
                "player_client": ["tv_embedded"],
            }
        },
    }

    search_url = f"ytsearch{max_results}:{query}"
    video_ids = []

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(search_url, download=False)
            if info and "entries" in info:
                for entry in info["entries"]:
                    if entry and entry.get("id"):
                        video_ids.append(entry["id"])
        logger.info(f"🔍 Found {len(video_ids)} videos for: '{query}'")
    except Exception as e:
        logger.error(f"❌ Search failed: {e}")

    return video_ids


# ─────────────────────────────────────────
# 6. GET VIDEO INFO — MULTI STRATEGY
# ─────────────────────────────────────────
def get_video_info(video_id: str, cookie_path: str | None) -> dict | None:
    """Try multiple strategies to extract video metadata."""
    import yt_dlp

    url = f"https://www.youtube.com/watch?v={video_id}"
    cookie_opts = get_cookie_opts(cookie_path)

    strategies = [
        {
            "name": "tv_embedded",
            "opts": {
                "quiet": True,
                "extractor_args": {
                    "youtube": {
                        "player_client": ["tv_embedded"],
                        "skip": ["dash", "hls"],
                    }
                },
            }
        },
        {
            "name": "android_cookie",
            "opts": {
                "quiet": True,
                **cookie_opts,
                "extractor_args": {
                    "youtube": {
                        "player_client": ["android"],
                        "skip": ["dash", "hls"],
                    }
                },
            }
        },
        {
            "name": "ios_cookie",
            "opts": {
                "quiet": True,
                **cookie_opts,
                "extractor_args": {
                    "youtube": {
                        "player_client": ["ios"],
                        "skip": ["dash"],
                    }
                },
            }
        },
        {
            "name": "web_cookie",
            "opts": {
                "quiet": True,
                **cookie_opts,
                "extractor_args": {
                    "youtube": {
                        "player_client": ["web"],
                    }
                },
            }
        },
        {
            "name": "mweb_cookie",
            "opts": {
                "quiet": True,
                **cookie_opts,
                "extractor_args": {
                    "youtube": {
                        "player_client": ["mweb"],
                        "skip": ["dash"],
                    }
                },
            }
        },
    ]

    for strategy in strategies:
        name = strategy["name"]
        opts = strategy["opts"]
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)

            if not info:
                logger.debug(f"  [{name}] No info returned")
                continue

            duration = info.get("duration", 0) or 0

            if duration < MIN_DURATION or duration > MAX_DURATION:
                logger.info(
                    f"  [{name}] ⏱️ Duration {duration}s out of range "
                    f"[{MIN_DURATION}–{MAX_DURATION}]"
                )
                return None  # بقیه استراتژی‌ها فایده ندارند

            logger.info(
                f"  [{name}] ✅ '{info.get('title', 'N/A')}' ({duration}s)"
            )
            return {
                "id":                  video_id,
                "url":                 url,
                "title":               info.get("title", "No Title"),
                "duration":            duration,
                "uploader":            info.get("uploader", "Unknown"),
                "view_count":          info.get("view_count", 0),
                "description":         (info.get("description") or "")[:500],
                "successful_strategy": name,
            }

        except Exception as e:
            err = str(e).lower()
            if "sign in" in err or "bot" in err:
                logger.warning(f"  [{name}] 🤖 Bot detected")
            elif "private" in err:
                logger.warning(f"  [{name}] 🔒 Private video")
            elif "unavailable" in err:
                logger.warning(f"  [{name}] ❌ Unavailable")
            elif "outdated" in err or "update" in err:
                logger.warning(f"  [{name}] 📦 yt-dlp outdated")
            else:
                logger.debug(f"  [{name}] Error: {str(e)[:120]}")

    logger.warning(f"⚠️ All strategies failed for {video_id}")
    return None


# ─────────────────────────────────────────
# 7. DOWNLOAD VIDEO
# ─────────────────────────────────────────
def download_video(video_info: dict, output_dir: str, cookie_path: str | None) -> str | None:
    """Download video file to output_dir and return file path."""
    import yt_dlp

    url           = video_info["url"]
    cookie_opts   = get_cookie_opts(cookie_path)
    strategy_name = video_info.get("successful_strategy", "android_cookie")

    output_template = os.path.join(output_dir, "%(id)s.%(ext)s")

    # بهترین کیفیت زیر ۴۵ مگابایت
    format_selector = (
        "bestvideo[ext=mp4][height<=720][filesize<45M]"
        "+bestaudio[ext=m4a]"
        "/best[ext=mp4][height<=720][filesize<45M]"
        "/best[height<=480]"
        "/best"
    )

    strategy_extra: dict = {
        "tv_embedded":   {
            "extractor_args": {
                "youtube": {"player_client": ["tv_embedded"], "skip": ["dash", "hls"]}
            }
        },
        "android_cookie": {
            **cookie_opts,
            "extractor_args": {"youtube": {"player_client": ["android"]}}
        },
        "ios_cookie": {
            **cookie_opts,
            "extractor_args": {"youtube": {"player_client": ["ios"]}}
        },
        "web_cookie": {
            **cookie_opts,
            "extractor_args": {"youtube": {"player_client": ["web"]}}
        },
        "mweb_cookie": {
            **cookie_opts,
            "extractor_args": {"youtube": {"player_client": ["mweb"]}}
        },
    }

    extra = strategy_extra.get(strategy_name, {**cookie_opts})

    ydl_opts = {
        "format":               format_selector,
        "outtmpl":              output_template,
        "quiet":                False,
        "no_warnings":          False,
        "merge_output_format":  "mp4",
        "postprocessors": [{
            "key":             "FFmpegVideoConvertor",
            "preferedformat":  "mp4",
        }],
        **extra,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

        # جستجوی فایل دانلود شده
        for ext in ["mp4", "mkv", "webm", "m4v"]:
            for f in Path(output_dir).glob(f"*.{ext}"):
                size_mb = f.stat().st_size / (1024 * 1024)
                logger.info(f"📥 Downloaded: {f.name} ({size_mb:.1f} MB)")
                return str(f)

    except Exception as e:
        logger.error(f"❌ Download failed: {str(e)[:200]}")

    return None


# ─────────────────────────────────────────
# 8. POST TO TELEGRAM
# ─────────────────────────────────────────
async def post_to_telegram(video_info: dict, video_path: str) -> bool:
    """Upload video file to Telegram channel."""
    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    mins, secs = divmod(video_info["duration"], 60)
    caption = (
        f"🎬 *{video_info['title']}*\n\n"
        f"👤 {video_info['uploader']}\n"
        f"⏱️ {mins}:{secs:02d}\n"
        f"👁️ {video_info.get('view_count', 0):,} views\n\n"
        f"🔗 [Watch on YouTube]({video_info['url']})"
    )

    size_mb = os.path.getsize(video_path) / (1024 * 1024)
    logger.info(f"📤 Uploading to Telegram ({size_mb:.1f} MB)…")

    try:
        with open(video_path, "rb") as vf:
            await bot.send_video(
                chat_id=TELEGRAM_CHANNEL_ID,
                video=vf,
                caption=caption,
                parse_mode="Markdown",
                supports_streaming=True,
                read_timeout=300,
                write_timeout=300,
                connect_timeout=60,
            )
        logger.info(f"✅ Posted: {video_info['title']}")
        return True
    except TelegramError as e:
        logger.error(f"❌ Telegram error: {e}")
        return False


# ─────────────────────────────────────────
# 9. HISTORY
# ─────────────────────────────────────────
HISTORY_FILE = "/tmp/posted_videos.json"


def load_history() -> set:
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE) as f:
                return set(json.load(f))
        except Exception:
            pass
    return set()


def save_history(history: set):
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(list(history), f)
    except Exception as e:
        logger.warning(f"⚠️ Could not save history: {e}")


# ─────────────────────────────────────────
# 10. MAIN
# ─────────────────────────────────────────
async def main():
    logger.info("=" * 60)
    logger.info("🚀 YouTube → Telegram Bot Started")
    logger.info(f"📅 {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)

    # ── مرحله ۱: نصب ffmpeg ──────────────────────────────────
    ffmpeg_ok = install_ffmpeg()
    if not ffmpeg_ok:
        logger.warning("⚠️ ffmpeg not available — merged/converted videos may fail")

    # ── مرحله ۲: به‌روزرسانی yt-dlp ─────────────────────────
    ensure_latest_ytdlp()

    # ── مرحله ۳: اعتبارسنجی config ──────────────────────────
    if not TELEGRAM_BOT_TOKEN:
        logger.error("❌ TELEGRAM_BOT_TOKEN not set!")
        return {"error": "Missing TELEGRAM_BOT_TOKEN"}

    if not TELEGRAM_CHANNEL_ID:
        logger.error("❌ TELEGRAM_CHANNEL_ID not set!")
        return {"error": "Missing TELEGRAM_CHANNEL_ID"}

    # ── مرحله ۴: آماده‌سازی کوکی ────────────────────────────
    cookie_path = prepare_cookies()

    # ── مرحله ۵: بارگذاری تاریخچه ───────────────────────────
    posted_history = load_history()
    logger.info(f"📋 Already posted: {len(posted_history)} videos")

    # ── مرحله ۶: جستجو ──────────────────────────────────────
    logger.info(f"🔍 Searching: '{YOUTUBE_SEARCH_QUERY}'")
    video_ids = search_youtube(YOUTUBE_SEARCH_QUERY, cookie_path, max_results=30)

    if not video_ids:
        logger.error("❌ No videos found")
        return {"error": "No videos found"}

    new_ids = [v for v in video_ids if v not in posted_history]
    logger.info(f"🆕 New videos: {len(new_ids)}/{len(video_ids)}")

    stats = {
        "posted":   0,
        "no_info":  0,
        "dl_fail":  0,
        "tg_fail":  0,
    }

    # ── مرحله ۷: پردازش ویدیوها ─────────────────────────────
    for video_id in new_ids:
        if stats["posted"] >= MAX_VIDEOS:
            logger.info(f"✅ Reached target of {MAX_VIDEOS} posts")
            break

        logger.info(f"\n{'─' * 40}")
        logger.info(f"🎬 Processing: https://youtu.be/{video_id}")

        video_info = get_video_info(video_id, cookie_path)

        if not video_info:
            stats["no_info"] += 1
            continue

        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = download_video(video_info, tmpdir, cookie_path)

            if not video_path:
                stats["dl_fail"] += 1
                logger.error(f"❌ Download failed: {video_id}")
                continue

            success = await post_to_telegram(video_info, video_path)

        if success:
            stats["posted"] += 1
            posted_history.add(video_id)
            save_history(posted_history)
        else:
            stats["tg_fail"] += 1

    # ── گزارش نهایی ─────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("📊 FINAL STATS:")
    logger.info(f"   ✅ Posted:         {stats['posted']}")
    logger.info(f"   📦 No info:        {stats['no_info']}")
    logger.info(f"   ❌ Download fail:  {stats['dl_fail']}")
    logger.info(f"   📱 Telegram fail:  {stats['tg_fail']}")
    logger.info("=" * 60)

    return stats


# ─────────────────────────────────────────
# 11. APPWRITE ENTRY POINT
# ─────────────────────────────────────────
def main_handler(context):
    """Appwrite Function entry point."""
    result = asyncio.run(main())
    return context.res.json(result)
