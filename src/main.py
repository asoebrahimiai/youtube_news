import os
import json
import logging
import asyncio
import tempfile
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone

from telegram import Bot, InputMediaVideo
from telegram.error import TelegramError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# 1. CONFIG
# ─────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "")
YOUTUBE_SEARCH_QUERY = os.environ.get("YOUTUBE_SEARCH_QUERY", "AI news today")
MAX_VIDEOS = int(os.environ.get("MAX_VIDEOS", "5"))
MAX_DURATION = int(os.environ.get("MAX_DURATION", "300"))
MIN_DURATION = int(os.environ.get("MIN_DURATION", "30"))
COOKIES_FILE = os.environ.get("COOKIES_FILE_PATH", "/usr/local/server/function/cookies.txt")


# ─────────────────────────────────────────
# 2. ENSURE LATEST yt-dlp
# ─────────────────────────────────────────
def ensure_latest_ytdlp():
    """Force update yt-dlp at runtime if needed."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--upgrade", "yt-dlp", "-q"],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0:
            logger.info("✅ yt-dlp updated successfully")
        else:
            logger.warning(f"⚠️ yt-dlp update warning: {result.stderr[:200]}")
    except Exception as e:
        logger.warning(f"⚠️ Could not update yt-dlp: {e}")

    # Log current version
    try:
        import yt_dlp
        logger.info(f"📦 yt-dlp version: {yt_dlp.version.__version__}")
    except Exception:
        pass


# ─────────────────────────────────────────
# 3. COOKIES HELPER
# ─────────────────────────────────────────
def get_cookie_opts():
    """Return cookie options if cookies.txt exists and is valid."""
    if os.path.exists(COOKIES_FILE):
        size = os.path.getsize(COOKIES_FILE)
        if size > 100:
            logger.info(f"🍪 Using cookies from: {COOKIES_FILE} ({size} bytes)")
            return {"cookiefile": COOKIES_FILE}
        else:
            logger.warning(f"⚠️ cookies.txt too small ({size} bytes) - skipping")
    else:
        logger.warning(f"⚠️ No cookies file found at: {COOKIES_FILE}")
    return {}


# ─────────────────────────────────────────
# 4. SEARCH YOUTUBE
# ─────────────────────────────────────────
def search_youtube(query: str, max_results: int = 20) -> list[str]:
    """Search YouTube and return list of video IDs."""
    import yt_dlp

    cookie_opts = get_cookie_opts()

    ydl_opts = {
        "quiet": True,
        "no_warnings": False,
        "extract_flat": True,
        "playlistend": max_results,
        **cookie_opts,
        "extractor_args": {
            "youtube": {
                "player_client": ["android"],
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
        logger.info(f"🔍 Found {len(video_ids)} videos for query: '{query}'")
    except Exception as e:
        logger.error(f"❌ Search failed: {e}")

    return video_ids


# ─────────────────────────────────────────
# 5. GET VIDEO INFO - MULTI STRATEGY
# ─────────────────────────────────────────
def get_video_info(video_id: str) -> dict | None:
    """
    Try multiple strategies to get video info.
    Returns dict with title, duration, uploader, etc.
    """
    import yt_dlp

    url = f"https://www.youtube.com/watch?v={video_id}"
    cookie_opts = get_cookie_opts()

    strategies = [
        # Strategy 1: TV Embedded (no login required, often bypasses bot check)
        {
            "name": "tv_embedded",
            "opts": {
                "quiet": True,
                "no_warnings": False,
                "extractor_args": {
                    "youtube": {
                        "player_client": ["tv_embedded"],
                        "skip": ["dash", "hls"],
                    }
                },
            }
        },
        # Strategy 2: Android with cookies
        {
            "name": "android_cookie",
            "opts": {
                "quiet": True,
                "no_warnings": False,
                **cookie_opts,
                "extractor_args": {
                    "youtube": {
                        "player_client": ["android"],
                        "skip": ["dash", "hls"],
                    }
                },
            }
        },
        # Strategy 3: iOS with cookies
        {
            "name": "ios_cookie",
            "opts": {
                "quiet": True,
                "no_warnings": False,
                **cookie_opts,
                "extractor_args": {
                    "youtube": {
                        "player_client": ["ios"],
                        "skip": ["dash"],
                    }
                },
            }
        },
        # Strategy 4: Web with cookies
        {
            "name": "web_cookie",
            "opts": {
                "quiet": True,
                "no_warnings": False,
                **cookie_opts,
                "extractor_args": {
                    "youtube": {
                        "player_client": ["web"],
                    }
                },
            }
        },
        # Strategy 5: mweb (mobile web)
        {
            "name": "mweb_cookie",
            "opts": {
                "quiet": True,
                "no_warnings": False,
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
                        f"[{MIN_DURATION}-{MAX_DURATION}]"
                    )
                    return None  # No need to try other strategies

                logger.info(f"  [{name}] ✅ Got info: '{info.get('title', 'N/A')}' ({duration}s)")
                return {
                    "id": video_id,
                    "url": url,
                    "title": info.get("title", "No Title"),
                    "duration": duration,
                    "uploader": info.get("uploader", "Unknown"),
                    "view_count": info.get("view_count", 0),
                    "description": (info.get("description") or "")[:500],
                    "successful_strategy": name,
                }

        except Exception as e:
            err_str = str(e).lower()
            if "sign in" in err_str or "bot" in err_str:
                logger.warning(f"  [{name}] 🤖 Bot detected")
            elif "private" in err_str:
                logger.warning(f"  [{name}] 🔒 Private video")
            elif "unavailable" in err_str:
                logger.warning(f"  [{name}] ❌ Video unavailable")
            elif "outdated" in err_str or "update" in err_str:
                logger.warning(f"  [{name}] 📦 yt-dlp outdated - update needed!")
            else:
                logger.debug(f"  [{name}] Error: {str(e)[:100]}")

    logger.warning(f"⚠️ All strategies failed for {video_id}")
    return None


# ─────────────────────────────────────────
# 6. DOWNLOAD VIDEO
# ─────────────────────────────────────────
def download_video(video_info: dict, output_dir: str) -> str | None:
    """Download video using the strategy that succeeded for info."""
    import yt_dlp

    url = video_info["url"]
    cookie_opts = get_cookie_opts()
    strategy_name = video_info.get("successful_strategy", "android_cookie")

    output_template = os.path.join(output_dir, "%(id)s.%(ext)s")

    # Format: best MP4 under 50MB (Telegram limit for bot API)
    format_selector = (
        "bestvideo[ext=mp4][height<=720][filesize<45M]"
        "+bestaudio[ext=m4a]/best[ext=mp4][height<=720][filesize<45M]"
        "/best[height<=480]/best"
    )

    # Build opts based on successful strategy
    strategy_map = {
        "tv_embedded": {
            "extractor_args": {
                "youtube": {
                    "player_client": ["tv_embedded"],
                    "skip": ["dash", "hls"],
                }
            }
        },
        "android_cookie": {
            **cookie_opts,
            "extractor_args": {
                "youtube": {
                    "player_client": ["android"],
                }
            }
        },
        "ios_cookie": {
            **cookie_opts,
            "extractor_args": {
                "youtube": {
                    "player_client": ["ios"],
                }
            }
        },
        "web_cookie": {
            **cookie_opts,
            "extractor_args": {
                "youtube": {
                    "player_client": ["web"],
                }
            }
        },
        "mweb_cookie": {
            **cookie_opts,
            "extractor_args": {
                "youtube": {
                    "player_client": ["mweb"],
                }
            }
        },
    }

    extra_opts = strategy_map.get(strategy_name, {**cookie_opts})

    ydl_opts = {
        "format": format_selector,
        "outtmpl": output_template,
        "quiet": False,
        "no_warnings": False,
        "merge_output_format": "mp4",
        "postprocessors": [{
            "key": "FFmpegVideoConvertor",
            "preferedformat": "mp4",
        }],
        **extra_opts,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

        # Find downloaded file
        for f in Path(output_dir).glob("*.mp4"):
            if video_info["id"] in f.name:
                size_mb = f.stat().st_size / (1024 * 1024)
                logger.info(f"📥 Downloaded: {f.name} ({size_mb:.1f} MB)")
                return str(f)

        # Try any video file
        for ext in ["mp4", "mkv", "webm", "m4v"]:
            for f in Path(output_dir).glob(f"*.{ext}"):
                size_mb = f.stat().st_size / (1024 * 1024)
                logger.info(f"📥 Downloaded: {f.name} ({size_mb:.1f} MB)")
                return str(f)

    except Exception as e:
        logger.error(f"❌ Download failed: {str(e)[:200]}")

    return None


# ─────────────────────────────────────────
# 7. POST TO TELEGRAM
# ─────────────────────────────────────────
async def post_to_telegram(video_info: dict, video_path: str) -> bool:
    """Send video to Telegram channel."""
    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    caption = (
        f"🎬 *{video_info['title']}*\n\n"
        f"👤 {video_info['uploader']}\n"
        f"⏱️ {video_info['duration'] // 60}:{video_info['duration'] % 60:02d}\n"
        f"👁️ {video_info.get('view_count', 0):,} views\n\n"
        f"🔗 [Watch on YouTube]({video_info['url']})"
    )

    file_size = os.path.getsize(video_path)
    logger.info(f"📤 Uploading to Telegram ({file_size / 1024 / 1024:.1f} MB)...")

    try:
        with open(video_path, "rb") as video_file:
            await bot.send_video(
                chat_id=TELEGRAM_CHANNEL_ID,
                video=video_file,
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
# 8. HISTORY MANAGEMENT
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
        logger.warning(f"Could not save history: {e}")


# ─────────────────────────────────────────
# 9. MAIN FUNCTION
# ─────────────────────────────────────────
async def main():
    logger.info("=" * 60)
    logger.info("🚀 YouTube → Telegram Bot Started")
    logger.info(f"📅 {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)

    # Step 0: Ensure latest yt-dlp
    ensure_latest_ytdlp()

    # Validate config
    if not TELEGRAM_BOT_TOKEN:
        logger.error("❌ TELEGRAM_BOT_TOKEN not set!")
        return {"error": "Missing TELEGRAM_BOT_TOKEN"}

    if not TELEGRAM_CHANNEL_ID:
        logger.error("❌ TELEGRAM_CHANNEL_ID not set!")
        return {"error": "Missing TELEGRAM_CHANNEL_ID"}

    # Load history
    posted_history = load_history()
    logger.info(f"📋 Already posted: {len(posted_history)} videos")

    # Search
    logger.info(f"🔍 Searching: '{YOUTUBE_SEARCH_QUERY}'")
    video_ids = search_youtube(YOUTUBE_SEARCH_QUERY, max_results=30)

    if not video_ids:
        logger.error("❌ No videos found in search")
        return {"error": "No videos found"}

    # Filter already posted
    new_ids = [vid for vid in video_ids if vid not in posted_history]
    logger.info(f"🆕 New videos to process: {len(new_ids)}/{len(video_ids)}")

    stats = {"posted": 0, "skipped_duration": 0, "no_info": 0, "dl_fail": 0, "tg_fail": 0}

    for video_id in new_ids:
        if stats["posted"] >= MAX_VIDEOS:
            logger.info(f"✅ Reached target of {MAX_VIDEOS} posts")
            break

        logger.info(f"\n{'─' * 40}")
        logger.info(f"🎬 Processing: {video_id}")
        logger.info(f"   https://www.youtube.com/watch?v={video_id}")

        # Get info
        video_info = get_video_info(video_id)

        if not video_info:
            stats["no_info"] += 1
            logger.info(f"⏭️ Skipping {video_id} (no info / out of range)")
            continue

        # Download
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = download_video(video_info, tmpdir)

            if not video_path:
                stats["dl_fail"] += 1
                logger.error(f"❌ Download failed for {video_id}")
                continue

            # Post to Telegram
            success = await post_to_telegram(video_info, video_path)

            if success:
                stats["posted"] += 1
                posted_history.add(video_id)
                save_history(posted_history)
                logger.info(f"✅ Successfully posted {video_id}")
            else:
                stats["tg_fail"] += 1

    logger.info("\n" + "=" * 60)
    logger.info("📊 FINAL STATS:")
    logger.info(f"   ✅ Posted:           {stats['posted']}")
    logger.info(f"   📦 No info/filtered: {stats['no_info']}")
    logger.info(f"   ❌ Download failed:  {stats['dl_fail']}")
    logger.info(f"   📱 Telegram failed:  {stats['tg_fail']}")
    logger.info("=" * 60)

    return stats


# ─────────────────────────────────────────
# 10. APPWRITE ENTRY POINT
# ─────────────────────────────────────────
def main_handler(context):
    """Appwrite Function entry point."""
    result = asyncio.run(main())
    return context.res.json(result)