import asyncio
import logging
import os
import sys
import tempfile
import json
import base64
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import yt_dlp
import requests

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── Config از محیط ───────────────────────────────────────────
TELEGRAM_BOT_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHANNEL_ID  = os.environ.get("TELEGRAM_CHANNEL", "")
YOUTUBE_SEARCH_QUERY = os.environ.get("YOUTUBE_SEARCH_QUERY", "python tutorial")
YOUTUBE_COOKIES_B64  = os.environ.get("YOUTUBE_COOKIES", "")
MAX_VIDEOS           = int(os.environ.get("MAX_VIDEOS", "3"))
HISTORY_FILE         = "/tmp/posted_history.json"

# ── آماده‌سازی کوکی ──────────────────────────────────────────
def prepare_cookies() -> str | None:
    if not YOUTUBE_COOKIES_B64:
        logger.info("ℹ️ No YOUTUBE_COOKIES env var — proceeding without cookies")
        return None
    try:
        cookie_path = "/tmp/yt_cookies.txt"
        data = base64.b64decode(YOUTUBE_COOKIES_B64)
        with open(cookie_path, "wb") as f:
            f.write(data)
        logger.info(f"✅ Cookies written to {cookie_path}")
        return cookie_path
    except Exception as e:
        logger.error(f"❌ Cookie decode error: {e}")
        return None

# ── به‌روزرسانی yt-dlp ────────────────────────────────────────
def ensure_latest_ytdlp():
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--upgrade", "yt-dlp", "-q"],
            capture_output=True, text=True, timeout=60
        )
        logger.info("✅ yt-dlp updated")
    except Exception as e:
        logger.warning(f"⚠️ yt-dlp update skipped: {e}")

# ── تاریخچه ──────────────────────────────────────────────────
def load_history() -> set:
    try:
        with open(HISTORY_FILE, "r") as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_history(history: set):
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(list(history), f)
    except Exception as e:
        logger.error(f"❌ Save history error: {e}")

# ── جستجوی یوتیوب ────────────────────────────────────────────
def search_youtube(query: str, cookie_path: str | None, max_results: int = 20) -> list[str]:
    ydl_opts = {
        "quiet":          True,
        "no_warnings":    True,
        "extract_flat":   True,
        "playlistend":    max_results,
        "socket_timeout": 30,
    }
    if cookie_path:
        ydl_opts["cookiefile"] = cookie_path

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            result = ydl.extract_info(f"ytsearch{max_results}:{query}", download=False)
            if result and "entries" in result:
                ids = [e["id"] for e in result["entries"] if e and "id" in e]
                logger.info(f"✅ Found {len(ids)} videos")
                return ids
    except Exception as e:
        logger.error(f"❌ Search error: {e}")
    return []

# ── دریافت اطلاعات ویدیو ─────────────────────────────────────
def get_video_info(video_id: str, cookie_path: str | None) -> dict | None:
    ydl_opts = {
        "quiet":          True,
        "no_warnings":    True,
        "socket_timeout": 30,
    }
    if cookie_path:
        ydl_opts["cookiefile"] = cookie_path

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(
                f"https://www.youtube.com/watch?v={video_id}",
                download=False
            )
            return {
                "id":          info.get("id"),
                "title":       info.get("title", "No Title"),
                "description": (info.get("description") or "")[:800],
                "duration":    info.get("duration", 0),
                "view_count":  info.get("view_count", 0),
                "uploader":    info.get("uploader", "Unknown"),
                "webpage_url": info.get("webpage_url"),
            }
    except Exception as e:
        logger.error(f"❌ Info error [{video_id}]: {e}")
        return None

# ── دانلود ویدیو — بدون FFmpeg ───────────────────────────────
def download_video(video_info: dict, tmpdir: str, cookie_path: str | None) -> str | None:
    video_id = video_info["id"]
    out_tmpl  = os.path.join(tmpdir, "%(id)s.%(ext)s")

    FORMAT_STRATEGIES = [
        "best[ext=mp4][filesize<50M]",
        "best[filesize<50M]",
        "best",
        "worst",
    ]

    base_opts = {
        "outtmpl":          out_tmpl,
        "quiet":            False,
        "no_warnings":      False,
        "socket_timeout":   60,
        "postprocessors":   [],
        "nopostoverwrites": True,
    }
    if cookie_path:
        base_opts["cookiefile"] = cookie_path

    for fmt in FORMAT_STRATEGIES:
        logger.info(f"🎯 Trying format: {fmt}")
        opts = {**base_opts, "format": fmt}

        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([f"https://www.youtube.com/watch?v={video_id}"])

            for f in Path(tmpdir).iterdir():
                if f.name.startswith(video_id):
                    size_mb = f.stat().st_size / (1024 * 1024)
                    logger.info(f"✅ Downloaded: {f.name} ({size_mb:.1f} MB)")
                    if size_mb > 50:
                        logger.warning(f"⚠️ File too large ({size_mb:.1f} MB) — skipping")
                        f.unlink()
                        continue
                    return str(f)

        except Exception as e:
            logger.warning(f"⚠️ Format '{fmt}' failed: {e}")
            for f in Path(tmpdir).iterdir():
                f.unlink(missing_ok=True)
            continue

    logger.error(f"❌ All format strategies failed for {video_id}")
    return None

# ── ارسال به تلگرام ──────────────────────────────────────────
async def post_to_telegram(video_info: dict, video_path: str) -> bool:
    caption = (
        f"🎬 *{video_info['title']}*\n\n"
        f"👤 {video_info['uploader']}\n"
        f"👁 {video_info['view_count']:,} views\n"
        f"⏱ {video_info['duration'] // 60}:{video_info['duration'] % 60:02d}\n\n"
        f"{video_info['description']}\n\n"
        f"🔗 {video_info['webpage_url']}"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendVideo"

    try:
        with open(video_path, "rb") as vf:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.post(
                    url,
                    data={
                        "chat_id":            TELEGRAM_CHANNEL_ID,
                        "caption":            caption[:1024],
                        "parse_mode":         "Markdown",
                        "supports_streaming": True,
                    },
                    files={"video": vf},
                    timeout=120,
                )
            )

        if response.status_code == 200:
            logger.info(f"✅ Posted to Telegram: {video_info['title']}")
            return True
        else:
            logger.error(f"❌ Telegram error: {response.status_code} — {response.text[:200]}")
            return False

    except Exception as e:
        logger.error(f"❌ Telegram post exception: {e}")
        return False

# ── تابع اصلی ────────────────────────────────────────────────
async def main(context):
    logger.info("=" * 60)
    logger.info("🚀 YouTube → Telegram Bot Started")
    logger.info(f"📅 {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)

    ensure_latest_ytdlp()

    # بررسی متغیرهای محیطی
    logger.info(f"🔑 TOKEN set: {bool(TELEGRAM_BOT_TOKEN)} | len={len(TELEGRAM_BOT_TOKEN)}")
    logger.info(f"📢 CHANNEL set: {bool(TELEGRAM_CHANNEL_ID)} | value='{TELEGRAM_CHANNEL_ID}'")
    logger.info(f"🔍 QUERY: '{YOUTUBE_SEARCH_QUERY}'")
    logger.info(f"🍪 COOKIES set: {bool(YOUTUBE_COOKIES_B64)}")
    logger.info(f"🎬 MAX_VIDEOS: {MAX_VIDEOS}")

    # اعتبارسنجی
    if not TELEGRAM_BOT_TOKEN:
        logger.error("❌ TELEGRAM_BOT_TOKEN is empty!")
        return context.res.json({"error": "Missing TELEGRAM_BOT_TOKEN"})

    if not TELEGRAM_CHANNEL_ID:
        logger.error("❌ TELEGRAM_CHANNEL_ID is empty!")
        return context.res.json({"error": "Missing TELEGRAM_CHANNEL_ID"})

    # تست اتصال تلگرام
    logger.info("🔌 Testing Telegram connection...")
    try:
        test_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe"
        test_resp = requests.get(test_url, timeout=10)
        if test_resp.status_code == 200:
            bot_name = test_resp.json().get("result", {}).get("username", "unknown")
            logger.info(f"✅ Telegram OK — Bot: @{bot_name}")
        else:
            logger.error(f"❌ Telegram auth failed: {test_resp.status_code} — {test_resp.text[:200]}")
            return context.res.json({"error": "Telegram auth failed", "detail": test_resp.text[:200]})
    except Exception as e:
        logger.error(f"❌ Telegram connection error: {e}")
        return context.res.json({"error": f"Telegram connection error: {e}"})

    # کوکی
    cookie_path = prepare_cookies()
    logger.info(f"🍪 Cookie path: {cookie_path}")

    # تاریخچه
    posted_history = load_history()
    logger.info(f"📋 Already posted: {len(posted_history)} videos")

    # جستجو
    logger.info(f"🔍 Starting search for: '{YOUTUBE_SEARCH_QUERY}'")
    video_ids = search_youtube(YOUTUBE_SEARCH_QUERY, cookie_path, max_results=30)
    logger.info(f"📦 Search result count: {len(video_ids)}")

    if not video_ids:
        logger.error("❌ No videos found — search returned empty")
        return context.res.json({"error": "No videos found"})

    new_ids = [v for v in video_ids if v not in posted_history]
    logger.info(f"🆕 New videos: {len(new_ids)}/{len(video_ids)}")

    if not new_ids:
        logger.info("ℹ️ All videos already posted — nothing to do")
        return context.res.json({"info": "All videos already posted"})

    stats = {"posted": 0, "no_info": 0, "dl_fail": 0, "tg_fail": 0}

    for video_id in new_ids:
        if stats["posted"] >= MAX_VIDEOS:
            logger.info(f"✅ Reached target of {MAX_VIDEOS} posts")
            break

        logger.info(f"\n{'─' * 40}")
        logger.info(f"🎬 Processing: https://youtu.be/{video_id}")

        video_info = get_video_info(video_id, cookie_path)
        if not video_info:
            logger.error(f"❌ No info for: {video_id}")
            stats["no_info"] += 1
            continue

        logger.info(f"📝 Title: {video_info['title']}")
        logger.info(f"⏱ Duration: {video_info['duration']}s")

        with tempfile.TemporaryDirectory() as tmpdir:
            logger.info(f"📥 Downloading to: {tmpdir}")
            video_path = download_video(video_info, tmpdir, cookie_path)

            if not video_path:
                logger.error(f"❌ Download failed: {video_id}")
                stats["dl_fail"] += 1
                continue

            logger.info(f"📤 Sending to Telegram...")
            success = await post_to_telegram(video_info, video_path)

        if success:
            stats["posted"] += 1
            posted_history.add(video_id)
            save_history(posted_history)
            logger.info(f"✅ Successfully posted: {video_info['title']}")
        else:
            stats["tg_fail"] += 1
            logger.error(f"❌ Telegram post failed: {video_id}")

    logger.info("\n" + "=" * 60)
    logger.info(f"📊 FINAL STATS: {stats}")
    logger.info("=" * 60)

    return context.res.json(stats)
