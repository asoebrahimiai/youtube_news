import asyncio
import logging
import os
import sys
import tempfile
import json
import base64
import subprocess
import time
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
        logger.info(f"✅ Cookies written to {cookie_path} ({len(data)} bytes)")
        return cookie_path
    except Exception as e:
        logger.error(f"❌ Cookie decode error: {e}")
        return None

# ── به‌روزرسانی yt-dlp ────────────────────────────────────────
def ensure_latest_ytdlp():
    try:
        subprocess.run(
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

# ── دانلود ویدیو — بدون FFmpeg ───────────────────────────────
def download_video(video_id: str, tmpdir: str, cookie_path: str | None) -> tuple[str | None, dict | None]:
    """
    دانلود ویدیو و برگرداندن (مسیر فایل، اطلاعات ویدیو)
    اطلاعات ویدیو مستقیم از مرحله دانلود استخراج می‌شود
    """
    out_tmpl = os.path.join(tmpdir, "%(id)s.%(ext)s")

    # فرمت‌ها به ترتیب اولویت — بدون نیاز به FFmpeg
    FORMAT_STRATEGIES = [
        "best[ext=mp4][filesize<50M]",
        "best[ext=mp4]",
        "best[filesize<50M]",
        "best",
        "worst[ext=mp4]",
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

        # پاک‌سازی فایل‌های قبلی در tmpdir
        for f in Path(tmpdir).iterdir():
            try:
                f.unlink()
            except Exception:
                pass

        try:
            video_info_container = {}

            class InfoExtractorHook(yt_dlp.YoutubeDL):
                pass

            with yt_dlp.YoutubeDL(opts) as ydl:
                # اطلاعات را قبل از دانلود می‌گیریم
                info = ydl.extract_info(
                    f"https://www.youtube.com/watch?v={video_id}",
                    download=True
                )
                if info:
                    video_info_container = {
                        "id":          info.get("id", video_id),
                        "title":       info.get("title", "No Title"),
                        "description": (info.get("description") or "")[:800],
                        "duration":    info.get("duration") or 0,
                        "view_count":  info.get("view_count") or 0,
                        "uploader":    info.get("uploader", "Unknown"),
                        "webpage_url": info.get("webpage_url", f"https://youtu.be/{video_id}"),
                    }

            # پیدا کردن فایل دانلود شده
            for f in Path(tmpdir).iterdir():
                if f.name.startswith(video_id):
                    size_mb = f.stat().st_size / (1024 * 1024)
                    logger.info(f"✅ Downloaded: {f.name} ({size_mb:.1f} MB)")
                    if size_mb > 50:
                        logger.warning(f"⚠️ File too large ({size_mb:.1f} MB) — skipping format")
                        f.unlink()
                        continue
                    if size_mb < 0.01:
                        logger.warning(f"⚠️ File too small ({size_mb:.2f} MB) — probably corrupt")
                        f.unlink()
                        continue
                    return str(f), video_info_container

        except Exception as e:
            err_str = str(e)
            logger.warning(f"⚠️ Format '{fmt}' failed: {err_str[:200]}")

            # اگر Rate Limited شدیم، صبر می‌کنیم
            if "rate" in err_str.lower() or "429" in err_str:
                logger.warning("⏳ Rate limited — waiting 30 seconds...")
                time.sleep(30)

            # پاک‌سازی
            for f in Path(tmpdir).iterdir():
                try:
                    f.unlink()
                except Exception:
                    pass
            continue

    logger.error(f"❌ All format strategies failed for {video_id}")
    return None, None

# ── ارسال به تلگرام ──────────────────────────────────────────
async def post_to_telegram(video_info: dict, video_path: str) -> bool:
    duration  = video_info.get("duration") or 0
    view_count = video_info.get("view_count") or 0

    caption = (
        f"🎬 *{video_info['title']}*\n\n"
        f"👤 {video_info.get('uploader', 'Unknown')}\n"
        f"👁 {view_count:,} views\n"
        f"⏱ {duration // 60}:{duration % 60:02d}\n\n"
        f"{video_info.get('description', '')}\n\n"
        f"🔗 {video_info.get('webpage_url', '')}"
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
            logger.error(f"❌ Telegram error: {response.status_code} — {response.text[:300]}")
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

    # ── بررسی متغیرهای محیطی ──────────────────────────────────
    logger.info(f"🔑 TOKEN set: {bool(TELEGRAM_BOT_TOKEN)} | len={len(TELEGRAM_BOT_TOKEN)}")
    logger.info(f"📢 CHANNEL set: {bool(TELEGRAM_CHANNEL_ID)} | value='{TELEGRAM_CHANNEL_ID}'")
    logger.info(f"🔍 QUERY: '{YOUTUBE_SEARCH_QUERY}'")
    logger.info(f"🍪 COOKIES set: {bool(YOUTUBE_COOKIES_B64)} | len={len(YOUTUBE_COOKIES_B64)}")
    logger.info(f"🎬 MAX_VIDEOS: {MAX_VIDEOS}")

    # ── اعتبارسنجی ────────────────────────────────────────────
    if not TELEGRAM_BOT_TOKEN:
        logger.error("❌ TELEGRAM_TOKEN is empty!")
        return context.res.json({"error": "Missing TELEGRAM_TOKEN"})

    if not TELEGRAM_CHANNEL_ID:
        logger.error("❌ TELEGRAM_CHANNEL is empty!")
        return context.res.json({"error": "Missing TELEGRAM_CHANNEL"})

    # ── تست اتصال تلگرام ──────────────────────────────────────
    logger.info("🔌 Testing Telegram connection...")
    try:
        test_url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe"
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

    # ── آماده‌سازی کوکی ───────────────────────────────────────
    cookie_path = prepare_cookies()
    logger.info(f"🍪 Cookie path: {cookie_path}")

    # ── تاریخچه ───────────────────────────────────────────────
    posted_history = load_history()
    logger.info(f"📋 Already posted: {len(posted_history)} videos")

    # ── جستجو ─────────────────────────────────────────────────
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

        # ── دانلود و دریافت اطلاعات همزمان ───────────────────
        with tempfile.TemporaryDirectory() as tmpdir:
            logger.info(f"📥 Downloading to: {tmpdir}")
            video_path, video_info = download_video(video_id, tmpdir, cookie_path)

            if not video_path or not video_info:
                logger.error(f"❌ Download/info failed: {video_id}")
                stats["dl_fail"] += 1
                # تأخیر بین ویدیوها برای جلوگیری از Rate Limit
                time.sleep(5)
                continue

            logger.info(f"📝 Title: {video_info['title']}")
            logger.info(f"⏱ Duration: {video_info['duration']}s")
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

        # تأخیر بین ویدیوها
        if stats["posted"] < MAX_VIDEOS:
            logger.info("⏳ Waiting 5s before next video...")
            time.sleep(5)

    logger.info("\n" + "=" * 60)
    logger.info(f"📊 FINAL STATS: {stats}")
    logger.info("=" * 60)

    return context.res.json(stats)
