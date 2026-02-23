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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHANNEL_ID  = os.environ.get("TELEGRAM_CHANNEL", "")
YOUTUBE_SEARCH_QUERY = os.environ.get("YOUTUBE_SEARCH_QUERY", "python tutorial")
YOUTUBE_COOKIES_B64  = os.environ.get("YOUTUBE_COOKIES", "")
MAX_VIDEOS           = int(os.environ.get("MAX_VIDEOS", "3"))
HISTORY_FILE         = "/tmp/posted_history.json"


def prepare_cookies():
    if not YOUTUBE_COOKIES_B64:
        return None
    try:
        cookie_path = "/tmp/yt_cookies.txt"
        data = base64.b64decode(YOUTUBE_COOKIES_B64)
        with open(cookie_path, "wb") as f:
            f.write(data)
        logger.info(f"✅ Cookies written ({len(data)} bytes)")
        return cookie_path
    except Exception as e:
        logger.error(f"❌ Cookie error: {e}")
        return None


def ensure_latest_ytdlp():
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "--upgrade", "yt-dlp", "-q"],
            capture_output=True, text=True, timeout=60
        )
        logger.info("✅ yt-dlp updated")
    except Exception as e:
        logger.warning(f"⚠️ yt-dlp update skipped: {e}")


def load_history():
    try:
        with open(HISTORY_FILE, "r") as f:
            return set(json.load(f))
    except Exception:
        return set()


def save_history(history):
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(list(history), f)
    except Exception as e:
        logger.error(f"❌ Save history error: {e}")


def search_youtube(query, cookie_path, max_results=30):
    ydl_opts = {
        "quiet":        True,
        "no_warnings":  True,
        "extract_flat": True,
        "playlistend":  max_results,
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


def download_video(video_id, tmpdir, cookie_path):
    url      = f"https://www.youtube.com/watch?v={video_id}"
    out_tmpl = os.path.join(tmpdir, "%(id)s.%(ext)s")

    # فقط فرمت‌های ساده — بدون FFmpeg
    FORMATS = ["best", "worst"]

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

    for fmt in FORMATS:
        logger.info(f"🎯 Trying format: '{fmt}'")

        for old_f in Path(tmpdir).iterdir():
            try:
                old_f.unlink()
            except Exception:
                pass

        try:
            with yt_dlp.YoutubeDL({**base_opts, "format": fmt}) as ydl:
                info = ydl.extract_info(url, download=True)

            if not info:
                continue

            for f in Path(tmpdir).iterdir():
                if f.name.startswith(video_id):
                    size_mb = f.stat().st_size / (1024 * 1024)
                    logger.info(f"📁 {f.name} — {size_mb:.2f} MB")
                    if size_mb < 0.01:
                        f.unlink()
                        continue
                    if size_mb > 50:
                        logger.warning(f"⚠️ Too large ({size_mb:.1f} MB)")
                        f.unlink()
                        break

                    video_info = {
                        "id":          info.get("id", video_id),
                        "title":       info.get("title", "No Title"),
                        "description": (info.get("description") or "")[:800],
                        "duration":    info.get("duration") or 0,
                        "view_count":  info.get("view_count") or 0,
                        "uploader":    info.get("uploader", "Unknown"),
                        "webpage_url": info.get("webpage_url", f"https://youtu.be/{video_id}"),
                    }
                    return str(f), video_info

        except Exception as e:
            err = str(e)
            logger.warning(f"⚠️ Format '{fmt}' error: {err[:200]}")
            if "rate" in err.lower() or "429" in err:
                logger.warning("⏳ Rate limited — waiting 30s...")
                time.sleep(30)
            for f in Path(tmpdir).iterdir():
                try:
                    f.unlink()
                except Exception:
                    pass

    return None, None


async def post_to_telegram(video_info, video_path):
    duration  = video_info.get("duration") or 0
    mins, secs = divmod(duration, 60)

    caption = (
        f"🎬 *{video_info['title']}*\n\n"
        f"👤 {video_info.get('uploader', 'Unknown')}\n"
        f"👁 {video_info.get('view_count', 0):,} views\n"
        f"⏱ {mins}:{secs:02d}\n\n"
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
                        "chat_id":    TELEGRAM_CHANNEL_ID,
                        "caption":    caption[:1024],
                        "parse_mode": "Markdown",
                        "supports_streaming": True,
                    },
                    files={"video": vf},
                    timeout=300,
                )
            )
        if response.status_code == 200:
            logger.info(f"✅ Posted: {video_info['title']}")
            return True
        else:
            logger.error(f"❌ Telegram {response.status_code}: {response.text[:300]}")
            return False
    except Exception as e:
        logger.error(f"❌ post_to_telegram exception: {e}")
        return False


async def main(context):
    logger.info("=" * 60)
    logger.info("🚀 YouTube → Telegram Bot Started")
    logger.info(f"📅 {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)

    try:
        ensure_latest_ytdlp()

        logger.info(f"🔑 TOKEN len={len(TELEGRAM_BOT_TOKEN)}")
        logger.info(f"📢 CHANNEL='{TELEGRAM_CHANNEL_ID}'")
        logger.info(f"🔍 QUERY='{YOUTUBE_SEARCH_QUERY}'")
        logger.info(f"🍪 COOKIES={'yes' if YOUTUBE_COOKIES_B64 else 'no'}")
        logger.info(f"🎬 MAX_VIDEOS={MAX_VIDEOS}")

        if not TELEGRAM_BOT_TOKEN:
            return context.res.json({"ok": False, "error": "Missing TELEGRAM_TOKEN"})
        if not TELEGRAM_CHANNEL_ID:
            return context.res.json({"ok": False, "error": "Missing TELEGRAM_CHANNEL"})

        # تست تلگرام
        resp = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe",
            timeout=10
        )
        if resp.status_code != 200:
            return context.res.json({"ok": False, "error": "Telegram auth failed"})
        bot_name = resp.json().get("result", {}).get("username", "?")
        logger.info(f"✅ Telegram OK — @{bot_name}")

        cookie_path    = prepare_cookies()
        posted_history = load_history()
        logger.info(f"📋 History: {len(posted_history)} videos")

        video_ids = search_youtube(YOUTUBE_SEARCH_QUERY, cookie_path)
        if not video_ids:
            return context.res.json({"ok": False, "error": "No videos found"})

        new_ids = [v for v in video_ids if v not in posted_history]
        logger.info(f"🆕 New: {len(new_ids)}/{len(video_ids)}")

        if not new_ids:
            return context.res.json({"ok": True, "info": "All already posted"})

        stats = {"posted": 0, "dl_fail": 0, "tg_fail": 0}

        for video_id in new_ids:
            if stats["posted"] >= MAX_VIDEOS:
                break

            logger.info(f"\n{'─'*40}")
            logger.info(f"🎬 https://youtu.be/{video_id}")

            with tempfile.TemporaryDirectory() as tmpdir:
                video_path, video_info = download_video(video_id, tmpdir, cookie_path)

                if not video_path or not video_info:
                    stats["dl_fail"] += 1
                    time.sleep(3)
                    continue

                logger.info(f"📝 {video_info['title']}")
                success = await post_to_telegram(video_info, video_path)

            if success:
                stats["posted"] += 1
                posted_history.add(video_id)
                save_history(posted_history)
            else:
                stats["tg_fail"] += 1

            if stats["posted"] < MAX_VIDEOS:
                time.sleep(5)

        logger.info(f"📊 DONE: {stats}")
        return context.res.json({"ok": True, "stats": stats})

    except Exception as e:
        logger.error(f"❌ FATAL: {e}", exc_info=True)
        return context.res.json({"ok": False, "error": str(e)})
