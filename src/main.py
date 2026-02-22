import os
import sys
import glob
import requests
import random
from datetime import datetime, timedelta
import yt_dlp
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from googleapiclient.discovery import build

class QuietLogger:
    def debug(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass
    def info(self, msg): pass

def main(context):
    context.log("⏰ Bot execution started...")

    # تنظیمات اولیه
    endpoint = os.environ.get("APPWRITE_ENDPOINT")
    project_id = os.environ.get("APPWRITE_PROJECT_ID")
    appwrite_api_key = os.environ.get("APPWRITE_API_KEY")
    db_id = os.environ.get("APPWRITE_DATABASE_ID")
    collection_id = os.environ.get("APPWRITE_COLLECTION_ID")
    youtube_api_key = os.environ.get("YOUTUBE_API_KEY")
    telegram_token = os.environ.get("TELEGRAM_TOKEN")
    telegram_channel = os.environ.get("TELEGRAM_CHANNEL")

    client = Client()
    client.set_endpoint(endpoint)
    client.set_project(project_id)
    client.set_key(appwrite_api_key)
    databases = Databases(client)

    youtube = build('youtube', 'v3', developerKey=youtube_api_key)

    # -----------------------------------------------------------------
    # 1. تنوع بخشیدن به جستجو (جلوگیری از تمام شدن ویدیوها)
    # -----------------------------------------------------------------
    queries = [
        "مهندسی مکانیک", 
        "Mechanical Engineering shorts",
        "Mechanical mechanisms", 
        "Engineering gears",
        "CNC machining process",
        "Thermodynamics experiment",
        "Fluid mechanics shorts",
        "Robotics mechanical design"
    ]
    search_query = random.choice(queries)
    context.log(f"🔍 Searching for: {search_query}")

    # گرفتن ویدیوهای وایرال شده در 6 ماه اخیر (محتوای تازه)
    six_months_ago = (datetime.utcnow() - timedelta(days=180)).isoformat() + "Z"

    try:
        search_response = youtube.search().list(
            q=search_query,
            part='snippet',
            type='video',
            videoDuration='short',
            order='viewCount',
            publishedAfter=six_months_ago,
            maxResults=50
        ).execute()
    except Exception as e:
        context.error(f"YouTube API Error: {str(e)}")
        return context.res.json({"success": False, "error": "YouTube API Error"})

    base_dir = os.path.dirname(os.path.abspath(__file__))
    cookie_path = os.path.join(base_dir, 'cookies.txt')

    # فرمت جادویی: سعی در یافتن 360p استاندارد، وگرنه بهترین فرمت یکپارچه
    ydl_opts = {
        'format': '18/b[ext=mp4]/b',
        'outtmpl': '/tmp/%(id)s.%(ext)s',
        'quiet': True,
        'no_warnings': True,
        'logger': QuietLogger(),
        'extractor_args': {'youtube': {'player_client': ['android', 'web']}}
    }
    if os.path.exists(cookie_path):
        ydl_opts['cookiefile'] = cookie_path

    # متغیرهای آمارگیری برای لاگ شفاف
    videos_posted = 0
    stats = {"duplicates": 0, "too_long": 0, "format_error": 0, "telegram_error": 0}

    for item in search_response.get('items', []):
        if videos_posted >= 2:
            break

        video_id = item['id']['videoId']
        video_title = item['snippet']['title']
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        # -----------------------------------------------------------------
        # 2. بررسی تکراری بودن در دیتابیس (بدون هشدارهای قرمز Appwrite)
        # -----------------------------------------------------------------
        is_duplicate = False
        old_stderr = sys.stderr
        with open(os.devnull, 'w') as devnull:
            sys.stderr = devnull
            try:
                existing_docs = databases.list_documents(
                    database_id=db_id,
                    collection_id=collection_id,
                    queries=[Query.equal("videoId", video_id)]
                )
                if existing_docs['total'] > 0:
                    is_duplicate = True
            except Exception:
                pass
            finally:
                sys.stderr = old_stderr

        if is_duplicate:
            stats["duplicates"] += 1
            continue

        # -----------------------------------------------------------------
        # 3. دانلود ویدیو (رد کردن خودکار ویدیوهایی که نیاز به FFmpeg دارند)
        # -----------------------------------------------------------------
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(video_url, download=False)
                if not info_dict: continue
                
                duration = info_dict.get('duration', 0)
                if duration == 0 or duration >= 180:
                    stats["too_long"] += 1
                    continue
                
                # اگر فرمت یکپارچه موجود نباشد، در این خط Exception رخ می‌دهد
                ydl.download([video_url])
                
            downloaded_files = glob.glob(f"/tmp/{video_id}.*")
            valid_files = [f for f in downloaded_files if not f.endswith('.part')]

            if not valid_files:
                stats["format_error"] += 1
                continue
            file_path = valid_files[0]

        except Exception:
            stats["format_error"] += 1
            continue

        # -----------------------------------------------------------------
        # 4. ارسال به کانال تلگرام
        # -----------------------------------------------------------------
        telegram_api_url = f"https://api.telegram.org/bot{telegram_token}/sendVideo"
        caption_text = f"🎥 **{video_title}**\n\n🔗 [مشاهده در یوتیوب]({video_url})\n\n#مهندسی_مکانیک #MechanicalEngineering"

        try:
            with open(file_path, 'rb') as video_file:
                payload = {
                    "chat_id": telegram_channel,
                    "caption": caption_text,
                    "parse_mode": "Markdown",
                    "supports_streaming": True
                }
                files = {"video": video_file}
                tg_response = requests.post(telegram_api_url, data=payload, files=files)
        except Exception:
            stats["telegram_error"] += 1
            for f in valid_files:
                if os.path.exists(f): os.remove(f)
            continue

        for f in valid_files:
            if os.path.exists(f): os.remove(f)

        # -----------------------------------------------------------------
        # 5. ثبت در دیتابیس در صورت موفقیت
        # -----------------------------------------------------------------
        if tg_response.status_code == 200:
            old_stderr = sys.stderr
            with open(os.devnull, 'w') as devnull:
                sys.stderr = devnull
                try:
                    databases.create_document(
                        database_id=db_id,
                        collection_id=collection_id,
                        document_id='unique()',
                        data={"videoId": video_id}
                    )
                except Exception:
                    pass
                finally:
                    sys.stderr = old_stderr
            
            videos_posted += 1
            context.log(f"✅ Successfully posted: {video_id}")
        else:
            stats["telegram_error"] += 1

    # گزارش نهایی به شما
    context.log(f"📊 Run Stats: {videos_posted} Posted | {stats['duplicates']} Duplicates | {stats['format_error']} Format Issues | {stats['too_long']} Too Long")

    return context.res.json({
        "success": True,
        "posted_count": videos_posted,
        "stats": stats
    })