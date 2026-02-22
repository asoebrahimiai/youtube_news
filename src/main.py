# این دو خط باید دقیقاً در ابتدای فایل باشند تا تمام اخطارهای مزاحم Appwrite خاموش شوند
import warnings
warnings.filterwarnings("ignore")

import os
import glob
import requests
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
    search_query = "مهندسی مکانیک OR Mechanical Engineering"

    # جستجوی 50 ویدیوی ترند و کوتاه
    try:
        search_response = youtube.search().list(
            q=search_query,
            part='snippet',
            type='video',
            videoDuration='short',
            order='viewCount',
            maxResults=50
        ).execute()
    except Exception as e:
        context.error(f"YouTube API Error: {str(e)}")
        return context.res.json({"success": False, "error": "YouTube API Error"})

    base_dir = os.path.dirname(os.path.abspath(__file__))
    cookie_path = os.path.join(base_dir, 'cookies.txt')

    # فاز 1: فقط استخراج اطلاعات پایه (سریع و بدون دانلود)
    ydl_opts_info = {
        'quiet': True,
        'noplaylist': True,
        'no_warnings': True,
        'logger': QuietLogger(),
        'extractor_args': {'youtube': {'player_client': ['android', 'web']}}
    }
    if os.path.exists(cookie_path):
        ydl_opts_info['cookiefile'] = cookie_path

    videos_posted_in_this_run = 0

    with yt_dlp.YoutubeDL(ydl_opts_info) as ydl_info:
        for item in search_response.get('items', []):
            if videos_posted_in_this_run >= 2:
                break

            video_id = item['id']['videoId']
            video_title = item['snippet']['title']
            video_url = f"https://www.youtube.com/watch?v={video_id}"

            # بررسی تکراری نبودن (اخطارهای Appwrite قبلاً توسط خط 3 مسدود شده‌اند)
            try:
                existing_docs = databases.list_documents(
                    database_id=db_id,
                    collection_id=collection_id,
                    queries=[Query.equal("videoId", video_id)]
                )
                if existing_docs['total'] > 0:
                    continue
            except Exception:
                continue

            # استخراج اطلاعات و بررسی زمان ویدیو
            try:
                info_dict = ydl_info.extract_info(video_url, download=False)
                if not info_dict: continue
                
                duration = info_dict.get('duration', 0)
                if duration == 0 or duration >= 180:
                    continue
            except Exception:
                continue

            # فاز 2: دانلود هوشمند.
            # فرمت 'best[ext=mp4]' به صورت خودکار فایلی را پیدا می‌کند که از قبل صدا و تصویر را با هم دارد.
            ydl_opts_download = {
                'format': 'best[ext=mp4]', 
                'outtmpl': '/tmp/%(id)s.%(ext)s',
                'quiet': True,
                'noplaylist': True,
                'no_warnings': True,
                'logger': QuietLogger(),
                'extractor_args': {'youtube': {'player_client': ['android', 'web']}}
            }
            if os.path.exists(cookie_path):
                ydl_opts_download['cookiefile'] = cookie_path

            try:
                with yt_dlp.YoutubeDL(ydl_opts_download) as ydl_dl:
                    ydl_dl.download([video_url])

                downloaded_files = glob.glob(f"/tmp/{video_id}.*")
                valid_files = [f for f in downloaded_files if not f.endswith('.part')]

                if not valid_files:
                    continue
                file_path = valid_files[0]
            except Exception:
                # اگر ویدیو هیچ فایل یکپارچه‌ای نداشته باشد، yt-dlp اینجا خطا می‌دهد.
                # ما خطا را نادیده می‌گیریم تا ربات سراغ ویدیوی بعدی برود (بدون کرش).
                continue

            # ارسال به تلگرام
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
                for f in valid_files:
                    if os.path.exists(f): os.remove(f)
                continue

            # پاکسازی سرور
            for f in valid_files:
                if os.path.exists(f): os.remove(f)

            if tg_response.status_code == 200:
                try:
                    databases.create_document(
                        database_id=db_id,
                        collection_id=collection_id,
                        document_id='unique()',
                        data={"videoId": video_id}
                    )
                    videos_posted_in_this_run += 1
                    context.log(f"✅ Successfully posted: {video_id}")
                except Exception:
                    pass

    if videos_posted_in_this_run == 0:
        context.log("ℹ️ No suitable videos found in this run (or all were duplicates/lacked merged formats).")

    return context.res.json({
        "success": True,
        "posted_count": videos_posted_in_this_run
    })
