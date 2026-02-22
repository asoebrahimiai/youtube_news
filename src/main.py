import os
import glob
import requests
import random
from datetime import datetime, timedelta
import yt_dlp
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from googleapiclient.discovery import build

def main(context):
    context.log("🚀 Starting Bot Execution (Safe Mode)...")

    # دریافت متغیرهای محیطی
    endpoint = os.environ.get("APPWRITE_ENDPOINT")
    project_id = os.environ.get("APPWRITE_PROJECT_ID")
    appwrite_api_key = os.environ.get("APPWRITE_API_KEY")
    db_id = os.environ.get("APPWRITE_DATABASE_ID")
    collection_id = os.environ.get("APPWRITE_COLLECTION_ID")
    youtube_api_key = os.environ.get("YOUTUBE_API_KEY")
    telegram_token = os.environ.get("TELEGRAM_TOKEN")
    telegram_channel = os.environ.get("TELEGRAM_CHANNEL")

    # راه اندازی Appwrite
    client = Client()
    client.set_endpoint(endpoint)
    client.set_project(project_id)
    client.set_key(appwrite_api_key)
    databases = Databases(client)

    # راه اندازی YouTube
    youtube = build('youtube', 'v3', developerKey=youtube_api_key)

    # جستجوی ساده و مستقیم
    search_query = "Mechanical Engineering"
    context.log(f"🔎 Searching for: {search_query}")

    try:
        # جستجوی ویدیوهای کوتاه و پربازدید
        search_response = youtube.search().list(
            q=search_query,
            part='snippet',
            type='video',
            order='viewCount',
            maxResults=20  # بررسی 20 ویدیوی اول
        ).execute()
    except Exception as e:
        context.error(f"❌ YouTube API Error: {str(e)}")
        return context.res.json({"success": False})

    # تنظیمات دانلود مخصوص سرور بدون FFmpeg
    # کلید موفقیت: format='best[ext=mp4]' یعنی بهترین فایل تکیِ موجود
    ydl_opts = {
        'format': 'best[ext=mp4]/best',  # اولویت با فایل تکی MP4 است
        'outtmpl': '/tmp/%(id)s.%(ext)s',
        'quiet': True,
        'no_warnings': True,
        # این خط بسیار مهم است: جلوگیری از دانلود فرمت‌های جداگانه
        'match_filter': lambda info, *args, **kwargs: None if info.get('acodec') != 'none' and info.get('vcodec') != 'none' else 'Video is not a single file'
    }

    count = 0

    for item in search_response.get('items', []):
        if count >= 2:  # فقط 2 ویدیو در هر اجرا
            break

        video_id = item['id']['videoId']
        title = item['snippet']['title']
        
        # 1. بررسی تکراری بودن در دیتابیس (با متد صحیح list_documents)
        try:
            # توجه: متد list_documents هنوز در پایتون کار می‌کند اما اگر خطای Deprecation دارید
            # نادیده بگیرید، فعلا کار می‌کند.
            result = databases.list_documents(
                database_id=db_id,
                collection_id=collection_id,
                queries=[Query.equal("videoId", video_id)]
            )
            if result['total'] > 0:
                context.log(f"⚠️ Duplicate skipped: {video_id}")
                continue
        except Exception as e:
            context.log(f"⚠️ DB Check Error (Ignoring): {str(e)}")

        # 2. تلاش برای دانلود
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        file_path = None
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_url, download=False)
                
                # فیلتر زمان (زیر 3 دقیقه)
                if info.get('duration', 0) > 180:
                    continue

                # دانلود واقعی
                ydl.download([video_url])
            
            # پیدا کردن فایل دانلود شده
            files = glob.glob(f"/tmp/{video_id}.mp4")
            if not files:
                # گاهی فرمت mkv دانلود می‌شود اگر mp4 نباشد
                files = glob.glob(f"/tmp/{video_id}.*")
            
            if not files:
                context.log(f"❌ Download failed (No file): {video_id}")
                continue
                
            file_path = files[0]

        except Exception as e:
            context.log(f"❌ Download Error for {video_id}: {str(e)}")
            continue

        # 3. ارسال به تلگرام
        try:
            with open(file_path, 'rb') as f:
                caption = f"🎥 **{title}**\n\n🔗 {video_url}\n\n#Engineering"
                url = f"https://api.telegram.org/bot{telegram_token}/sendVideo"
                payload = {"chat_id": telegram_channel, "caption": caption, "parse_mode": "Markdown"}
                files_data = {"video": f}
                
                resp = requests.post(url, data=payload, files=files_data)
                
                if resp.status_code == 200:
                    # 4. ثبت موفقیت در دیتابیس
                    databases.create_document(
                        database_id=db_id,
                        collection_id=collection_id,
                        document_id='unique()',
                        data={"videoId": video_id}
                    )
                    context.log(f"✅ POSTED: {title}")
                    count += 1
                else:
                    context.log(f"❌ Telegram Error: {resp.text}")

        except Exception as e:
            context.error(f"❌ Upload Error: {str(e)}")
        
        # پاکسازی فایل
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

    context.log(f"🏁 Finished. Total posted: {count}")
    return context.res.json({"posted": count})