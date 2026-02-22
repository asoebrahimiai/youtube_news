import os
import requests
import yt_dlp
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from googleapiclient.discovery import build

def main(context):
    # دریافت متغیرهای محیطی
    endpoint = os.environ.get("APPWRITE_ENDPOINT")
    project_id = os.environ.get("APPWRITE_PROJECT_ID")
    appwrite_api_key = os.environ.get("APPWRITE_API_KEY")
    db_id = os.environ.get("APPWRITE_DATABASE_ID")
    collection_id = os.environ.get("APPWRITE_COLLECTION_ID")
    youtube_api_key = os.environ.get("YOUTUBE_API_KEY")
    telegram_token = os.environ.get("TELEGRAM_TOKEN")
    telegram_channel = os.environ.get("TELEGRAM_CHANNEL")

    # تنظیم کلاینت Appwrite
    client = Client()
    client.set_endpoint(endpoint)
    client.set_project(project_id)
    client.set_key(appwrite_api_key)
    databases = Databases(client)

    # تنظیم کلاینت یوتیوب
    youtube = build('youtube', 'v3', developerKey=youtube_api_key)
    search_query = "مهندسی مکانیک OR Mechanical Engineering"

    try:
        search_response = youtube.search().list(
            q=search_query,
            part='snippet',
            type='video',
            order='viewCount',
            maxResults=15,
            videoDuration='short'
        ).execute()
    except Exception as e:
        context.error(f"YouTube API Error: {str(e)}")
        return context.res.json({"success": False, "error": "YouTube API Error"})

    # پیدا کردن مسیر فایل کوکی در سرور Appwrite
    base_dir = os.path.dirname(os.path.abspath(__file__))
    cookie_path = os.path.join(base_dir, 'cookies.txt')

    # تنظیمات جدید yt-dlp برای حل مشکل فرمت و ربات
    ydl_opts = {
        'format': 'b[ext=mp4]/b',  # حل مشکل ویدئوهای عمودی (Shorts) و فرمت‌ها
        'outtmpl': '/tmp/%(id)s.%(ext)s',
        'quiet': True,
        'noplaylist': True,
        'no_warnings': True,
        'extractor_args': {
            'youtube': {
                'player_client': ['android', 'web']  # ترفند جا زدن به عنوان موبایل
            }
        }
    }

    # اعمال فایل کوکی در صورت وجود
    if os.path.exists(cookie_path):
        ydl_opts['cookiefile'] = cookie_path
        context.log("✅ فایل cookies.txt با موفقیت پیدا شد و اعمال گردید.")
    else:
        context.log("⚠️ هشدار: فایل cookies.txt یافت نشد! امکان بلاک شدن وجود دارد.")

    videos_posted_in_this_run = 0

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for item in search_response.get('items', []):
            if videos_posted_in_this_run >= 2:
                break

            video_id = item['id']['videoId']
            video_title = item['snippet']['title']
            video_url = f"https://www.youtube.com/watch?v={video_id}"

            # بررسی دیتابیس برای جلوگیری از ارسال تکراری
            try:
                existing_docs = databases.list_documents(
                    database_id=db_id,
                    collection_id=collection_id,
                    queries=[Query.equal("videoId", video_id)]
                )
                if existing_docs['total'] > 0:
                    continue
            except Exception as e:
                context.error(f"Database Query Error: {str(e)}")
                continue

            # استخراج اطلاعات بدون دانلود
            try:
                info_dict = ydl.extract_info(video_url, download=False)
                video_duration = info_dict.get('duration', 0)

                # محدودیت زمانی (کمتر از ۱۸۰ ثانیه)
                if video_duration >= 180:
                    context.log(f"Skipped {video_id}: Duration >= 180s")
                    continue
            except Exception as e:
                context.error(f"Extraction Error for {video_id}: {str(e)}")
                continue

            # دانلود ویدئو
            context.log(f"Downloading {video_id}...")
            try:
                ydl.download([video_url])
                downloaded_ext = info_dict.get('ext', 'mp4')
                file_path = f"/tmp/{video_id}.{downloaded_ext}"
            except Exception as e:
                context.error(f"Download failed for {video_id}: {str(e)}")
                continue

            # آپلود در تلگرام
            context.log(f"Uploading {video_id} to Telegram...")
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
            except Exception as e:
                context.error(f"Failed to read/send file: {str(e)}")
                if os.path.exists(file_path): os.remove(file_path)
                continue

            # پاکسازی سرور
            if os.path.exists(file_path):
                os.remove(file_path)

            # ثبت در Appwrite Database
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
                except Exception as e:
                    context.error(f"Database Save Error: {str(e)}")
            else:
                context.error(f"Telegram API Error: {tg_response.text}")

    return context.res.json({
        "success": True,
        "posted_count": videos_posted_in_this_run
    })
