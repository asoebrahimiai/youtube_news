import os
import glob
import requests
import yt_dlp
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from googleapiclient.discovery import build

def main(context):
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

    try:
        # تغییر کلیدی: videoDuration='short' حذف شد تا ویدیوهای معمولی (که نیاز به FFmpeg ندارند) پیدا شوند
        search_response = youtube.search().list(
            q=search_query,
            part='snippet',
            type='video',
            order='viewCount',
            maxResults=20 
        ).execute()
    except Exception as e:
        context.error(f"YouTube API Error: {str(e)}")
        return context.res.json({"success": False, "error": "YouTube API Error"})

    base_dir = os.path.dirname(os.path.abspath(__file__))
    cookie_path = os.path.join(base_dir, 'cookies.txt')

    # تنظیمات yt-dlp برای ویدیوهای استاندارد
    ydl_opts = {
        'format': '18/best[ext=mp4]/best', # فرمت 18 همیشه برای ویدیوهای معمولی وجود دارد
        'outtmpl': '/tmp/%(id)s.%(ext)s',
        'quiet': True,
        'noplaylist': True,
        'no_warnings': True,
        'ignoreerrors': True # این خط از توقف کامل برنامه در صورت خرابی یک ویدیو جلوگیری می‌کند
    }

    if os.path.exists(cookie_path):
        ydl_opts['cookiefile'] = cookie_path

    videos_posted_in_this_run = 0

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for item in search_response.get('items', []):
            if videos_posted_in_this_run >= 2:
                break

            video_id = item['id']['videoId']
            video_title = item['snippet']['title']
            video_url = f"https://www.youtube.com/watch?v={video_id}"

            try:
                existing_docs = databases.list_documents(
                    database_id=db_id,
                    collection_id=collection_id,
                    queries=[Query.equal("videoId", video_id)]
                )
                if existing_docs['total'] > 0:
                    continue
            except Exception as e:
                context.error(f"Database Error: {str(e)}")
                continue

            try:
                info_dict = ydl.extract_info(video_url, download=False)
                if not info_dict:
                    continue # ویدیوهای در دسترس نبوده را رد می‌کند
                
                video_duration = info_dict.get('duration', 0)

                # فیلتر پایتون: ویدیوهای معمولی اما زیر 3 دقیقه (180 ثانیه) را انتخاب می‌کند
                if video_duration == 0 or video_duration >= 180:
                    context.log(f"Skipped {video_id}: Duration >= 180s")
                    continue

            except Exception as e:
                context.error(f"Extraction Error for {video_id}: {str(e)}")
                continue

            # فاز دانلود
            context.log(f"Downloading {video_id}...")
            try:
                ydl.download([video_url])
                
                downloaded_files = glob.glob(f"/tmp/{video_id}.*")
                valid_files = [f for f in downloaded_files if not f.endswith('.part') and not f.endswith('.ytdl')]
                
                if not valid_files:
                    context.error(f"File not found in /tmp/ for {video_id}")
                    continue
                
                file_path = valid_files[0]
            except Exception as e:
                context.error(f"Download failed for {video_id}: {str(e)}")
                continue

            # آپلود به تلگرام
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
                context.error(f"Telegram Read/Send Error: {str(e)}")
                for f in valid_files:
                    if os.path.exists(f): os.remove(f)
                continue

            # پاکسازی سرور
            for f in valid_files:
                if os.path.exists(f):
                    os.remove(f)

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
