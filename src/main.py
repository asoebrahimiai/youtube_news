import os
import glob
import requests
import yt_dlp
import warnings
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from googleapiclient.discovery import build

# این خط باعث می‌شود اخطارهای زردرنگ و بی‌اهمیت (مثل DeprecationWarning) لاگ شما را شلوغ نکنند
warnings.filterwarnings('ignore')

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

    # -----------------------------------------------------------------
    # فاز اول: استخراج اطلاعات بدون هیچ محدودیت فرمت (غیرممکن است ارور بدهد)
    # -----------------------------------------------------------------
    ydl_opts_extract = {
        'quiet': True,
        'noplaylist': True,
        'no_warnings': True,
        'ignoreerrors': True
    }
    
    if os.path.exists(cookie_path):
        ydl_opts_extract['cookiefile'] = cookie_path

    videos_posted_in_this_run = 0

    with yt_dlp.YoutubeDL(ydl_opts_extract) as ydl_extract:
        for item in search_response.get('items', []):
            if videos_posted_in_this_run >= 2:
                break

            video_id = item['id']['videoId']
            video_title = item['snippet']['title']
            video_url = f"https://www.youtube.com/watch?v={video_id}"

            # بررسی تکراری نبودن در دیتابیس
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

            # استخراج اطلاعات ویدیو
            try:
                info_dict = ydl_extract.extract_info(video_url, download=False)
                if not info_dict:
                    continue 

                video_duration = info_dict.get('duration', 0)
                if video_duration == 0 or video_duration >= 180:
                    continue # رد کردن ویدیوهای بالای 3 دقیقه

                # پیدا کردن فرمت‌هایی که هم صدا دارند هم تصویر (بدون نیاز به ادغام)
                formats = info_dict.get('formats', [])
                merged_formats = [
                    f for f in formats 
                    if f.get('vcodec') not in ['none', None] and f.get('acodec') not in ['none', None]
                ]

                # *** نقطه جادویی ***
                # اگر یوتیوب فایل چسبیده‌ای نداد، به جای کرش کردن، خیلی راحت از این ویدیو می‌گذریم
                if not merged_formats:
                    context.log(f"Skipped {video_id}: No pre-merged formats found by YouTube.")
                    continue
                
                # انتخاب بهترین کیفیت MP4 از بین فایل‌های موجود
                mp4_merged = [f for f in merged_formats if f.get('ext') == 'mp4']
                target_formats = mp4_merged if mp4_merged else merged_formats
                selected_format_id = target_formats[-1]['format_id']

            except Exception as e:
                context.log(f"Skipped {video_id}: Extraction problem.")
                continue

            # -----------------------------------------------------------------
            # فاز دوم: دانلود دقیقاً با همان فرمتی که مطمئنیم وجود دارد!
            # -----------------------------------------------------------------
            ydl_opts_download = {
                'format': selected_format_id,
                'outtmpl': '/tmp/%(id)s.%(ext)s',
                'quiet': True,
                'noplaylist': True,
                'no_warnings': True
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
                context.log(f"Skipped {video_id}: Download failed.")
                continue

            # آپلود به تلگرام
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
                except Exception:
                    pass

    return context.res.json({
        "success": True,
        "posted_count": videos_posted_in_this_run
    })
