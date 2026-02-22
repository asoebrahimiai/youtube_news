import os
import sys
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

    # مرحله ۱: یافتن ۵۰ ویدیو از یوتیوب
    try:
        search_response = youtube.search().list(
            q=search_query,
            part='snippet',
            type='video',
            videoDuration='short', # گرفتن ویدیوهای کوتاه
            order='viewCount',
            maxResults=50
        ).execute()
    except Exception as e:
        context.error(f"YouTube API Error: {str(e)}")
        return context.res.json({"success": False, "error": "YouTube API Error"})

    base_dir = os.path.dirname(os.path.abspath(__file__))
    cookie_path = os.path.join(base_dir, 'cookies.txt')

    videos_posted_in_this_run = 0

    for item in search_response.get('items', []):
        if videos_posted_in_this_run >= 2:
            break

        video_id = item['id']['videoId']
        video_title = item['snippet']['title']
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        # -----------------------------------------------------------------
        # بررسی دیتابیس (بدون چاپ شدن هشدارهای مزاحم Appwrite)
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
            continue

        # -----------------------------------------------------------------
        # مرحله ۲: استخراج اطلاعات و پیدا کردن فایل یکپارچه (صدا + تصویر)
        # -----------------------------------------------------------------
        ydl_opts_extract = {
            'quiet': True,
            'no_warnings': True,
            'logger': QuietLogger(),
            'extractor_args': {'youtube': {'player_client': ['android', 'web']}}
        }
        if os.path.exists(cookie_path):
            ydl_opts_extract['cookiefile'] = cookie_path

        try:
            with yt_dlp.YoutubeDL(ydl_opts_extract) as ydl:
                info_dict = ydl.extract_info(video_url, download=False)
                if not info_dict: 
                    continue
                
                # بررسی مدت زمان (زیر 3 دقیقه)
                duration = info_dict.get('duration', 0)
                if duration == 0 or duration >= 180:
                    context.log(f"⏩ Skipped {video_id}: Too long ({duration}s)")
                    continue

                # ----- فیلتر جادویی پایتون -----
                # پیدا کردن فرمت‌هایی که از قبل صدا و تصویرشان چسبیده است
                formats = info_dict.get('formats', [])
                valid_formats = []
                for f in formats:
                    vcodec = f.get('vcodec')
                    acodec = f.get('acodec')
                    protocol = f.get('protocol', '')
                    
                    # باید هم تصویر داشته باشد، هم صدا و از نوع پخش زنده (m3u8) نباشد
                    if (vcodec != 'none' and vcodec is not None) and \
                       (acodec != 'none' and acodec is not None) and \
                       ('m3u8' not in protocol):
                        valid_formats.append(f)

                if not valid_formats:
                    context.log(f"⏩ Skipped {video_id}: No pre-merged format available.")
                    continue

                # انتخاب بهترین کیفیت از بین فایل‌های یکپارچه
                valid_formats.sort(key=lambda x: x.get('height', 0) or 0, reverse=True)
                best_format_id = valid_formats[0]['format_id']
                best_ext = valid_formats[0].get('ext', 'mp4')

        except Exception as e:
            continue

        # -----------------------------------------------------------------
        # مرحله ۳: دانلود دقیقاً همان فرمت پیدا شده
        # -----------------------------------------------------------------
        ydl_opts_download = {
            'format': best_format_id, # دانلود با ID دقیق
            'outtmpl': f'/tmp/{video_id}.{best_ext}',
            'quiet': True,
            'no_warnings': True,
            'logger': QuietLogger()
        }
        if os.path.exists(cookie_path):
            ydl_opts_download['cookiefile'] = cookie_path

        try:
            with yt_dlp.YoutubeDL(ydl_opts_download) as ydl:
                ydl.download([video_url])

            downloaded_files = glob.glob(f"/tmp/{video_id}.*")
            valid_files = [f for f in downloaded_files if not f.endswith('.part')]

            if not valid_files:
                continue
            file_path = valid_files[0]
        except Exception:
            context.log(f"❌ Failed to download {video_id}")
            continue

        # -----------------------------------------------------------------
        # مرحله ۴: ارسال به تلگرام
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
            for f in valid_files:
                if os.path.exists(f): os.remove(f)
            continue

        # پاک کردن فایل از روی سرور پس از ارسال
        for f in valid_files:
            if os.path.exists(f): os.remove(f)

        # -----------------------------------------------------------------
        # مرحله ۵: ثبت در دیتابیس
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
            
            videos_posted_in_this_run += 1
            context.log(f"✅ Successfully posted: {video_id}")

    if videos_posted_in_this_run == 0:
        context.log("ℹ️ Evaluated 50 videos, but couldn't find a compatible/new one in this run.")

    return context.res.json({
        "success": True,
        "posted_count": videos_posted_in_this_run
    })
