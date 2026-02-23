async def main(context):
    logger.info("=" * 60)
    logger.info("🚀 YouTube → Telegram Bot Started")
    logger.info(f"📅 {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)

    # ── مرحله ۱: نصب ffmpeg ──────────────────────────────────
    ffmpeg_ok = install_ffmpeg()
    if not ffmpeg_ok:
        logger.warning("⚠️ ffmpeg not available — merged/converted videos may fail")

    # ── مرحله ۲: به‌روزرسانی yt-dlp ─────────────────────────
    ensure_latest_ytdlp()

    # ── مرحله ۳: اعتبارسنجی config ──────────────────────────
    if not TELEGRAM_BOT_TOKEN:
        logger.error("❌ TELEGRAM_BOT_TOKEN not set!")
        return context.res.json({"error": "Missing TELEGRAM_BOT_TOKEN"})

    if not TELEGRAM_CHANNEL_ID:
        logger.error("❌ TELEGRAM_CHANNEL_ID not set!")
        return context.res.json({"error": "Missing TELEGRAM_CHANNEL_ID"})

    # ── مرحله ۴: آماده‌سازی کوکی ────────────────────────────
    cookie_path = prepare_cookies()

    # ── مرحله ۵: بارگذاری تاریخچه ───────────────────────────
    posted_history = load_history()
    logger.info(f"📋 Already posted: {len(posted_history)} videos")

    # ── مرحله ۶: جستجو ──────────────────────────────────────
    logger.info(f"🔍 Searching: '{YOUTUBE_SEARCH_QUERY}'")
    video_ids = search_youtube(YOUTUBE_SEARCH_QUERY, cookie_path, max_results=30)

    if not video_ids:
        logger.error("❌ No videos found")
        return context.res.json({"error": "No videos found"})

    new_ids = [v for v in video_ids if v not in posted_history]
    logger.info(f"🆕 New videos: {len(new_ids)}/{len(video_ids)}")

    stats = {
        "posted":  0,
        "no_info": 0,
        "dl_fail": 0,
        "tg_fail": 0,
    }

    # ── مرحله ۷: پردازش ویدیوها ─────────────────────────────
    for video_id in new_ids:
        if stats["posted"] >= MAX_VIDEOS:
            logger.info(f"✅ Reached target of {MAX_VIDEOS} posts")
            break

        logger.info(f"\n{'─' * 40}")
        logger.info(f"🎬 Processing: https://youtu.be/{video_id}")

        video_info = get_video_info(video_id, cookie_path)

        if not video_info:
            stats["no_info"] += 1
            continue

        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = download_video(video_info, tmpdir, cookie_path)

            if not video_path:
                stats["dl_fail"] += 1
                logger.error(f"❌ Download failed: {video_id}")
                continue

            success = await post_to_telegram(video_info, video_path)

        if success:
            stats["posted"] += 1
            posted_history.add(video_id)
            save_history(posted_history)
        else:
            stats["tg_fail"] += 1

    # ── گزارش نهایی ─────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("📊 FINAL STATS:")
    logger.info(f"   ✅ Posted:         {stats['posted']}")
    logger.info(f"   📦 No info:        {stats['no_info']}")
    logger.info(f"   ❌ Download fail:  {stats['dl_fail']}")
    logger.info(f"   📱 Telegram fail:  {stats['tg_fail']}")
    logger.info("=" * 60)

    return context.res.json(stats)
