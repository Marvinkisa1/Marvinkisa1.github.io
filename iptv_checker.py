import asyncio
import sys
import json
from datetime import date, timedelta

import aiohttp

from logger import setup_logger
from config import *
from utils import remove_duplicates, add_channel_type
from storage import load_split_json, save_split_json, generate_categories_summary, delete_split_files, save_channels
from checker import FastChecker
from processor import M3UProcessor
from scrapers import scrape_kenya_tv_channels, fetch_and_process_uganda_channels

logger = setup_logger()


async def main():
    logger.info("🚀 Starting IPTV Channel Collection...")

    checker = FastChecker()
    processor = M3UProcessor()

    async with aiohttp.ClientSession(
        connector=checker.connector,
        headers=checker.headers
    ) as session:

        # Load logos
        logos_data = []
        try:
            async with session.get(LOGOS_URL, timeout=40) as resp:
                if resp.status == 200:
                    logos_data = await resp.json()
            logger.info(f"✅ Loaded {len(logos_data)} logos")
        except Exception as e:
            logger.warning(f"Could not load logos: {e}")

        # === Step 0: Daily M3U URLs ===
        logger.info("📡 Scraping daily M3U URLs...")
        global M3U_URLS
        M3U_URLS = M3U_URLS + ADDITIONAL_M3U
        logger.info(f"Total M3U URLs: {len(M3U_URLS)}")

        # === Step 1: Kenya channels ===
        logger.info("🇰🇪 Scraping Kenya channels...")
        kenya_channels = await scrape_kenya_tv_channels(logos_data)
        if kenya_channels:
            country_files = {"KE": kenya_channels}
            category_files = {}
            for ch in kenya_channels:
                for cat in ch.get("categories", ["general"]):
                    category_files.setdefault(cat, []).append(ch)
            save_channels(kenya_channels, country_files, category_files, append=True)
            logger.info(f"Added {len(kenya_channels)} Kenya channels")

        # === Step 1.5: Uganda channels ===
        logger.info("🇺🇬 Fetching Uganda channels...")
        await fetch_and_process_uganda_channels(session, checker, logos_data)

        # === Step 2: Process M3U Playlists ===
        logger.info("🎬 Processing M3U playlists...")
        m3u_channels = []
        for m3u_url in M3U_URLS:
            if not m3u_url:
                continue
            logger.info(f"Processing M3U: {m3u_url}")
            content = await processor.fetch_m3u_content(session, m3u_url)
            if not content:
                continue
            parsed = processor.parse_m3u(content)

            # Prepare channels that have a URL
            candidates = [ch for ch in parsed if ch.get('url')]
            if not candidates:
                continue

            # Launch all checks concurrently – the semaphore inside FastChecker limits parallelism
            tasks = [checker.check_single_url(session, ch['url']) for ch in candidates]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            valid = []
            for ch, result in zip(candidates, results):
                if isinstance(result, Exception):
                    logger.debug(f"Error checking {ch.get('name', '?')}: {result}")
                    continue
                url, is_working, reason = result
                if is_working:
                    valid.append(ch)

            if valid:
                formatted = processor.format_channel_data(valid, logos_data)
                m3u_channels.extend(formatted)
                logger.info(f"Found {len(valid)} working streams from {m3u_url}")

        if m3u_channels:
            country_files = {}
            category_files = {}
            for ch in m3u_channels:
                country = ch.get("country", "Unknown")
                country_files.setdefault(country, []).append(ch)
                for cat in ch.get("categories", ["general"]):
                    category_files.setdefault(cat, []).append(ch)
            save_channels(m3u_channels, country_files, category_files, append=True)
            logger.info(f"Added {len(m3u_channels)} channels from M3U sources")

        # === Final Sync & Save ===
        logger.info("🔄 Final syncing all channels...")
        all_channels = load_split_json(WORKING_CHANNELS_BASE)
        all_channels = remove_duplicates(all_channels)
        all_channels = [add_channel_type(ch) for ch in all_channels]

        save_split_json(WORKING_CHANNELS_BASE, all_channels)
        generate_categories_summary(all_channels)

        logger.info(f"✅ Process completed! Total channels: {len(all_channels)}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⛔ Stopped by user")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}", exc_info=True)
        sys.exit(1)