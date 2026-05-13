import asyncio
import sys
import json
from datetime import date, timedelta

import aiohttp

from logger import setup_logger
from config import *
from utils import remove_duplicates, add_channel_type
from storage import load_split_json, save_split_json, generate_categories_summary, delete_split_files
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
            async with session.get(LOGOS_URL, timeout=30) as resp:
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

        # === Kenya & Uganda ===
        logger.info("🇰🇪 Scraping Kenya channels...")
        kenya_channels = await scrape_kenya_tv_channels(logos_data)

        logger.info("🇺🇬 Fetching Uganda channels...")
        await fetch_and_process_uganda_channels(session, checker, logos_data)

        # === Process M3U Playlists ===
        logger.info("🎬 Processing M3U playlists...")
        # (You can expand this part later)

        # === Final Sync & Save ===
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