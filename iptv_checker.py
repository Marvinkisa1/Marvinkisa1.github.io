import asyncio
import sys
import logging

import aiohttp

from logger import setup_logger
from config import *
from utils import remove_duplicates, add_channel_type, is_fake_name
from storage import load_split_json, save_split_json, generate_categories_summary, save_channels
from checker import FastChecker
from processor import M3UProcessor
from scrapers import scrape_kenya_tv_channels, fetch_and_process_uganda_channels

# Initialize root logger so all modules inherit the same handlers
setup_logger()
logger = logging.getLogger(__name__)


async def verify_existing_channels(session, checker, channels):
    """
    Check a list of already‑saved channels and return only the ones that still respond.
    """
    if not channels:
        return []

    tasks = []
    for ch in channels:
        url = ch.get('url')
        if not url:
            continue
        tasks.append(checker.check_single_url(session, url))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    working = []
    for ch, result in zip(channels, results):
        if isinstance(result, Exception):
            continue
        url, is_working, reason = result
        if is_working:
            working.append(ch)

    logger.info(f"Verified existing channels: {len(working)}/{len(channels)} still working")
    return working


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

        # ===================== STEP 0: Load & verify previously saved channels =====================
        logger.info("📁 Loading previously saved channels...")
        existing_channels = load_split_json(WORKING_CHANNELS_BASE)

        
        existing_channels = [
            ch for ch in existing_channels
            if not is_fake_name(ch.get('name', '') or ch.get('name', ''))
        ]

        logger.info(f"Found {len(existing_channels)} channels in working files, checking them...")
        verified_old = await verify_existing_channels(session, checker, existing_channels)
        logger.info(f"✅ {len(verified_old)} previously saved channels are still working")

        # ===================== STEP 1: Scrape new sources =====================

        # --- Kenya ---
        logger.info("🇰🇪 Scraping Kenya channels...")
        kenya_channels = await scrape_kenya_tv_channels(logos_data)
        logger.info(f"✅ Kenya: {len(kenya_channels)} new channels")

        # --- Uganda ---
        logger.info("🇺🇬 Fetching Uganda channels...")
        ug_channels = await fetch_and_process_uganda_channels(session, checker, logos_data)
        logger.info(f"✅ Uganda: {len(ug_channels)} new channels")

        # --- M3U Playlists ---
        logger.info("🎬 Processing M3U playlists...")
        global M3U_URLS
        M3U_URLS = M3U_URLS + ADDITIONAL_M3U
        logger.info(f"Total M3U URLs: {len(M3U_URLS)}")

        m3u_channels = []
        for m3u_url in M3U_URLS:
            if not m3u_url:
                continue
            logger.info(f"📥 Fetching: {m3u_url}")
            content = await processor.fetch_m3u_content(session, m3u_url)
            if not content:
                logger.warning(f"  ↳ No content")
                continue
            parsed = processor.parse_m3u(content)
            candidates = [ch for ch in parsed if ch.get('url')]
            if not candidates:
                logger.info("  ↳ No URLs found")
                continue

            # Remove fake/adult names before checking
            candidates = [
                ch for ch in candidates
                if not is_fake_name(ch.get('name', '') or ch.get('display_name', ''))
            ]
            if not candidates:
                logger.info("  ↳ No URLs remaining after name filter")
                continue

            total = len(candidates)
            logger.info(f"  ↳ Checking {total} streams (concurrent limit: {MAX_CONCURRENT})...")

            tasks = [checker.check_single_url(session, ch['url']) for ch in candidates]
            valid = []
            checked = 0

            async def check_with_progress(ch, coro):
                nonlocal checked, valid
                try:
                    result = await coro
                except Exception as e:
                    logger.debug(f"Error checking {ch.get('name', '?')}: {e}")
                    result = (ch['url'], False, str(e))
                checked += 1
                if checked % M3U_PROGRESS_INTERVAL == 0 or checked == total:
                    logger.info(f"  🟢 Progress: {checked}/{total} checked")
                url, is_working, reason = result
                if is_working:
                    valid.append(ch)

            await asyncio.gather(*[
                check_with_progress(ch, task) for ch, task in zip(candidates, tasks)
            ])

            if valid:
                formatted = processor.format_channel_data(valid, logos_data)
                m3u_channels.extend(formatted)
                logger.info(f"  ✅ Found {len(valid)} working streams")
            else:
                logger.info("  ⚠️ No working streams found")

        logger.info(f"✅ M3U: {len(m3u_channels)} new channels")

        # ===================== STEP 2: Combine all working channels =====================
        all_channels = verified_old + kenya_channels + ug_channels + m3u_channels
        logger.info(f"🔀 Combined raw total: {len(all_channels)} channels")

        # Remove duplicates and add type info
        all_channels = remove_duplicates(all_channels)
        all_channels = [add_channel_type(ch) for ch in all_channels]

        # Final safety filter – drop any adult channels
        all_channels = [ch for ch in all_channels if ch.get("type") != "adult"]

        # ===================== STEP 3: Build country/category breakdown =====================
        country_files = {}
        category_files = {}
        for ch in all_channels:
            country = ch.get("country", "Unknown")
            country_files.setdefault(country, []).append(ch)
            for cat in ch.get("categories", ["general"]):
                category_files.setdefault(cat, []).append(ch)

        # ===================== STEP 4: Overwrite all saved files with the fresh list =====================
        logger.info("💾 Replacing old channel files with fresh data...")
        save_channels(all_channels, country_files, category_files, append=False)

        # Generate categories summary (also done inside save_channels, but safe)
        generate_categories_summary(all_channels)

        logger.info(f"✅ Process completed! Total live channels: {len(all_channels)}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⛔ Stopped by user")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}", exc_info=True)
        sys.exit(1)