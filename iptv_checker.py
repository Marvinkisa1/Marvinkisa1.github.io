import asyncio
import sys
import logging
import time
import json
from datetime import datetime

import aiohttp

from logger import setup_logger
from config import *
from utils import remove_duplicates, add_channel_type, is_fake_name, deduplicate_channels
from storage import load_split_json, save_split_json, generate_categories_summary, save_channels, save_last_update_info
from checker import FastChecker
from processor import M3UProcessor
from scrapers import scrape_kenya_tv_channels, fetch_and_process_uganda_channels
from adult_scraper import scrape_adult_channels  
from world_iptv_scraper import scrape_world_iptv_channels
from config import ADDITIONAL_M3U

# Initialize root logger
setup_logger()
logger = logging.getLogger(__name__)

start_time = time.time()


def log_elapsed_time(step_name: str):
    elapsed = time.time() - start_time
    logger.info(f"⏱️ {step_name} completed in {elapsed:.1f}s")


def get_url_set(channels):
    return {ch.get('url') for ch in channels if ch.get('url')}


async def ls_concurrently(session, checker, channels):
    if not channels:
        return []
    
    total = len(channels)
    valid = []
    checked = 0
    
    logger.info(f"⚡ Checking {total:,} candidates (batch size: {BATCH_SIZE})...")
    
    for i in range(0, total, BATCH_SIZE):
        batch = channels[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        
        tasks = [checker.check_single_url(session, ch.get('url')) for ch in batch if ch.get('url')]
        
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for ch, result in zip(batch, results):
                checked += 1
                if isinstance(result, Exception) or not (isinstance(result, tuple) and len(result) >= 2):
                    continue
                if result[1]:  # is_working
                    valid.append(ch)
        except Exception as e:
            logger.error(f"Batch {batch_num} error: {e}")
        
        progress = (checked / total) * 100
        logger.info(f"  ✅ Batch {batch_num}: {len(valid):,} working ({progress:.1f}%)")
        
        if hasattr(checker, 'get_cache_stats'):
            stats = checker.get_cache_stats()
            logger.debug(f"  📊 Cache → Working: {stats['working_cached']} | Failed: {stats['failed_cached']}")
        
        await asyncio.sleep(BATCH_DELAY)
    
    return valid


async def main():
    logger.info("🚀 Starting IPTV Channel Collection (HIGH SPEED MODE)...")
    logger.info(f"⚡ Max concurrent: {MAX_CONCURRENT} | Batch: {BATCH_SIZE} | Timeout: {URL_CHECK_TIMEOUT}s | Retries: {RETRIES}")

    checker = FastChecker()
    processor = M3UProcessor()

    async with aiohttp.ClientSession(
        connector=checker.connector,
        headers=checker.headers,
        timeout=aiohttp.ClientTimeout(total=URL_CHECK_TIMEOUT + 3),
        trust_env=True
    ) as session:

        # ===================== STEP 1: Load existing + cache =====================
        logger.info("📁 Loading previously saved channels...")
        existing_channels = load_split_json(WORKING_CHANNELS_BASE)
        
        existing_channels = [ch for ch in existing_channels if not is_fake_name(ch.get('name', ''))]
        existing_channels = deduplicate_channels(existing_channels)
        existing_urls = get_url_set(existing_channels)
        
        logger.info(f"📋 Loaded {len(existing_channels):,} channels ({len(existing_urls):,} unique URLs)")
        log_elapsed_time("Loading existing channels")

        # ===================== STEP 2: Load logos =====================
        logos_data = []
        try:
            async with session.get(LOGOS_URL, timeout=15) as resp:
                if resp.status == 200:
                    logos_data = await resp.json(content_type=None)
            logger.info(f"✅ Loaded {len(logos_data)} logos")
        except Exception as e:
            logger.warning(f"⚠️ Logos failed: {e}")
        log_elapsed_time("Loading logos")

        # ===================== STEP 3: World IPTV =====================
        logger.info("🌍 Scraping World IPTV sources...")
        try:
            world_iptv_urls = scrape_world_iptv_channels()
            if world_iptv_urls:
                new_urls = [u for u in world_iptv_urls if u not in ADDITIONAL_M3U]
                ADDITIONAL_M3U.extend(new_urls)
                logger.info(f"✅ Added {len(new_urls)} M3U sources")
        except Exception as e:
            logger.error(f"World IPTV error: {e}")
        log_elapsed_time("World IPTV scraping")

        # ===================== STEP 4: Regional scrapers =====================
        kenya_channels = await scrape_kenya_tv_channels(logos_data, existing_urls)
        logger.info(f"🇰🇪 Kenya: {len(kenya_channels)} new")
        log_elapsed_time("Kenya")

        ug_channels = await fetch_and_process_uganda_channels(session, checker, logos_data, existing_urls)
        logger.info(f"🇺🇬 Uganda: {len(ug_channels)} new")
        log_elapsed_time("Uganda")

        adult_channels = await scrape_adult_channels(session, logos_data, existing_urls)
        logger.info(f"🔞 Adult: {len(adult_channels)} new")
        log_elapsed_time("Adult")

        # ===================== STEP 5: M3U Playlists =====================
        logger.info("🎬 Processing M3U playlists...")
        global M3U_URLS
        M3U_URLS = list(dict.fromkeys(M3U_URLS + ADDITIONAL_M3U))  # dedup
        M3U_URLS = [url for url in M3U_URLS if url]

        all_m3u_candidates = []
        
        async def fetch_and_parse_m3u(url):
            try:
                logger.info(f"📥 Fetching {url}")
                content = await processor.fetch_m3u_content(session, url)
                if not content:
                    return []
                parsed = processor.parse_m3u(content)
                candidates = [
                    ch for ch in parsed 
                    if ch.get('url') 
                    and not is_fake_name(ch.get('name', '') or ch.get('display_name', ''))
                    and ch.get('url') not in existing_urls
                ]
                logger.info(f"  📥 {url}: {len(candidates)} candidates")
                return candidates
            except Exception as e:
                logger.warning(f"Error {url}: {e}")
                return []

        m3u_results = await asyncio.gather(*[fetch_and_parse_m3u(u) for u in M3U_URLS])
        for res in m3u_results:
            all_m3u_candidates.extend(res)

        all_m3u_candidates = deduplicate_channels(all_m3u_candidates)
        logger.info(f"🎯 {len(all_m3u_candidates):,} M3U candidates to verify")

        m3u_channels = []
        if all_m3u_candidates:
            working_m3u = await ls_concurrently(session, checker, all_m3u_candidates)
            if working_m3u:
                m3u_channels = processor.format_channel_data(working_m3u, logos_data)
                logger.info(f"✅ M3U: {len(m3u_channels)} working")
        log_elapsed_time("M3U processing")

        checker.clear_cache()

        # ===================== STEP 6: Smart Combine =====================
        logger.info("🔄 Smart retention combine...")
        new_channels = kenya_channels + ug_channels + m3u_channels + adult_channels
        new_urls = get_url_set(new_channels)

        retained_existing = [ch for ch in existing_channels if ch.get('url') in new_urls]
        removed_count = len(existing_channels) - len(retained_existing)

        logger.info(f"📈 Retained {len(retained_existing):,} | Removed {removed_count:,} stale")
        
        all_channels = retained_existing + new_channels
        all_channels = deduplicate_channels(all_channels)
        all_channels = [add_channel_type(ch) for ch in all_channels]

        logger.info(f"🎉 Final: {len(all_channels):,} channels")
        log_elapsed_time("Combining")

        # ===================== STEP 7: Save =====================
        logger.info("💾 Saving...")
        country_files = {}
        category_files = {}
        for ch in all_channels:
            country = ch.get("country", "Unknown")
            country_files.setdefault(country, []).append(ch)
            for cat in ch.get("categories", ["general"]):
                category_files.setdefault(cat, []).append(ch)

        save_channels(all_channels, country_files, category_files, append=False)
        
        save_last_update_info(
            total_channels=len(all_channels),
            retained_channels=len(retained_existing),
            new_channels=len(new_channels),
            removed_channels=removed_count
        )
        
        log_elapsed_time("Saving")

        total_time = time.time() - start_time
        logger.info("=" * 70)
        logger.info("✅ SUCCESS!")
        logger.info(f"⏱️ Total: {total_time:.1f}s ({total_time/60:.1f} min)")
        logger.info(f"📊 Total channels: {len(all_channels):,}")
        logger.info(f"New: {len(new_channels):,} | Retained: {len(retained_existing):,} | Removed: {removed_count:,}")
        logger.info("=" * 70)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⛔ Stopped")
    except Exception as e:
        logger.error(f"💥 Fatal: {e}", exc_info=True)
        sys.exit(1)