import asyncio
import sys
import logging
import time
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

# Initialize root logger so all modules inherit the same handlers
setup_logger()
logger = logging.getLogger(__name__)

# Track start time for performance monitoring
start_time = time.time()


def log_elapsed_time(step_name: str):
    """Log how long a step took."""
    elapsed = time.time() - start_time
    logger.info(f"⏱️ {step_name} completed in {elapsed:.1f}s")


def get_url_set(channels):
    """Extract a set of URLs from channel list for fast lookup."""
    return {ch.get('url') for ch in channels if ch.get('url')}


async def check_urls_concurrently(session, checker, channels):
    """
    Ultra-fast concurrent URL checking with batching and progress tracking.
    Uses your existing checker's methods.
    """
    if not channels:
        return []
    
    total = len(channels)
    valid = []
    checked = 0
    
    # Process in massive batches for speed
    for i in range(0, total, BATCH_SIZE):
        batch = channels[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        
        logger.info(f"🔄 Batch {batch_num}/{total_batches}: Checking {len(batch)} URLs...")
        
        # Create tasks using your checker's method
        tasks = []
        for ch in batch:
            url = ch.get('url')
            if not url:
                continue
            # Use your existing check_single_url method
            tasks.append(checker.check_single_url(session, url))
        
        # Execute all tasks concurrently
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Batch error: {e}")
            continue
        
        # Process results
        for ch, result in zip(batch, results):
            checked += 1
            if isinstance(result, Exception):
                continue
            if isinstance(result, tuple) and len(result) >= 2:
                _, is_working, _ = result
                if is_working:
                    valid.append(ch)
        
        progress = (checked / total) * 100
        logger.info(f"  ✅ Batch {batch_num}: {len(valid)} working so far ({progress:.1f}% complete)")
        
        # Log cache stats
        cache_stats = checker.get_cache_stats()
        logger.debug(f"  📊 Cache: {cache_stats['working_cached']} working, {cache_stats['failed_cached']} failed cached")
    
    return valid


async def main():
    logger.info("🚀 Starting IPTV Channel Collection (Speed Optimized)...")
    logger.info(f"⚡ Max concurrent checks: {MAX_CONCURRENT}")
    logger.info(f"⚡ Batch size: {BATCH_SIZE}")
    logger.info(f"⚡ URL timeout: {URL_CHECK_TIMEOUT}s")
    logger.info(f"⚡ Retries: {RETRIES}")

    checker = FastChecker()
    processor = M3UProcessor()

    # Use checker's optimized connector
    async with aiohttp.ClientSession(
        connector=checker.connector,
        headers=checker.headers,
        timeout=aiohttp.ClientTimeout(total=URL_CHECK_TIMEOUT + 2),
        trust_env=True
    ) as session:

        # ===================== STEP 1: Load existing channels (NO VERIFICATION) =====================
        logger.info("📁 Loading previously saved channels...")
        existing_channels = load_split_json(WORKING_CHANNELS_BASE)
        
        # Clean existing channels
        existing_channels = [
            ch for ch in existing_channels
            if not is_fake_name(ch.get('name', '') or ch.get('name', ''))
        ]
        
        existing_channels = deduplicate_channels(existing_channels)
        existing_urls = get_url_set(existing_channels)
        logger.info(f"📋 Loaded {len(existing_channels)} existing channels ({len(existing_urls)} unique URLs)")
        log_elapsed_time("Loading existing channels")

        # ===================== STEP 2: Load logos =====================
        logos_data = []
        try:
            async with session.get(LOGOS_URL, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 200:
                    logos_data = await resp.json(content_type=None)
            logger.info(f"✅ Loaded {len(logos_data)} logos")
        except Exception as e:
            logger.warning(f"⚠️ Could not load logos: {e}")
        log_elapsed_time("Loading logos")

        # ===================== STEP 3: World IPTV Scraper =====================
        logger.info("🌍 Scraping World IPTV sources...")
        try:
            world_iptv_urls = scrape_world_iptv_channels()
            if world_iptv_urls:
                new_world_urls = [url for url in world_iptv_urls if url not in ADDITIONAL_M3U]
                logger.info(f"✅ Added {len(new_world_urls)} new M3U sources")
                ADDITIONAL_M3U.extend(new_world_urls)
            else:
                logger.warning("⚠️ No working URLs found from World IPTV scraper")
        except Exception as e:
            logger.error(f"❌ World IPTV scraper failed: {e}")
        log_elapsed_time("World IPTV scraping")

        # ===================== STEP 4: Scrape new sources =====================
        # --- Kenya ---
        logger.info("🇰🇪 Scraping Kenya channels...")
        kenya_channels = await scrape_kenya_tv_channels(logos_data, existing_urls)
        logger.info(f"✅ Kenya: {len(kenya_channels)} new channels")
        log_elapsed_time("Kenya scraping")

        # --- Uganda ---
        logger.info("🇺🇬 Fetching Uganda channels...")
        ug_channels = await fetch_and_process_uganda_channels(session, checker, logos_data, existing_urls)
        logger.info(f"✅ Uganda: {len(ug_channels)} new channels")
        log_elapsed_time("Uganda scraping")

        # --- Adult channels ---
        logger.info("🔞 Fetching adult channels...")
        adult_channels = await scrape_adult_channels(session, logos_data, existing_urls)
        logger.info(f"🔞 Adult: {len(adult_channels)} new channels")
        log_elapsed_time("Adult scraping")

        # --- M3U Playlists (Massively parallel processing) ---
        logger.info("🎬 Processing M3U playlists...")
        global M3U_URLS
        M3U_URLS = M3U_URLS + ADDITIONAL_M3U
        # Filter out empty URLs
        M3U_URLS = [url for url in M3U_URLS if url]
        logger.info(f"Total M3U URLs to process: {len(M3U_URLS)}")

        all_m3u_candidates = []
        
        # First, fetch and parse all M3U files in parallel
        async def fetch_and_parse_m3u(url):
            if not url:
                return []
            try:
                logger.info(f"📥 Fetching: {url}")
                content = await processor.fetch_m3u_content(session, url)
                if not content:
                    logger.warning(f"  ↳ No content from {url}")
                    return []
                parsed = processor.parse_m3u(content)
                candidates = [ch for ch in parsed if ch.get('url')]
                
                # Filter fake names and duplicates
                original_count = len(candidates)
                candidates = [
                    ch for ch in candidates
                    if not is_fake_name(ch.get('name', '') or ch.get('display_name', ''))
                    and ch.get('url') not in existing_urls
                ]
                
                logger.info(f"  📥 {url}: {len(candidates)} candidates (filtered from {original_count})")
                return candidates
            except Exception as e:
                logger.warning(f"  ❌ Error fetching {url}: {e}")
                return []

        # Fetch all M3U files concurrently
        m3u_results = await asyncio.gather(*[fetch_and_parse_m3u(url) for url in M3U_URLS])
        
        # Combine all candidates
        for candidates in m3u_results:
            all_m3u_candidates.extend(candidates)
        
        # Remove duplicates from combined list
        all_m3u_candidates = deduplicate_channels(all_m3u_candidates)
        logger.info(f"🎯 Total unique M3U candidates to check: {len(all_m3u_candidates)}")
        
        # Check all M3U candidates in one massive batch using your checker
        m3u_channels = []
        if all_m3u_candidates:
            logger.info(f"⚡ Starting massive URL check for {len(all_m3u_candidates)} candidates...")
            working_m3u = await check_urls_concurrently(session, checker, all_m3u_candidates)
            
            if working_m3u:
                m3u_channels = processor.format_channel_data(working_m3u, logos_data)
                logger.info(f"✅ M3U: {len(m3u_channels)} new working channels")
                
                # Log cache statistics
                cache_stats = checker.get_cache_stats()
                logger.info(f"📊 Final cache stats: {cache_stats}")
            else:
                logger.info("⚠️ No working M3U channels found")
        else:
            logger.info("✅ M3U: No new candidates to check")
        
        log_elapsed_time("M3U processing")
        
        # Clear cache to free memory
        checker.clear_cache()

        # ===================== STEP 5: Combine with smart retention =====================
        logger.info("🔄 Combining channels with smart retention...")
        
        # Collect all newly found channels
        new_channels = kenya_channels + ug_channels + m3u_channels + adult_channels
        logger.info(f"📊 New channels found: {len(new_channels)}")
        
        # Get URLs of new channels
        new_urls = get_url_set(new_channels)
        logger.info(f"🔗 New unique URLs: {len(new_urls)}")
        
        # Find existing channels that are STILL in the new scrape (retain these)
        # AND find existing channels that are NOT in the new scrape (remove these)
        retained_existing = []
        removed_count = 0
        
        for ch in existing_channels:
            if ch.get('url') in new_urls:
                retained_existing.append(ch)
            else:
                removed_count += 1
        
        logger.info(f"📈 Retained {len(retained_existing)} existing channels (found in new scrape)")
        logger.info(f"🗑️ Removing {removed_count} stale channels (not found in new scrape)")
        
        # Combine retained existing with new channels
        all_channels = retained_existing + new_channels
        
        # Final deduplication
        all_channels = deduplicate_channels(all_channels)
        all_channels = [add_channel_type(ch) for ch in all_channels]
        
        logger.info(f"🎉 Final channel count: {len(all_channels)} (retained: {len(retained_existing)}, new: {len(new_channels)})")
        log_elapsed_time("Channel combination")

        # ===================== STEP 6: Save everything =====================
        logger.info("💾 Saving channels...")
        
        # Build country/category breakdown
        country_files = {}
        category_files = {}
        for ch in all_channels:
            country = ch.get("country", "Unknown")
            country_files.setdefault(country, []).append(ch)
            for cat in ch.get("categories", ["general"]):
                category_files.setdefault(cat, []).append(ch)
        
        # Save channels (this will also update working_channels)
        save_channels(all_channels, country_files, category_files, append=False)
        
        # Save update information
        save_last_update_info(
            total_channels=len(all_channels),
            retained_channels=len(retained_existing),
            new_channels=len(new_channels),
            removed_channels=removed_count
        )
        
        log_elapsed_time("Saving channels")
        
        # ===================== SUMMARY =====================
        total_time = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"✅ PROCESS COMPLETED SUCCESSFULLY!")
        logger.info(f"⏱️ Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
        logger.info(f"📊 Total live channels: {len(all_channels)}")
        logger.info(f"✅ New channels added: {len(new_channels)}")
        logger.info(f"♻️ Existing channels retained: {len(retained_existing)}")
        logger.info(f"🗑️ Stale channels removed: {removed_count}")
        logger.info(f"💾 Working channels file size: {len(all_channels)} channels")
        logger.info("=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⛔ Stopped by user")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}", exc_info=True)
        sys.exit(1)