import asyncio
import logging
import re
from urllib.parse import urljoin
import aiohttp
from aiohttp import ClientTimeout

from config import ADULT_BASE_URL, ADULT_SEARCH_URL, ADULT_REQUEST_DELAY
from utils import remove_duplicates

logger = logging.getLogger(__name__)


def extract_m3u8_from_page(html: str) -> str | None:
    """Extract .m3u8 URL from HTML using multiple regex patterns."""
    patterns = [
        r'https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*',
        r'["\']([^"\']*\.m3u8[^"\']*)["\']',
        r'source\s*:\s*["\']([^"\']*\.m3u8[^"\']*)["\']',
        r'hls\.src\s*=\s*["\']([^"\']+)["\']',
        r'file\s*:\s*["\']([^"\']*\.m3u8[^"\']*)["\']',
    ]
    for pattern in patterns:
        matches = re.findall(pattern, html, re.IGNORECASE)
        if matches:
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0]
                if '.m3u8' in match:
                    return match.strip()
    return None


async def scrape_adult_channels(session: aiohttp.ClientSession, logos_data: list) -> list:
    """
    Scrape adult channels from adult-tv-channels.click.
    Returns list of channel dicts with type="adult".
    """
    logger.info("🔞 Scraping adult channels...")
    try:
        async with session.get(ADULT_SEARCH_URL, timeout=15) as resp:
            if resp.status != 200:
                logger.error(f"Failed to fetch adult search.json: HTTP {resp.status}")
                return []
            channels_json = await resp.json()
    except Exception as e:
        logger.error(f"Error fetching adult channel list: {e}")
        return []

    total = len(channels_json)
    logger.info(f"Found {total} adult channel entries, checking each...")
    adult_channels = []
    checked = 0

    for ch in channels_json:
        title = ch.get("title", "").strip()
        url_path = ch.get("url", "")
        image = ch.get("image", "")
        categories = ch.get("categories", ["adult"])
        if not title or not url_path:
            continue

        full_page_url = urljoin(ADULT_BASE_URL, url_path)
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            async with session.get(full_page_url, headers=headers, timeout=15) as page_resp:
                if page_resp.status != 200:
                    logger.debug(f"Adult channel {title}: HTTP {page_resp.status}")
                    await asyncio.sleep(ADULT_REQUEST_DELAY)
                    continue
                html = await page_resp.text()
                m3u8_url = extract_m3u8_from_page(html)

            if m3u8_url:
                # Normalize URL
                if m3u8_url.startswith("//"):
                    m3u8_url = "https:" + m3u8_url
                elif not m3u8_url.startswith("http"):
                    m3u8_url = urljoin(full_page_url, m3u8_url)

                # Build channel object
                channel = {
                    "name": title,
                    "id": f"adult_{title.lower().replace(' ', '_')}",  # simple ID
                    "logo": urljoin(ADULT_BASE_URL, image) if image else "",
                    "url": m3u8_url,
                    "categories": categories if categories else ["adult"],
                    "country": "Unknown"  # Adult sites rarely specify country
                }
                adult_channels.append(channel)
                logger.debug(f"  ✅ Adult channel: {title}")
            else:
                logger.debug(f"  ❌ No m3u8 for {title}")

        except Exception as e:
            logger.debug(f"Error processing adult channel {title}: {e}")

        checked += 1
        if checked % 20 == 0 or checked == total:
            logger.info(f"   ...processed {checked}/{total} adult pages")

        await asyncio.sleep(ADULT_REQUEST_DELAY)  # be gentle

    logger.info(f"✅ Found {len(adult_channels)} working adult channels (m3u8 extracted)")
    return remove_duplicates(adult_channels)