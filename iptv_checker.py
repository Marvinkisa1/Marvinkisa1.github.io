import aiohttp
import asyncio
import json
import os
import re
import logging
import sys
import time
import html
from datetime import date, timedelta
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from aiohttp import ClientTimeout, TCPConnector
import shutil
from fuzzywuzzy import fuzz

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-5s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ─── Config ─────────────────────────────────────────────────────────────────
MAX_CONCURRENT     = int(os.getenv("MAX_CONCURRENT", 100))
INITIAL_TIMEOUT    = 14
MAX_TIMEOUT        = 28
USE_HEAD_METHOD    = True
BATCH_SIZE         = 350

WORKING_CHANNELS_BASE = "working_channels"
COUNTRIES_DIR  = "countries"
CATEGORIES_DIR = "categories"
MAX_CHANNELS_PER_FILE = 4000

KENYA_BASE_URL = os.getenv("KENYA_BASE_URL", "")
UGANDA_API_URL = "https://apps.moochatplus.net/bash/api/api.php?get_posts&page=1&count=100&api_key=cda11bx8aITlKsXCpNB7yVLnOdEGqg342ZFrQzJRetkSoUMi9w"

SCRAPER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
}

# ─── Sources ────────────────────────────────────────────────────────────────
M3U_BASE_SOURCES = [
    "https://raw.githubusercontent.com/ipstreet312/freeiptv/refs/heads/master/all.m3u",
    "https://raw.githubusercontent.com/abusaeeidx/IPTV-Scraper-Zilla/refs/heads/main/combined-playlist.m3u",
]

SCRAPED_M3U_URLS = []

CHANNELS_URL = "https://iptv-org.github.io/api/channels.json"
STREAMS_URL  = "https://iptv-org.github.io/api/streams.json"
LOGOS_URL    = "https://iptv-org.github.io/api/logos.json"

UNWANTED_EXTENSIONS = ['.mkv', '.mp4', '.avi', '.mov', '.flv', '.wmv', '.ts', '.m2ts']

# ─── File helpers ───────────────────────────────────────────────────────────
def delete_split_files(base_name):
    ext = '.json'
    if os.path.exists(base_name + ext):
        os.remove(base_name + ext)
    part = 1
    while True:
        p = f"{base_name}{part}{ext}"
        if not os.path.exists(p):
            break
        os.remove(p)
        part += 1

def load_split_json(base_name):
    ext = '.json'
    all_data = []
    part = 1
    while True:
        p = f"{base_name}{part}{ext}"
        if not os.path.exists(p):
            break
        with open(p, 'r', encoding='utf-8') as f:
            all_data.extend(json.load(f))
        part += 1
    if not all_data and os.path.exists(base_name + ext):
        with open(base_name + ext, 'r', encoding='utf-8') as f:
            all_data = json.load(f)
    return all_data

def save_split_json(base_name, data):
    if not data:
        return
    ext = '.json'
    if len(data) <= MAX_CHANNELS_PER_FILE:
        with open(base_name + ext, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    else:
        part_num = 1
        for i in range(0, len(data), MAX_CHANNELS_PER_FILE):
            chunk = data[i:i + MAX_CHANNELS_PER_FILE]
            p = f"{base_name}{part_num}{ext}"
            with open(p, 'w', encoding='utf-8') as f:
                json.dump(chunk, f, indent=2, ensure_ascii=False)
            part_num += 1

def remove_duplicates(channels):
    seen = set()
    unique = []
    for ch in channels:
        key = (ch.get("url"), ch.get("id"))
        if key not in seen and all(key):
            seen.add(key)
            unique.append(ch)
    return unique

def clear_directories():
    delete_split_files(WORKING_CHANNELS_BASE)
    for d in [COUNTRIES_DIR, CATEGORIES_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

def save_channels(channels, country_map, category_map, append=False):
    if not append:
        clear_directories()
    channels = remove_duplicates(channels)
    os.makedirs(COUNTRIES_DIR, exist_ok=True)
    os.makedirs(CATEGORIES_DIR, exist_ok=True)

    if append:
        old = load_split_json(WORKING_CHANNELS_BASE)
        channels = remove_duplicates(old + channels)

    save_split_json(WORKING_CHANNELS_BASE, channels)

    for cc, chlist in country_map.items():
        if not cc or cc == "Unknown":
            continue
        safe = re.sub(r'[^a-zA-Z0-9_-]', '', cc)
        if not safe:
            continue
        path = os.path.join(COUNTRIES_DIR, safe)
        if append:
            ex = load_split_json(path)
            chlist = remove_duplicates(ex + chlist)
        save_split_json(path, chlist)

    for cat, chlist in category_map.items():
        if not cat:
            continue
        safe = re.sub(r'[^a-zA-Z0-9_-]', '', cat)
        if not safe:
            continue
        path = os.path.join(CATEGORIES_DIR, safe)
        if append:
            ex = load_split_json(path)
            chlist = remove_duplicates(ex + chlist)
        save_split_json(path, chlist)

def sync_working_channels():
    all_channels = []
    for folder in [COUNTRIES_DIR, CATEGORIES_DIR]:
        if os.path.exists(folder):
            for fname in os.listdir(folder):
                if fname.endswith('.json'):
                    base = os.path.join(folder, fname[:-5])
                    all_channels.extend(load_split_json(base))
    all_channels = remove_duplicates(all_channels)
    save_split_json(WORKING_CHANNELS_BASE, all_channels)
    logging.info(f"Synced {len(all_channels)} channels to main file")

def load_all_existing_channels():
    if not os.path.exists(WORKING_CHANNELS_BASE + ".json") and not any(
        f.startswith(WORKING_CHANNELS_BASE) and f.endswith(".json")
        for f in os.listdir(".")
    ):
        return []
    data = load_existing_data()
    return data["all_existing_channels"]

# ─── Improved scraper ───────────────────────────────────────────────────────
def scrape_world_iptv_latest_playlists(max_attempts=12, max_final_urls=10):
    global SCRAPED_M3U_URLS
    SCRAPED_M3U_URLS.clear()

    base = "https://world-iptv.club"
    category_url = f"{base}/category/iptv/"

    logging.info("Scraping world-iptv.club for recent IPTV/M3U playlists...")

    try:
        r = requests.get(category_url, headers=SCRAPER_HEADERS, timeout=18)
        r.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to reach category page: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    articles = soup.find_all("article")

    candidates = []
    keywords = ["free", "iptv", "m3u", "m3u8", "playlist", "working", "update", "daily", "202", "live", "tv", "full", "list", "channel", "world", "latest", "new", "hd"]

    date_pat = r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}'

    for article in articles:
        link_tag = article.find("a", href=True)
        if not link_tag:
            continue
        href = link_tag["href"]
        if not href.startswith(("http", "/")):
            continue
        if href.startswith("/"):
            href = urljoin(base, href)

        title_tag = article.find(["h1","h2","h3","h4"])
        title = title_tag.get_text(" ", strip=True).lower() if title_tag else ""

        full_text = article.get_text(" ", strip=True).lower()

        score = sum(1 for kw in keywords if kw in title or kw in full_text)
        has_date = bool(re.search(date_pat, href)) or bool(re.search(date_pat, title))

        if score >= 2 or has_date or "m3u" in full_text:
            candidates.append((href, score + (3 if has_date else 0) + (1 if "m3u" in full_text else 0)))

    def sort_key(item):
        url, score = item
        m = re.search(r'(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})', url)
        if m:
            try:
                d, mo, y = map(int, m.groups())
                if y < 100: y += 2000
                dt = date(y, mo, d)
                return (-score, -dt.toordinal())
            except:
                pass
        return (-score, 0)

    candidates.sort(key=sort_key)
    top_pages = [url for url, _ in candidates][:max_attempts]

    if not top_pages:
        logging.warning("No promising post pages detected")
        return []

    logging.info(f"Checking {len(top_pages)} most promising pages...")

    found_urls = []

    for page_url in top_pages:
        try:
            resp = requests.get(page_url, headers=SCRAPER_HEADERS, timeout=22)
            if resp.status_code != 200:
                continue

            text = resp.text

            patterns = [
                r'(https?://[^\s\'"<>{}[\]`]+?\.(?:m3u8?|m3u|playlist)(?:[^\s\'"<>{}[\]`]*))',
                r'(https?://[^\s\'"<>{}[\]`]+get\.php\?[^\'"<>{}[\]`]*type=(?:m3u|m3u_plus|m3u8)[^\s\'"<>{}[\]`]*)',
                r'(https?://[^\s\'"<>{}[\]`]+/playlist\.m3u8?[^\s\'"<>{}[\]`]*)',
                r'(https?://[^\s\'"<>{}[\]`]+/live/[^\'"<>{}[\]`]*\.m3u8?[^\s\'"<>{}[\]`]*)',
                r'(https?://[^\s\'"<>{}[\]`]+/index\.m3u8?[^\s\'"<>{}[\]`]*)',
            ]

            for pat in patterns:
                matches = re.findall(pat, text, re.IGNORECASE | re.DOTALL)
                for match in matches:
                    cleaned = html.unescape(match.strip())
                    if len(cleaned) > 30 and cleaned not in found_urls:
                        found_urls.append(cleaned)

        except Exception as e:
            logging.warning(f"Failed to scrape page {page_url[:90]}...: {e}")

    found_urls = list(dict.fromkeys(found_urls))

    verified = []
    for u in found_urls[:max_final_urls * 4]:
        try:
            head = requests.head(u, headers=SCRAPER_HEADERS, timeout=10, allow_redirects=True)
            if head.status_code != 200:
                continue
            ct = head.headers.get("content-type", "").lower()
            cl = int(head.headers.get("content-length", "0"))
            if any(k in ct for k in ["mpegurl", "mp2t", "octet-stream", "video"]) or cl > 150 or u.lower().endswith((".m3u", ".m3u8")):
                verified.append(u)
                if len(verified) >= max_final_urls:
                    break
        except:
            pass

    SCRAPED_M3U_URLS.extend(verified[:max_final_urls])

    if verified:
        logging.info(f"Found {len(verified)} potential fresh playlist links:")
        for i, link in enumerate(verified[:max_final_urls], 1):
            logging.info(f"  {i}. {link}")
    else:
        logging.warning("No verified playlist links found")

    return verified[:max_final_urls]

# ─── Checker ────────────────────────────────────────────────────────────────
class StrictFastChecker:
    def __init__(self):
        self.connector = TCPConnector(limit=MAX_CONCURRENT, force_close=True, enable_cleanup_closed=True)
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    def has_unwanted_extension(self, url):
        return any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS)

    async def check_single_url(self, session, url):
        if self.has_unwanted_extension(url):
            return url, False

        timeouts = [
            ClientTimeout(total=8, connect=3.5, sock_connect=3, sock_read=5),
            ClientTimeout(total=14, connect=5, sock_connect=4, sock_read=9),
            ClientTimeout(total=25, connect=6, sock_connect=5, sock_read=15),
        ]

        headers = {"User-Agent": SCRAPER_HEADERS["User-Agent"], "Accept": "*/*"}

        for attempt, to in enumerate(timeouts, 1):
            try:
                if USE_HEAD_METHOD:
                    async with session.head(url, timeout=to, allow_redirects=True, headers=headers) as r:
                        if r.status in (403, 404, 410):
                            return url, False
                        ct = r.headers.get("content-type", "").lower()
                        if any(x in ct for x in ["mpegurl", "mp2t", "octet-stream"]):
                            return url, True
                        if "text/html" in ct and r.headers.get("content-length", "0") != "0":
                            return url, False

                async with session.get(url, timeout=to, allow_redirects=True,
                                      headers={**headers, "Range": "bytes=0-12287"}) as resp:
                    if resp.status >= 400:
                        continue

                    try:
                        data = await asyncio.wait_for(resp.content.read(16384), 9)
                    except asyncio.TimeoutError:
                        return url, True
                    except:
                        data = b""

                    try:
                        txt = data.decode("utf-8", errors="ignore").lower()
                    except:
                        txt = ""

                    if any(m in txt for m in ["#extm3u", "#ext-x-version", "#ext-x-stream-inf", "#ext-x-targetduration"]):
                        return url, True

                    if len(data) > 400 and data.startswith(b"G@"):
                        return url, True

                    ct = resp.headers.get("content-type", "").lower()
                    if any(k in ct for k in ["mpegurl", "mp2t", "octet-stream"]) and len(data) > 300:
                        return url, True

                    if "text/html" in ct and "<html" in txt:
                        return url, False

            except Exception:
                await asyncio.sleep(0.4 * attempt)
                continue

        return url, False

# ─── M3U Processor ──────────────────────────────────────────────────────────
class M3UProcessor:
    def __init__(self):
        self.unwanted = UNWANTED_EXTENSIONS

    def has_unwanted_extension(self, url):
        return any(url.lower().endswith(e) for e in self.unwanted)

    async def fetch_m3u_content(self, session, url):
        try:
            async with session.get(url, timeout=ClientTimeout(total=INITIAL_TIMEOUT)) as r:
                if r.status == 200:
                    return await r.text()
        except:
            pass
        return None

    def parse_m3u(self, content):
        channels = []
        current = {}
        for line in content.splitlines():
            line = line.strip()
            if line.startswith('#EXTINF:'):
                current = self._parse_extinf(line)
            elif line and not line.startswith('#') and current:
                if not self.has_unwanted_extension(line):
                    current['url'] = line
                    channels.append(current)
                current = {}
        return channels

    def _parse_extinf(self, line):
        attrs = dict(re.findall(r'(\S+)="([^"]*)"', line))
        name_part = line.split(',', 1)[-1].strip()
        country = ''
        clean_name = name_part
        m = re.match(r'^(?:\|([A-Z]{2})\|)|(?:([A-Z]{2}/))', name_part)
        if m:
            country = (m.group(1) or m.group(2) or '').strip('/ ')
            clean_name = name_part[m.end():].strip()
        return {
            'tvg_id': attrs.get('tvg-id', ''),
            'tvg_name': attrs.get('tvg-name', ''),
            'tvg_logo': attrs.get('tvg-logo', ''),
            'group_title': attrs.get('group-title', ''),
            'display_name': clean_name,
            'country_code': country,
            'raw_name': name_part
        }

    def format_channel_data(self, channels, logos):
        result = []
        for ch in channels:
            cid = ch['tvg_id'].lower() if ch['tvg_id'] else \
                  re.sub(r'[^a-z0-9]', '', ch['display_name'].lower()) + \
                  (f".{ch['country_code'].lower()}" if ch['country_code'] else "")
            logo = ch.get('tvg_logo') or ""
            if not logo:
                for lg in logos:
                    if lg["channel"] == cid:
                        logo = lg["url"]
                        break
            result.append({
                'name': ch['display_name'],
                'id': cid,
                'logo': logo,
                'url': ch['url'],
                'categories': self._extract_cats(ch['group_title']),
                'country': ch['country_code'] or "Unknown"
            })
        return result

    def _extract_cats(self, group):
        if not group:
            return ["general"]
        parts = [p.strip().lower() for p in group.split('/') if p.strip()]
        if len(parts) > 1 and re.match(r'^[a-z]{2}$', parts[0]):
            return parts[1:] or ["general"]
        return parts or ["general"]

# ─── Full validation of ALL channels ────────────────────────────────────────
async def validate_all_channels(session, checker):
    all_channels = load_all_existing_channels()
    if not all_channels:
        logging.info("No existing channels to validate.")
        return [], {}, {}

    logging.info(f"Validating {len(all_channels)} existing channels...")

    country_map = {}
    category_map = {}

    async def check(ch):
        async with checker.semaphore:
            _, ok = await checker.check_single_url(session, ch["url"])
            if ok:
                c = ch.get("country", "Unknown")
                country_map.setdefault(c, []).append(ch)
                for cat in ch.get("categories", []):
                    category_map.setdefault(cat, []).append(ch)
                return ch
            return None

    tasks = [check(ch) for ch in all_channels]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    kept = [r for r in results if not isinstance(r, Exception) and r is not None]

    if kept:
        save_channels(kept, country_map, category_map, append=False)
        logging.info(f"Kept {len(kept)} working channels after full validation")
    else:
        logging.info("No working channels remained after validation")

    return kept, country_map, category_map

# ─── Add new channels from M3U ──────────────────────────────────────────────
async def add_new_from_m3u(session, logos_data, checker, m3u_urls):
    processor = M3UProcessor()
    total_new = 0
    country_map = {}
    category_map = {}

    for url in m3u_urls:
        if not url:
            continue
        logging.info(f"Processing M3U: {url}")
        content = await processor.fetch_m3u_content(session, url)
        if not content:
            continue
        channels = processor.parse_m3u(content)
        if not channels:
            continue

        async def check(ch):
            async with checker.semaphore:
                _, ok = await checker.check_single_url(session, ch['url'])
                return ch if ok else None

        tasks = [check(ch) for ch in channels]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        working = [r for r in results if not isinstance(r, Exception) and r]
        formatted = processor.format_channel_data(working, logos_data)
        total_new += len(formatted)

        for ch in formatted:
            c = ch["country"]
            country_map.setdefault(c, []).append(ch)
            for cat in ch["categories"]:
                category_map.setdefault(cat, []).append(ch)

    if total_new > 0:
        save_channels([], country_map, category_map, append=True)
        logging.info(f"Added {total_new} new working channels from M3U")

    return total_new

# ─── Main ───────────────────────────────────────────────────────────────────
async def main():
    logging.info("IPTV collection & full validation started...")

    logging.info("Searching recent playlists on world-iptv.club...")
    scrape_world_iptv_latest_playlists(max_attempts=12, max_final_urls=10)

    all_m3u_urls = M3U_BASE_SOURCES + SCRAPED_M3U_URLS
    logging.info(f"Will process {len(all_m3u_urls)} M3U sources")

    checker = StrictFastChecker()

    async with aiohttp.ClientSession(connector=checker.connector) as session:
        logos_data = await fetch_json(session, LOGOS_URL)
        logging.info(f"Loaded {len(logos_data)} logos")

        # Step 1: Validate & clean ALL existing channels
        _, _, _ = await validate_all_channels(session, checker)

        # Step 2: Add new channels from M3U sources (only working ones)
        new_count = await add_new_from_m3u(session, logos_data, checker, all_m3u_urls)

        # Step 3: Final sync
        sync_working_channels()

    logging.info("──────────────────────────────────────────────")
    logging.info(f"Finished. New working channels added: {new_count}")
    logging.info("All existing channels were checked — broken ones removed.")
    logging.info("──────────────────────────────────────────────")

async def fetch_json(session, url):
    try:
        async with session.get(url, timeout=18) as r:
            if r.status == 200:
                return await r.json()
    except:
        pass
    return []

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.warning("Stopped by user")
    except Exception as e:
        logging.error(f"Critical error: {e}", exc_info=True)
        sys.exit(1)