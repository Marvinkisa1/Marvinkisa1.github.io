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
from difflib import SequenceMatcher
from fuzzywuzzy import fuzz
import m3u8
from tqdm import tqdm

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-5s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ─── Config ─────────────────────────────────────────────────────────────────
MAX_CONCURRENT     = int(os.getenv("MAX_CONCURRENT", 120))
INITIAL_TIMEOUT    = 16
MAX_TIMEOUT        = 32
RETRIES            = 2
USE_HEAD_METHOD    = True
BATCH_SIZE         = 400

WORKING_CHANNELS_BASE = "working_channels"
COUNTRIES_DIR  = "countries"
CATEGORIES_DIR = "categories"
MAX_CHANNELS_PER_FILE = 4000

KENYA_BASE_URL = os.getenv("KENYA_BASE_URL", "")
UGANDA_API_URL = "https://apps.moochatplus.net/bash/api/api.php?get_posts&page=1&count=100&api_key=cda11bx8aITlKsXCpNB7yVLnOdEGqg342ZFrQzJRetkSoUMi9w"

SCRAPER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
}

# ─── Primary M3U sources (your requested links) ─────────────────────────────
M3U_BASE_SOURCES = [
    "https://raw.githubusercontent.com/ipstreet312/freeiptv/refs/heads/master/all.m3u",
    "https://raw.githubusercontent.com/abusaeeidx/IPTV-Scraper-Zilla/refs/heads/main/combined-playlist.m3u",
]

# Will be filled by world-iptv.club scraper
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
        part_file = f"{base_name}{part}{ext}"
        if not os.path.exists(part_file):
            break
        os.remove(part_file)
        part += 1

def load_split_json(base_name):
    ext = '.json'
    all_data = []
    part = 1
    while True:
        part_file = f"{base_name}{part}{ext}"
        if not os.path.exists(part_file):
            break
        with open(part_file, 'r', encoding='utf-8') as f:
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
            json.dump(data, f, indent=4, ensure_ascii=False)
    else:
        part_num = 1
        for i in range(0, len(data), MAX_CHANNELS_PER_FILE):
            chunk = data[i:i + MAX_CHANNELS_PER_FILE]
            part_file = f"{base_name}{part_num}{ext}"
            with open(part_file, 'w', encoding='utf-8') as f:
                json.dump(chunk, f, indent=4, ensure_ascii=False)
            part_num += 1

def remove_duplicates(channels):
    seen_urls = set()
    seen_ids = set()
    unique = []
    for ch in channels:
        url = ch.get("url")
        cid = ch.get("id")
        if url and cid and url not in seen_urls and cid not in seen_ids:
            seen_urls.add(url)
            seen_ids.add(cid)
            unique.append(ch)
    return unique

def clear_directories():
    delete_split_files(WORKING_CHANNELS_BASE)
    for d in [COUNTRIES_DIR, CATEGORIES_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

def save_channels(channels, country_files, category_files, append=False):
    if not append:
        clear_directories()
    channels = remove_duplicates(channels)
    os.makedirs(COUNTRIES_DIR, exist_ok=True)
    os.makedirs(CATEGORIES_DIR, exist_ok=True)

    if append:
        existing = load_split_json(WORKING_CHANNELS_BASE)
        existing.extend(channels)
        channels = remove_duplicates(existing)

    save_split_json(WORKING_CHANNELS_BASE, channels)

    for country, chs in country_files.items():
        if not country or country == "Unknown":
            continue
        safe = re.sub(r'[^a-zA-Z0-9_-]', '', country)
        if not safe:
            continue
        base = os.path.join(COUNTRIES_DIR, safe)
        if append:
            ex = load_split_json(base)
            ex.extend(chs)
            chs = remove_duplicates(ex)
        save_split_json(base, chs)

    for cat, chs in category_files.items():
        if not cat:
            continue
        safe = re.sub(r'[^a-zA-Z0-9_-]', '', cat)
        if not safe:
            continue
        base = os.path.join(CATEGORIES_DIR, safe)
        if append:
            ex = load_split_json(base)
            ex.extend(chs)
            chs = remove_duplicates(ex)
        save_split_json(base, chs)

def sync_working_channels():
    all_ch = []
    for folder in [COUNTRIES_DIR, CATEGORIES_DIR]:
        if os.path.exists(folder):
            for f in os.listdir(folder):
                if f.endswith('.json'):
                    base = os.path.join(folder, f[:-5])
                    all_ch.extend(load_split_json(base))
    all_ch = remove_duplicates(all_ch)
    save_split_json(WORKING_CHANNELS_BASE, all_ch)
    logging.info(f"Synced {len(all_ch)} channels to working_channels")

def load_existing_data():
    data = {"working_channels": [], "countries": {}, "categories": {}, "all_existing_channels": []}
    data["working_channels"] = load_split_json(WORKING_CHANNELS_BASE)
    data["all_existing_channels"].extend(data["working_channels"])

    if os.path.exists(COUNTRIES_DIR):
        for f in os.listdir(COUNTRIES_DIR):
            if f.endswith(".json"):
                base = os.path.join(COUNTRIES_DIR, f[:-5])
                chs = load_split_json(base)
                data["countries"][f[:-5]] = chs
                data["all_existing_channels"].extend(chs)

    if os.path.exists(CATEGORIES_DIR):
        for f in os.listdir(CATEGORIES_DIR):
            if f.endswith(".json"):
                base = os.path.join(CATEGORIES_DIR, f[:-5])
                chs = load_split_json(base)
                data["categories"][f[:-5]] = chs
                data["all_existing_channels"].extend(chs)

    data["all_existing_channels"] = remove_duplicates(data["all_existing_channels"])
    return data

# ─── World-iptv.club scraper ────────────────────────────────────────────────
def scrape_world_iptv_latest_playlists(max_attempts=8):
    global SCRAPED_M3U_URLS
    SCRAPED_M3U_URLS.clear()

    base = "https://world-iptv.club"
    try:
        r = requests.get(f"{base}/category/iptv/", headers=SCRAPER_HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        logging.error(f"Category page failed: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    articles = soup.find_all("article")

    candidates = []
    for art in articles:
        a = art.find("a", href=True)
        if not a:
            continue
        href = a["href"]
        if not href.startswith("http"):
            href = urljoin(base, href)
        title = (art.find("h2") or art.find("h3") or {}).get_text(" ", strip=True).lower()
        if any(w in title for w in ["free", "world", "full", "latest", "update", "working"]):
            candidates.append(href)
        if re.search(r'\d{1,2}-\d{1,2}-\d{2,4}', href):
            candidates.append(href)

    def date_key(u):
        m = re.search(r'(\d{1,2})-(\d{1,2})-(\d{2,4})', u)
        if m:
            try:
                d, m, y = map(int, m.groups())
                if y < 100:
                    y += 2000
                return date(y, m, d)
            except:
                pass
        return date(2000, 1, 1)

    candidates.sort(key=date_key, reverse=True)
    candidates = candidates[:max_attempts]

    found = []
    for url in candidates:
        try:
            r = requests.get(url, headers=SCRAPER_HEADERS, timeout=20)
            if r.status_code != 200:
                continue
            links = re.findall(
                r'(https?://[^\s\'"]+(?:\.m3u|\.m3u8|get\.php\?.*?type=m3u))',
                r.text, re.IGNORECASE
            )
            for lnk in links:
                lnk = html.unescape(lnk.strip())
                if lnk not in found:
                    found.append(lnk)
        except Exception as e:
            logging.warning(f"Page {url} → {e}")

    working = []
    for u in found[:12]:
        try:
            r = requests.head(u, headers=SCRAPER_HEADERS, timeout=10, allow_redirects=True)
            if r.status_code == 200:
                working.append(u)
        except:
            pass

    SCRAPED_M3U_URLS.extend(working)
    logging.info(f"Scraped {len(SCRAPED_M3U_URLS)} potential fresh M3U links")
    return working

# ─── Stricter checker ───────────────────────────────────────────────────────
class StrictFastChecker:
    def __init__(self):
        self.connector = TCPConnector(limit=MAX_CONCURRENT, force_close=True, enable_cleanup_closed=True)
        self.timeout = ClientTimeout(total=INITIAL_TIMEOUT, connect=6, sock_connect=5, sock_read=12)
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    def has_unwanted_extension(self, url):
        if not url:
            return False
        return any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS)

    async def check_single_url(self, session, url):
        if self.has_unwanted_extension(url):
            return url, False

        quick = ClientTimeout(total=8, connect=4, sock_connect=4, sock_read=6)

        # HEAD first
        if USE_HEAD_METHOD:
            try:
                async with session.head(url, timeout=quick, allow_redirects=True) as resp:
                    if resp.status != 200:
                        return url, False
                    ct = resp.headers.get('Content-Type', '').lower()
                    if 'text/html' in ct and 'video' not in ct:
                        return url, False
            except:
                pass

        # GET + inspect
        try:
            async with session.get(url, timeout=self.timeout, allow_redirects=True) as resp:
                if resp.status != 200:
                    return url, False

                chunk = await resp.content.read(4096)
                try:
                    text = chunk.decode('utf-8', errors='ignore').lower()
                    if any(kw in text for kw in ['#extm3u', '#extinf', '#ext-x-version', '#ext-x-stream-inf']):
                        return url, True
                    if text.strip().startswith(('http://', 'https://')):
                        return url, True
                    logging.debug(f"No HLS markers → rejected {url[:100]}")
                    return url, False
                except UnicodeDecodeError:
                    return url, False  # binary → reject

        except Exception as e:
            logging.debug(f"Reject {url[:90]} → {type(e).__name__}")
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
                return None
        except:
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

# ─── Other functions (Kenya, Uganda, IPTV-org, cleaning) ─────────────────────
# (kept minimal - you can expand them later if needed)

async def process_m3u_urls(session, logos_data, checker, m3u_urls):
    processor = M3UProcessor()
    total_working = 0
    country_files = {}
    category_files = {}

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

        tasks = [checker.check_single_url(session, ch['url']) for ch in channels]
        results = await asyncio.gather(*tasks)

        working = [channels[i] for i, (_, ok) in enumerate(results) if ok]
        formatted = processor.format_channel_data(working, logos_data)
        total_working += len(formatted)

        for ch in formatted:
            c = ch["country"]
            country_files.setdefault(c, []).append(ch)
            for cat in ch["categories"]:
                category_files.setdefault(cat, []).append(ch)

    if total_working > 0:
        save_channels([], country_files, category_files, append=True)
        logging.info(f"Added {total_working} working channels from M3U sources")
    return total_working

async def main():
    logging.info("IPTV collection started...")

    # ── 0. Scrape fresh links from world-iptv.club ──────────────────────────
    logging.info("Searching recent playlists on world-iptv.club...")
    scrape_world_iptv_latest_playlists(max_attempts=8)

    all_m3u_urls = M3U_BASE_SOURCES + SCRAPED_M3U_URLS
    logging.info(f"Processing {len(all_m3u_urls)} M3U sources in total")

    checker = StrictFastChecker()

    async with aiohttp.ClientSession(connector=checker.connector) as session:
        logos_data = await fetch_json(session, LOGOS_URL)
        logging.info(f"Loaded {len(logos_data)} logos")

        # Process all M3U sources (your two + scraped ones)
        m3u_count = await process_m3u_urls(session, logos_data, checker, all_m3u_urls)

        # You can add back Kenya / Uganda / IPTV-org logic here if you want
        # For now focusing on M3U part you emphasized

        sync_working_channels()

    logging.info("Finished.")
    logging.info(f"Final working M3U channels added: {m3u_count}")

async def fetch_json(session, url):
    try:
        async with session.get(url) as r:
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