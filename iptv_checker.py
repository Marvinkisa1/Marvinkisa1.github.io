import aiohttp
import asyncio
import json
import os
import m3u8
from tqdm import tqdm
from aiohttp import ClientTimeout, TCPConnector
from urllib.parse import urlparse, urljoin
import requests
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor
import time
from pathlib import Path
import logging
import sys
from fuzzywuzzy import fuzz
import shutil
from difflib import SequenceMatcher
import html  # For decoding entities like &#038; to &
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# URLs (loaded from environment variables, no defaults to ensure secrecy)
CHANNELS_URL = os.getenv("CHANNELS_URL", "https://iptv-org.github.io/api/channels.json")
STREAMS_URL = os.getenv("STREAMS_URL", "https://iptv-org.github.io/api/streams.json")
LOGOS_URL = os.getenv("LOGOS_URL", "https://iptv-org.github.io/api/logos.json")
KENYA_BASE_URL = os.getenv("KENYA_BASE_URL", "")
UGANDA_API_URL = "https://apps.moochatplus.net/bash/api/api.php?get_posts&page=1&count=100&api_key=cda11bx8aITlKsXCpNB7yVLnOdEGqg342ZFrQzJRetkSoUMi9w"
M3U_URLS = [
    os.getenv("M3U_URL_1", ""),
    os.getenv("M3U_URL_2", "")
]

# Additional M3U for news and XXX to get more channels
ADDITIONAL_M3U = [
    "https://raw.githubusercontent.com/ipstreet312/freeiptv/refs/heads/master/all.m3u",
    "https://raw.githubusercontent.com/abusaeeidx/IPTV-Scraper-Zilla/refs/heads/main/combined-playlist.m3u"
]

# File paths
WORKING_CHANNELS_BASE = "working_channels"
CATEGORIES_DIR = "categories"
COUNTRIES_DIR = "countries"

# Settings - Optimized for speed but still thorough
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 100))  # Increased for efficiency
INITIAL_TIMEOUT = 20  # Increased for reliability
MAX_TIMEOUT = 30  # Balanced maximum timeout
RETRIES = 2  # Reduced for efficiency
BATCH_DELAY = 0.1
BATCH_SIZE = 500
USE_HEAD_METHOD = True
BYTE_RANGE_CHECK = False  # Disabled for broader compatibility
KENYA_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
MAX_CHANNELS_PER_FILE = 4000

# Unwanted extensions for filtering
UNWANTED_EXTENSIONS = ['.mkv', '.mp4', '.avi', '.mov', '.flv', '.wmv']

# Scraper headers
SCRAPER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def delete_split_files(base_name):
    """Delete all split files and the base file if exists."""
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
    """Load data from split JSON files or the base file."""
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
    """Save data to JSON, splitting if exceeds MAX_CHANNELS_PER_FILE."""
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



BASE_URL = "https://world-iptv.club"
CATEGORY_URL = f"{BASE_URL}/category/iptv/"

M3U_KEYWORDS = (
    ".m3u", ".m3u8", "m3u"
    "get.php", "playlist",
    "iptv", "username=", "password="
)

DAYS_BACK = 3


def create_session():
    session = requests.Session()
    session.headers.update(SCRAPER_HEADERS)
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def extract_post_datetime(html_text):
    soup = BeautifulSoup(html_text, "lxml")

    meta = soup.find("meta", property="article:published_time") \
        or soup.find("meta", property="article:modified_time")

    if not meta or not meta.get("content"):
        return None

    try:
        return datetime.fromisoformat(meta["content"])
    except ValueError:
        return None


def is_recent(post_dt, days=DAYS_BACK):
    if not post_dt:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return post_dt >= cutoff


def extract_candidate_m3u_links(page_url, html_text):
    soup = BeautifulSoup(html_text, "lxml")
    found = set()

    for a in soup.select("a[href]"):
        href = html.unescape(a["href"])
        if any(k in href.lower() for k in M3U_KEYWORDS):
            found.add(urljoin(page_url, href))

    # raw text fallback
    for text in soup.stripped_strings:
        if text.startswith("http") and any(k in text.lower() for k in M3U_KEYWORDS):
            found.add(text)

    return list(found)


def is_working_m3u(session, url):
    try:
        head = session.head(url, allow_redirects=True, timeout=10)
        if head.status_code != 200:
            return False

        get = session.get(url, stream=True, timeout=15)
        chunk = next(get.iter_content(chunk_size=2048), b"")

        if b"#EXT" in chunk:
            return True

        ctype = get.headers.get("content-type", "").lower()
        return "mpegurl" in ctype or "m3u" in ctype

    except Exception:
        return False


def scrape_daily_m3u_urls(max_working=5):
    logging.info("🚀 Starting professional IPTV scraper")

    session = create_session()
    working_m3u = []
    page = 1
    stop_pagination = False

    while not stop_pagination:
        page_url = CATEGORY_URL if page == 1 else f"{CATEGORY_URL}page/{page}/"
        logging.info(f"[DISCOVERY] Fetching {page_url}")

        try:
            resp = session.get(page_url, timeout=30)
            if resp.status_code != 200:
                break
        except Exception as e:
            logging.error(f"Category fetch failed: {e}")
            break

        soup = BeautifulSoup(resp.text, "lxml")
        post_links = [
            urljoin(BASE_URL, a["href"])
            for a in soup.select("a[href]")
            if "/iptv" in a["href"]
        ]

        if not post_links:
            break

        for post_url in post_links:
            logging.info(f"[POST] Checking {post_url}")

            try:
                post_resp = session.get(post_url, timeout=30)
                if post_resp.status_code != 200:
                    continue
            except Exception:
                continue

            post_dt = extract_post_datetime(post_resp.text)

            if post_dt and not is_recent(post_dt):
                logging.info("[FILTER] Post too old → stopping pagination")
                stop_pagination = True
                break

            if not post_dt:
                logging.info("[FILTER] No date found → skipping")
                continue

            logging.info(f"[DATE] Post published {post_dt.isoformat()}")

            candidates = extract_candidate_m3u_links(post_url, post_resp.text)
            logging.info(f"[EXTRACT] Found {len(candidates)} candidate links")

            for m3u in candidates:
                if m3u in working_m3u:
                    continue

                logging.info(f"[TEST] {m3u}")
                if is_working_m3u(session, m3u):
                    logging.info("✅ WORKING M3U")
                    working_m3u.append(m3u)

                if len(working_m3u) >= max_working:
                    break

            if len(working_m3u) >= max_working:
                break

        page += 1

    if working_m3u:
        logging.info(f"🎉 Found {len(working_m3u)} working M3U URLs:")
        for i, u in enumerate(working_m3u, 1):
            logging.info(f"{i}. {u}")
    else:
        logging.warning("❌ No working M3U URLs found")

    return working_m3u

# ---------------- CONFIG ---------------- #

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "*/*",
    "Connection": "keep-alive",
}

READ_LIMIT = 8192          # 8 KB is enough to detect HTML + m3u8
SEGMENT_TIMEOUT = 6        # Fast probe
USE_HEAD_METHOD = False    # HEAD is unreliable for streams

# --------------------------------------- #


class FastChecker:
    def __init__(self):
        self.connector = TCPConnector(
            limit=MAX_CONCURRENT,
            force_close=True,
            enable_cleanup_closed=True,
            ttl_dns_cache=300,
        )
        self.timeout = ClientTimeout(total=INITIAL_TIMEOUT)
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    def has_unwanted_extension(self, url):
        return bool(url) and any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS)

    def is_html_response(self, text):
        html_markers = ("<html", "<!doctype", "<body", "<script", "<head")
        return any(marker in text for marker in html_markers)

    def is_valid_m3u8(self, text):
        if "#EXTM3U" not in text:
            return False
        for line in text.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                return True
        return False

    def extract_first_segment(self, playlist_text):
        for line in playlist_text.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                return line
        return None

    async def probe_segment(self, session, playlist_url, segment):
        segment_url = urljoin(playlist_url, segment)
        try:
            async with session.get(
                segment_url,
                timeout=ClientTimeout(total=SEGMENT_TIMEOUT),
                allow_redirects=True,
            ) as resp:
                return resp.status == 200
        except Exception:
            return False

    async def check_single_url(self, session, url):
        if not url or self.has_unwanted_extension(url):
            return url, False

        async with self.semaphore:
            try:
                if USE_HEAD_METHOD:
                    try:
                        async with session.head(url, timeout=self.timeout, allow_redirects=True) as r:
                            if r.status >= 400:
                                return url, False
                    except Exception:
                        pass

                async with session.get(
                    url,
                    timeout=self.timeout,
                    allow_redirects=True,
                ) as response:

                    if response.status != 200:
                        return url, False

                    content_type = response.headers.get("Content-Type", "").lower()
                    raw = await response.content.read(READ_LIMIT)
                    text = raw.decode("utf-8", errors="ignore").lower()

                    if self.is_html_response(text):
                        logging.debug(f"HTML error page detected: {url}")
                        return url, False

                    if url.endswith(".m3u8") or "m3u8" in content_type:
                        if not self.is_valid_m3u8(text):
                            logging.debug(f"Invalid m3u8 playlist: {url}")
                            return url, False

                        segment = self.extract_first_segment(text)
                        if segment:
                            ok = await self.probe_segment(session, url, segment)
                            if not ok:
                                logging.debug(f"Segment probe failed: {url}")
                                return url, False

                    return url, True

            except (aiohttp.ClientError, asyncio.TimeoutError):
                return url, False
            except Exception as e:
                logging.debug(f"Unexpected error {url}: {e}")
                return url, False


class M3UProcessor:
    def __init__(self):
        self.unwanted_extensions = UNWANTED_EXTENSIONS
        self.failed_urls = []

    def has_unwanted_extension(self, url):
        if not url:
            return False
        return any(url.lower().endswith(ext) for ext in self.unwanted_extensions)

    async def fetch_m3u_content(self, session, m3u_url):
        try:
            async with session.get(m3u_url, timeout=ClientTimeout(total=INITIAL_TIMEOUT)) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logging.error(f"Failed to fetch M3U content from {m3u_url}: Status {response.status}")
                    return None
        except Exception as e:
            logging.error(f"Error fetching M3U content from {m3u_url}: {e}")
            return None

    def parse_m3u(self, content):
        channels = []
        current_channel = {}
        
        for line in content.split('\n'):
            line = line.strip()
            if line.startswith('#EXTINF:-1'):
                current_channel = self._parse_extinf_line(line)
            elif line and not line.startswith('#') and current_channel:
                if not self.has_unwanted_extension(line):
                    current_channel['url'] = line
                    channels.append(current_channel)
                current_channel = {}
        
        return channels

    def _parse_extinf_line(self, line):
        attrs = dict(re.findall(r'(\S+)="([^"]*)"', line))
        channel_name = line.split(',')[-1].strip()
        
        country_code = ''
        clean_name = channel_name
        match = re.match(r'^(?:\|([A-Z]{2})\|)|(?:([A-Z]{2}/ ?))', channel_name)
        if match:
            if match.group(1):
                country_code = match.group(1)
            elif match.group(2):
                country_code = match.group(2).strip('/ ')
            prefix_end = match.end()
            clean_name = channel_name[prefix_end:].strip()
        
        return {
            'tvg_id': attrs.get('tvg-ID', ''),
            'tvg_name': attrs.get('tvg-name', ''),
            'tvg_logo': attrs.get('tvg-logo', ''),
            'group_title': attrs.get('group-title', ''),
            'display_name': clean_name,
            'country_code': country_code,
            'raw_name': channel_name
        }

    def _extract_categories(self, group_title):
        if not group_title:
            return ['general']
        parts = [p.strip().lower() for p in group_title.split('/') if p.strip()]
        if len(parts) > 1 and re.match(r'^[a-z]{2}$', parts[0]):
            return parts[1:]
        return parts

    def format_channel_data(self, channels, logos_data):
        formatted_channels = []
        
        for channel in channels:
            if channel['tvg_id']:
                channel_id = channel['tvg_id'].lower()
            else:
                base_id = re.sub(r'[^a-zA-Z0-9]', '', channel['display_name'])
                if not base_id:
                    base_id = re.sub(r'[^a-zA-Z0-9]', '', channel['raw_name'])
                country_code = channel['country_code']
                channel_id = f"{base_id}.{country_code.lower()}" if country_code else base_id.lower()
            
            logo_url = channel.get('tvg_logo', '')
            if not logo_url:
                matching_logos = [l for l in logos_data if l["channel"] == channel_id]
                if matching_logos:
                    logo_url = matching_logos[0]["url"]
            
            formatted_channels.append({
                'name': channel['display_name'],
                'id': channel_id,
                'logo': logo_url,
                'url': channel['url'],
                'categories': self._extract_categories(channel['group_title']),
                'country': channel['country_code']
            })
        
        return formatted_channels

def remove_duplicates(channels):
    seen_urls = set()
    seen_ids = set()
    unique_channels = []
    
    for channel in channels:
        channel_url = channel.get("url")
        channel_id = channel.get("id")
        
        if not channel_url or not channel_id:
            continue
            
        if channel_url not in seen_urls and channel_id not in seen_ids:
            seen_urls.add(channel_url)
            seen_ids.add(channel_id)
            unique_channels.append(channel)
        else:
            logging.info(f"Removed duplicate channel: {channel.get('name')} (URL: {channel_url}, ID: {channel_id})")
    
    return unique_channels

async def fetch_json(session, url):
    try:
        async with session.get(url, headers={"Accept-Encoding": "gzip"}) as response:
            response.raise_for_status()
            text = await response.text()
            return json.loads(text)
    except json.JSONDecodeError as e:
        logging.error(f"JSON decoding error for {url}: {e}")
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
    return []

def load_existing_data():
    existing_data = {
        "working_channels": [],
        "countries": {},
        "categories": {},
        "all_existing_channels": []
    }

    existing_data["working_channels"] = load_split_json(WORKING_CHANNELS_BASE)
    existing_data["all_existing_channels"].extend(existing_data["working_channels"])

    if os.path.exists(COUNTRIES_DIR):
        for filename in os.listdir(COUNTRIES_DIR):
            if filename.endswith(".json") and filename != ".json":
                base = os.path.join(COUNTRIES_DIR, filename[:-5])
                channels = load_split_json(base)
                country = filename[:-5]
                existing_data["countries"][country] = channels
                existing_data["all_existing_channels"].extend(channels)

    if os.path.exists(CATEGORIES_DIR):
        for filename in os.listdir(CATEGORIES_DIR):
            if filename.endswith(".json"):
                base = os.path.join(CATEGORIES_DIR, filename[:-5])
                channels = load_split_json(base)
                category = filename[:-5]
                existing_data["categories"][category] = channels
                existing_data["all_existing_channels"].extend(channels)

    existing_data["all_existing_channels"] = remove_duplicates(existing_data["all_existing_channels"])
    return existing_data

def clear_directories():
    delete_split_files(WORKING_CHANNELS_BASE)
    for dir_path in [COUNTRIES_DIR, CATEGORIES_DIR]:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        os.makedirs(dir_path, exist_ok=True)

def save_channels(channels, country_files, category_files, append=False):
    if not append:
        clear_directories()

    os.makedirs(COUNTRIES_DIR, exist_ok=True)
    os.makedirs(CATEGORIES_DIR, exist_ok=True)

    channels = remove_duplicates(channels)
    
    if append:
        existing_working = load_split_json(WORKING_CHANNELS_BASE)
        existing_working.extend(channels)
        channels = remove_duplicates(existing_working)
    
    save_split_json(WORKING_CHANNELS_BASE, channels)

    for country, country_channels in country_files.items():
        if not country or country == "Unknown":
            continue
        safe_country = "".join(c for c in country if c.isalnum() or c in (' ', '_', '-')).rstrip()
        if not safe_country:
            continue
        
        country_channels = remove_duplicates(country_channels)
        country_base = os.path.join(COUNTRIES_DIR, safe_country)
        
        if append:
            existing_country = load_split_json(country_base)
            existing_country.extend(country_channels)
            country_channels = remove_duplicates(existing_country)
        
        save_split_json(country_base, country_channels)

    for category, category_channels in category_files.items():
        if not category:
            continue
        safe_category = "".join(c for c in category if c.isalnum() or c in (' ', '_', '-')).rstrip()
        if not safe_category:
            continue
        
        category_channels = remove_duplicates(category_channels)
        category_base = os.path.join(CATEGORIES_DIR, safe_category)
        
        if append:
            existing_category = load_split_json(category_base)
            existing_category.extend(category_channels)
            category_channels = remove_duplicates(existing_category)
        
        save_split_json(category_base, category_channels)

def update_logos_for_null_channels(channels, logos_data):
    updated_count = 0
    
    for channel in channels:
        if channel.get("logo") in (None, "null", "", None):
            channel_id = channel.get("id")
            if channel_id:
                matching_logos = [logo for logo in logos_data if logo["channel"] == channel_id]
                if matching_logos:
                    channel["logo"] = matching_logos[0]["url"]
                    updated_count += 1
                    logging.info(f"Updated logo for {channel_id}: {matching_logos[0]['url']}")
    
    logging.info(f"Updated logos for {updated_count} channels with logo: null/empty")
    return channels

async def validate_channels(session, checker, all_existing_channels, iptv_channel_ids, logos_data):
    valid_channels_count = 0
    valid_channels = []
    country_files = {}
    category_files = {}

    all_existing_channels = update_logos_for_null_channels(all_existing_channels, logos_data)

    async def validate_channel(channel):
        async with checker.semaphore:
            channel_url = channel.get("url", "").strip()
            if not channel_url:
                logging.debug(f"Skipping validation - no URL: {channel.get('name')}")
                return None

            if checker.has_unwanted_extension(channel_url):
                logging.debug(f"Skipping - unwanted extension: {channel_url}")
                return None

            ch_id = channel["id"]
            matching_logos = [l for l in logos_data if l["channel"] == ch_id]
            if matching_logos:
                channel["logo"] = matching_logos[0]["url"]

            for retry in range(RETRIES):
                checker.timeout = ClientTimeout(total=min(INITIAL_TIMEOUT * (retry + 1), MAX_TIMEOUT))
                _, is_working = await checker.check_single_url(session, channel_url)
                if is_working:
                    channel_copy = channel.copy()
                    channel_copy["country"] = channel.get("country", "Unknown")
                    channel_copy["categories"] = channel.get("categories", [])
                    valid_channels.append(channel_copy)
                    country = channel_copy["country"]
                    country_files.setdefault(country, []).append(channel_copy)
                    for cat in channel_copy["categories"]:
                        category_files.setdefault(cat, []).append(channel_copy)
                    return channel_copy
                await asyncio.sleep(0.1 * (retry + 1))
            return None

    total_channels = len(all_existing_channels)
    batch_size = 500
    
    for batch_start in range(0, total_channels, batch_size):
        batch_end = min(batch_start + batch_size, total_channels)
        current_batch = all_existing_channels[batch_start:batch_end]
        
        tasks = [validate_channel(channel) for channel in current_batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Exception in validation: {result}")
            elif result:
                valid_channels_count += 1

    save_channels(valid_channels, country_files, category_files, append=False)

    return valid_channels_count

async def check_iptv_channels(session, checker, channels_data, streams_dict, existing_urls, logos_data):
    new_iptv_channels_count = 0
    new_channels = []
    country_files = {}
    category_files = {}

    channels_to_check = [
        channel
        for channel in channels_data
        if channel.get("id") in streams_dict
        and streams_dict[channel["id"]].get("url") not in existing_urls
    ]

    async def process_channel(channel):
        async with checker.semaphore:
            stream = streams_dict[channel["id"]]
            url = stream.get("url", "").strip()
            if not url:
                return None

            if checker.has_unwanted_extension(url):
                return None

            logo_url = ""
            ch_id = channel["id"]
            feed = stream.get("feed")
            
            matching_logos = [l for l in logos_data if l["channel"] == ch_id and l.get("feed") == feed]
            if matching_logos:
                logo_url = matching_logos[0]["url"]
            else:
                channel_logos = [l for l in logos_data if l["channel"] == ch_id]
                if channel_logos:
                    logo_url = channel_logos[0]["url"]

            for retry in range(RETRIES):
                checker.timeout = ClientTimeout(total=min(INITIAL_TIMEOUT * (retry + 1), MAX_TIMEOUT))
                _, is_working = await checker.check_single_url(session, url)
                if is_working:
                    channel_data = {
                        "name": channel.get("name", "Unknown"),
                        "id": channel.get("id"),
                        "logo": logo_url,
                        "url": url,
                        "categories": channel.get("categories", []),
                        "country": channel.get("country", "Unknown"),
                    }
                    new_channels.append(channel_data)
                    country_files.setdefault(channel_data["country"], []).append(channel_data)
                    for cat in channel_data["categories"]:
                        category_files.setdefault(cat, []).append(channel_data)
                    return channel_data
                await asyncio.sleep(0.1 * (retry + 1))
            return None

    total_channels = len(channels_to_check)
    batch_size = 300
    
    for batch_start in range(0, total_channels, batch_size):
        batch_end = min(batch_start + batch_size, total_channels)
        current_batch = channels_to_check[batch_start:batch_end]
        
        tasks = [process_channel(channel) for channel in current_batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Error processing channel: {result}")
            elif result:
                new_iptv_channels_count += 1

    save_channels(new_channels, country_files, category_files, append=True)

    return new_iptv_channels_count

def get_m3u8_from_page(url_data):
    url, index = url_data
    try:
        response = requests.get(url, headers=KENYA_HEADERS, timeout=10)
        m3u8_pattern = r'(https?://[^\s\'"]+\.m3u8)'
        m3u8_links = re.findall(m3u8_pattern, response.text)
        
        valid_m3u8_links = [link for link in m3u8_links if 'youtube' not in link.lower()]
        
        logging.info(f"[{index}] Processed linked page: {url} - Found {len(valid_m3u8_links)} valid m3u8 links")
        return valid_m3u8_links
    except Exception as e:
        logging.error(f"[{index}] Error processing {url}: {str(e)}")
        return []

async def check_single_m3u8_url(session, url, timeout=15):
    if any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS):
        return url, False
        
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status == 200:
                content = await response.text()
                playlist = m3u8.loads(content)
                if playlist.segments or playlist.playlists:
                    logging.info(f"Valid m3u8 found: {url}")
                    return url, True
                else:
                    logging.info(f"m3u8 parsing failed but keeping URL: {url}")
                    return url, True
    except Exception as e:
        logging.error(f"Failed to check {url} (timeout={timeout}s): {e}")
    return url, False

async def check_m3u8_urls(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [check_single_m3u8_url(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        for url, is_valid in results:
            if is_valid:
                return url
        return None

async def scrape_kenya_tv_channels(logos_data):
    start_time = time.time()
    logging.info("Starting Kenya TV scrape...")

    try:
        if not KENYA_BASE_URL:
            logging.error("KENYA_BASE_URL is not set. Skipping Kenya TV scrape.")
            return []

        response = requests.get(KENYA_BASE_URL, headers=KENYA_HEADERS, timeout=10)
        logging.info("Main page downloaded")

        soup = BeautifulSoup(response.text, 'html.parser')

        main_tag = soup.find('main')
        if not main_tag:
            logging.error("No main tag found")
            return []

        section = main_tag.find('section', class_='tv-grid-container')
        if not section:
            logging.error("No tv-grid-container section found")
            return []

        tv_cards = section.find_all('article', class_='tv-card')
        logging.info(f"Found {len(tv_cards)} TV cards")

        results = []
        urls_to_process = []

        for i, card in enumerate(tv_cards, 1):
            img_container = card.find('div', class_='img-container')
            if not img_container:
                continue

            a_tag = img_container.find('a')
            img_tag = img_container.find('img')
            if not a_tag or not img_tag:
                continue

            href = a_tag.get('href', '')
            full_url = href if href.startswith('http') else KENYA_BASE_URL + href
            channel_name = img_tag.get('alt', '').strip()

            if not channel_name:
                continue

            channel_id = f"{re.sub(r'[^a-zA-Z0-9]', '', channel_name).lower()}.ke"

            logo_url = ""
            matching_logos = [l for l in logos_data if l["channel"] == channel_id]
            if matching_logos:
                logo_url = matching_logos[0]["url"]

            channel_data = {
                "name": channel_name,
                "id": channel_id,
                "logo": logo_url,
                "url": None,
                "categories": ["general"],
                "country": "KE"
            }

            results.append(channel_data)
            urls_to_process.append((full_url, i))

            logging.info(f"[{i}/{len(tv_cards)}] Collected: {channel_name}")

        with ThreadPoolExecutor(max_workers=5) as executor:
            m3u8_lists = list(executor.map(get_m3u8_from_page, urls_to_process))

        logging.info("Checking m3u8 URLs for validity...")

        valid_urls = await asyncio.gather(
            *[check_m3u8_urls(url_list) for url_list in m3u8_lists]
        )

        filtered_results = []
        for channel_data, valid_url in zip(results, valid_urls):
            if valid_url:
                channel_data["url"] = valid_url
                filtered_results.append(channel_data)

        logging.info(
            f"Found {len(filtered_results)} working channels "
            f"out of {len(results)} total channels"
        )

        logging.info(f"Completed in {time.time() - start_time:.2f} seconds")

        return remove_duplicates(filtered_results)

    except Exception as e:
        logging.error(f"Error occurred in Kenya TV scrape: {e}")
        return []


async def fetch_and_process_uganda_channels(session, checker, logos_data):
    def normalize(name):
        name = name.lower()
        name = re.sub(r'[^a-z0-9]', '', name)
        return name

    def get_score(a, b):
        if a in b or b in a:
            return 1.0
        else:
            return SequenceMatcher(None, a, b).ratio()

    logging.info("Starting Uganda channels fetch...")
    api_url = UGANDA_API_URL
    try:
        async with session.get(api_url) as response:
            if response.status == 200:
                data = await response.json()
                posts = data.get("posts", [])
                logging.info(f"Fetched {len(posts)} posts from Uganda API")
            else:
                logging.error(f"Failed to fetch Uganda API: Status {response.status}")
                return 0
    except Exception as e:
        logging.error(f"Error fetching Uganda API: {e}")
        return 0

    ug_logos = [l for l in logos_data if str(l["channel"]).lower().endswith('.ug')]
    channels = []
    country_files = {"UG": []}
    category_files = {}
    match_threshold = 0.8

    async def process_post(post):
        name = str(post.get("channel_name", "").strip())
        if not name:
            return None
        url = post.get("channel_url", "").strip()
        if not url:
            return None
           
        if any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS):
            logging.info(f"Skipping unwanted extension URL for channel: {name}")
            return None
           
        category = post.get("category_name", "").lower().strip() or "entertainment"

        logo = ""
        best_logo_data = None
        best_score = 0
       
        norm_inp = normalize(name)
       
        for logo_data in ug_logos:
            logo_channel = logo_data["channel"]
            norm_key = normalize(logo_channel.split('.')[0])
           
            score = get_score(norm_inp, norm_key)
           
            if score > best_score:
                best_score = score
                best_logo_data = logo_data
       
        ch_id = None
        if best_logo_data and best_score >= match_threshold:
            logo = best_logo_data["url"]
            ch_id = best_logo_data['channel']
            logging.info(f"Logo match for {name} (ID: {ch_id}): {logo} with score {best_score:.2f}")
        else:
            base_id = norm_inp
            ch_id = f"{base_id}.ug"
            logging.info(f"No good logo match for {name} (best score: {best_score:.2f}) | No logo | ID: {ch_id}")
       
        channel = {
            "name": name,
            "id": ch_id,
            "logo": logo,
            "url": url,
            "categories": [category],
            "country": "UG"
        }

        is_working = False
        for retry in range(RETRIES):
            checker.timeout = ClientTimeout(total=min(INITIAL_TIMEOUT * (retry + 1), MAX_TIMEOUT))
            _, is_working = await checker.check_single_url(session, url)
            if is_working:
                break
            await asyncio.sleep(0.1 * (retry + 1))
        if is_working:
            return channel
        return None

    tasks = [process_post(post) for post in posts]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            logging.error(f"Error processing Uganda post: {result}")
        elif result:
            channels.append(result)
            country_files["UG"].append(result)
            cat = result["categories"][0]
            category_files.setdefault(cat, []).append(result)

    if channels:
        save_channels(channels, country_files, category_files, append=True)
        logging.info(f"Added {len(channels)} working Uganda channels")

    return len(channels)

async def clean_and_replace_channels(session, checker, all_channels, streams_dict, m3u_channels, logos_data):
    logging.info("\n=== Step 5: Cleaning non-working channels and replacing URLs ===")
    
    all_channels = update_logos_for_null_channels(all_channels, logos_data)
    
    valid_channels = []
    replaced_channels = 0
    non_working_channels = 0
    country_files = {}
    category_files = {}

    async def find_replacement_url(channel, streams_dict, m3u_channels, session, checker):
        channel_id = channel.get("id")
        channel_name = channel.get("name", "").lower()

        if streams_dict and channel_id in streams_dict:
            new_url = streams_dict[channel_id].get("url", "").strip()
            if new_url and not checker.has_unwanted_extension(new_url):
                for retry in range(RETRIES):
                    checker.timeout = ClientTimeout(total=min(INITIAL_TIMEOUT * (retry + 1), MAX_TIMEOUT))
                    _, is_working = await checker.check_single_url(session, new_url)
                    if is_working:
                        logging.info(f"Found IPTV-org replacement for {channel_name}: {new_url}")
                        return new_url
                    await asyncio.sleep(0.3 * (retry + 1))

        if m3u_channels:
            for m3u_channel in m3u_channels:
                m3u_name = m3u_channel.get("display_name", "").lower()
                if fuzz.ratio(channel_name, m3u_name) > 80:
                    new_url = m3u_channel.get("url", "").strip()
                    if new_url and not checker.has_unwanted_extension(new_url):
                        for retry in range(RETRIES):
                            checker.timeout = ClientTimeout(total=min(INITIAL_TIMEOUT * (retry + 1), MAX_TIMEOUT))
                            _, is_working = await checker.check_single_url(session, new_url)
                            if is_working:
                                logging.info(f"Found M3U replacement for {channel_name}: {new_url}")
                                return new_url
                            await asyncio.sleep(0.3 * (retry + 1))

        logging.info(f"No replacement found for {channel_name}")
        return None

    async def check_and_process_channel(channel):
        nonlocal valid_channels, non_working_channels, replaced_channels
        channel_url = channel.get("url", "").strip()
        channel_name = channel.get("name", "Unknown")
        channel_id = channel.get("id", "no-id")

        if not channel_url:
            logging.info(f"REMOVED - no valid URL: {channel_name} (id: {channel_id})")
            non_working_channels += 1
            return

        if checker.has_unwanted_extension(channel_url):
            logging.info(f"REMOVED - unwanted extension: {channel_name} ({channel_url})")
            non_working_channels += 1
            return

        matching_logos = [logo for logo in logos_data if logo["channel"] == channel_id]
        if matching_logos:
            channel["logo"] = matching_logos[0]["url"]

        is_working = False
        for retry in range(RETRIES):
            checker.timeout = ClientTimeout(total=min(INITIAL_TIMEOUT * (retry + 1), MAX_TIMEOUT))
            _, is_working = await checker.check_single_url(session, channel_url)
            if is_working:
                break
            await asyncio.sleep(0.1 * (retry + 1))

        if is_working:
            valid_channels.append(channel)
            country = channel.get("country", "Unknown")
            if country and country != "Unknown":
                country_files.setdefault(country, []).append(channel)
            for cat in channel.get("categories", []):
                if cat:
                    category_files.setdefault(cat, []).append(channel)
            return

        logging.info(f"Channel not working: {channel_name} ({channel_url}). Trying replacement...")
        new_url = await find_replacement_url(channel, streams_dict, m3u_channels, session, checker)
        if new_url:
            channel["url"] = new_url
            valid_channels.append(channel)
            country = channel.get("country", "Unknown")
            if country and country != "Unknown":
                country_files.setdefault(country, []).append(channel)
            for cat in channel.get("categories", []):
                if cat:
                    category_files.setdefault(cat, []).append(channel)
            replaced_channels += 1
            logging.info(f"REPLACED → {channel_name} now uses: {new_url}")
        else:
            logging.info(f"REMOVED - no replacement: {channel_name} ({channel_url})")
            non_working_channels += 1

    total_channels = len(all_channels)
    logging.info(f"Cleaning {total_channels} channels...")

    batch_size = 400
    
    for batch_start in range(0, total_channels, batch_size):
        batch_end = min(batch_start + batch_size, total_channels)
        current_batch = all_channels[batch_start:batch_end]
        
        tasks = [check_and_process_channel(channel) for channel in current_batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Error in cleaning batch: {result}")

    save_channels(valid_channels, country_files, category_files, append=False)

    logging.info(f"Replaced {replaced_channels} channels with new URLs")
    logging.info(f"Removed {non_working_channels} non-working / invalid channels")
    logging.info(f"Total channels after cleaning: {len(valid_channels)}")

    return len(valid_channels), non_working_channels, replaced_channels

def sync_working_channels():
    logging.info("Syncing all channels to working_channels...")
    
    all_channels = []
    
    if os.path.exists(COUNTRIES_DIR):
        for filename in os.listdir(COUNTRIES_DIR):
            if filename.endswith(".json"):
                base = os.path.join(COUNTRIES_DIR, filename[:-5])
                channels = load_split_json(base)
                all_channels.extend(remove_duplicates(channels))
    
    if os.path.exists(CATEGORIES_DIR):
        for filename in os.listdir(CATEGORIES_DIR):
            if filename.endswith(".json"):
                base = os.path.join(CATEGORIES_DIR, filename[:-5])
                channels = load_split_json(base)
                all_channels.extend(remove_duplicates(channels))
    
    all_channels = remove_duplicates(all_channels)
    
    save_split_json(WORKING_CHANNELS_BASE, all_channels)
    
    logging.info(f"Synced {len(all_channels)} channels to working_channels")

async def process_m3u_urls(session, logos_data, checker, m3u_urls):
    logging.info("\n=== Step 2: Processing M3U URLs ===")
    processor = M3UProcessor()
    all_channels = []
    
    for m3u_url in m3u_urls:
        if not m3u_url:
            continue
            
        logging.info(f"Processing M3U URL: {m3u_url}")
        content = await processor.fetch_m3u_content(session, m3u_url)
        if content:
            channels = processor.parse_m3u(content)
            logging.info(f"Found {len(channels)} channels in {m3u_url}")
            
            check_tasks = [checker.check_single_url(session, channel['url']) for channel in channels if channel.get('url')]
            check_results = await asyncio.gather(*check_tasks)
            
            working_channels = []
            for i, (url, is_working) in enumerate(check_results):
                if is_working:
                    working_channels.append(channels[i])
            
            logging.info(f"Found {len(working_channels)} working channels in {m3u_url}")
            
            formatted_channels = processor.format_channel_data(working_channels, logos_data)
            all_channels.extend(formatted_channels)
    
    if all_channels:
        country_files = {}
        category_files = {}
        
        for channel in all_channels:
            country = channel.get("country", "Unknown")
            country_files.setdefault(country, []).append(channel)
            
            for category in channel.get("categories", ["general"]):
                category_files.setdefault(category, []).append(channel)
        
        save_channels(all_channels, country_files, category_files, append=True)
        logging.info(f"Added {len(all_channels)} working channels from M3U URLs")
    
    return len(all_channels)

async def main():
    global M3U_URLS
    
    logging.info("Starting IPTV channel collection process...")
    
    logging.info("\n=== Step 0: Scraping daily M3U URLs ===")
    scraped_m3u = scrape_daily_m3u_urls(max_working=5)
    M3U_URLS = scraped_m3u + ADDITIONAL_M3U
    logging.info(f"Updated M3U_URLS with {len(M3U_URLS)} URLs (scraped + additional)")
    
    checker = FastChecker()
    
    async with aiohttp.ClientSession(connector=checker.connector) as session:
        logos_data = await fetch_json(session, LOGOS_URL)
        logging.info(f"Loaded {len(logos_data)} logos from {LOGOS_URL}")
        
        logging.info("\n=== Step 1: Scraping Kenya TV channels ===")
        kenya_channels = await scrape_kenya_tv_channels(logos_data)
        
        if kenya_channels:
            country_files = {}
            category_files = {}
            
            for channel in kenya_channels:
                country = channel.get("country", "KE")
                country_files.setdefault(country, []).append(channel)
                
                for category in channel.get("categories", ["general"]):
                    category_files.setdefault(category, []).append(channel)
            
            save_channels(kenya_channels, country_files, category_files, append=True)
            logging.info(f"Added {len(kenya_channels)} Kenya channels")

        logging.info("\n=== Step 1.5: Scraping Uganda channels ===")
        ug_channels_count = await fetch_and_process_uganda_channels(session, checker, logos_data)
        
        m3u_channels_count = await process_m3u_urls(session, logos_data, checker, M3U_URLS)
        
        logging.info("\n=== Step 3: Checking IPTV-org channels ===")
        try:
            if not CHANNELS_URL or not STREAMS_URL:
                logging.error("CHANNELS_URL or STREAMS_URL is not set. Skipping IPTV-org channels.")
                streams_dict = {}
                channels_data = []
            else:
                channels_data, streams_data = await asyncio.gather(
                    fetch_json(session, CHANNELS_URL),
                    fetch_json(session, STREAMS_URL),
                )

                streams_dict = {stream["channel"]: stream for stream in streams_data if stream.get("channel")}
                iptv_channel_ids = set(streams_dict.keys())

                existing_data = load_existing_data()
                all_existing_channels = existing_data["all_existing_channels"]
                existing_urls = {ch.get("url") for ch in all_existing_channels if ch.get("url")}

                valid_channels_count = await validate_channels(
                    session, checker, all_existing_channels, iptv_channel_ids, logos_data
                )

                new_iptv_channels_count = await check_iptv_channels(
                    session, checker, channels_data, streams_dict, existing_urls, logos_data
                )

                total_channels = valid_channels_count + new_iptv_channels_count + m3u_channels_count + ug_channels_count + len(kenya_channels)
                logging.info(f"\nTotal working channels before cleaning: {total_channels}")
                logging.info(f"Working manual channels: {valid_channels_count}")
                logging.info(f"New working IPTV channels: {new_iptv_channels_count}")
                logging.info(f"New working M3U channels: {m3u_channels_count}")
                logging.info(f"New working Uganda channels: {ug_channels_count}")
                logging.info(f"New working Kenya channels: {len(kenya_channels)}")
        except Exception as e:
            logging.error(f"Error in IPTV-org processing: {e}")
            streams_dict = {}
            channels_data = []

        logging.info("\n=== Step 4: Syncing channels ===")
        sync_working_channels()

        logging.info("\n=== Step 5: Cleaning non-working channels and replacing URLs ===")
        existing_data = load_existing_data()
        all_existing_channels = existing_data["all_existing_channels"]
        
        m3u_channels = []
        processor = M3UProcessor()
        for m3u_url in M3U_URLS:
            if not m3u_url:
                continue
            content = await processor.fetch_m3u_content(session, m3u_url)
            if content:
                channels = processor.parse_m3u(content)
                m3u_channels.extend(channels)
        
        valid_channels_count, non_working_count, replaced_count = await clean_and_replace_channels(
            session, checker, all_existing_channels, streams_dict, m3u_channels, logos_data
        )

        logging.info("\n=== Step 6: Syncing updated channels ===")
        sync_working_channels()

        logging.info("\n=== Process completed ===")
        logging.info(f"Final count: {valid_channels_count} channels")
        logging.info(f"Removed non-working channels: {non_working_count}")
        logging.info(f"Channels replaced: {replaced_count}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Script failed: {e}")
        sys.exit(1)