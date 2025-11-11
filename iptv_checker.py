import aiohttp
import asyncio
import json
import os
import m3u8
from tqdm import tqdm
from aiohttp import ClientTimeout, TCPConnector
from urllib.parse import urlparse
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

# File paths
WORKING_CHANNELS_FILE = "working_channels.json"
CATEGORIES_DIR = "categories"
COUNTRIES_DIR = "countries"

# Settings - Optimized for speed but still thorough
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 80))  # Balanced concurrency
INITIAL_TIMEOUT = 15  # Balanced timeout
MAX_TIMEOUT = 30  # Balanced maximum timeout
RETRIES = 4  # Balanced retries
BATCH_DELAY = 0.1
BATCH_SIZE = 500
USE_HEAD_METHOD = True
BYTE_RANGE_CHECK = True
KENYA_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# Unwanted extensions for filtering
UNWANTED_EXTENSIONS = ['.mkv', '.mp4', '.avi', '.mov', '.flv', '.wmv']

class FastChecker:
    def __init__(self):
        self.connector = TCPConnector(
            limit=MAX_CONCURRENT,
            force_close=True,
            enable_cleanup_closed=True,
            ttl_dns_cache=300
        )
        self.timeout = ClientTimeout(total=INITIAL_TIMEOUT)
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    def has_unwanted_extension(self, url):
        """Check if URL has unwanted video file extension"""
        if not url:
            return False
        return any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS)

    async def check_single_url(self, session, url):
        """Efficient but thorough URL checker"""
        # Skip URLs with unwanted extensions
        if self.has_unwanted_extension(url):
            return url, False
            
        try:
            # First try HEAD request (fastest)
            if USE_HEAD_METHOD:
                try:
                    async with session.head(url, timeout=self.timeout, allow_redirects=True) as response:
                        if response.status == 200:
                            content_type = response.headers.get('Content-Type', '').lower()
                            if any(x in content_type for x in ['video', 'mpegurl', 'application/octet-stream']):
                                return url, True
                            return url, True
                        elif response.status in [301, 302, 307, 308]:
                            redirect_url = response.headers.get('Location')
                            if redirect_url:
                                if not redirect_url.startswith('http'):
                                    parsed = urlparse(url)
                                    redirect_url = f"{parsed.scheme}://{parsed.netloc}{redirect_url}"
                                # Check if redirect URL has unwanted extension
                                if self.has_unwanted_extension(redirect_url):
                                    return url, False
                                try:
                                    async with session.head(redirect_url, timeout=self.timeout, allow_redirects=False) as redirect_response:
                                        if redirect_response.status in (200, 301, 302, 307, 308):
                                            return url, True
                                except:
                                    pass
                        return url, False
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    pass
            
            headers = {'Range': 'bytes=0-1'} if BYTE_RANGE_CHECK else {}
            try:
                async with session.get(url, timeout=self.timeout, headers=headers, 
                                      allow_redirects=True) as response:
                    if response.status in (200, 206):
                        if url.endswith('.m3u8') or 'm3u8' in response.headers.get('Content-Type', ''):
                            try:
                                content = await response.content.read(1024)
                                content_str = content.decode('utf-8', errors='ignore')
                                if '#EXTM3U' in content_str:
                                    return url, True
                            except:
                                return url, True
                        return url, True
                    return url, False
            except (aiohttp.ClientError, asyncio.TimeoutError):
                return url, False
        except Exception:
            return url, False

class M3UProcessor:
    def __init__(self):
        self.connector = aiohttp.TCPConnector(limit=50, force_close=True)
        self.timeout = aiohttp.ClientTimeout(total=15)
        self.semaphore = asyncio.Semaphore(50)
        self.unwanted_extensions = UNWANTED_EXTENSIONS
        self.failed_urls = []

    def has_unwanted_extension(self, url):
        """Check if URL has unwanted video file extension"""
        if not url:
            return False
        return any(url.lower().endswith(ext) for ext in self.unwanted_extensions)

    async def fetch_m3u_content(self, session, m3u_url):
        """Fetch M3U content from URL"""
        try:
            async with session.get(m3u_url, timeout=self.timeout) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logging.error(f"Failed to fetch M3U content from {m3u_url}: Status {response.status}")
                    return None
        except Exception as e:
            logging.error(f"Error fetching M3U content from {m3u_url}: {e}")
            return None

    def parse_m3u(self, content):
        """Parse M3U content and extract channel information"""
        channels = []
        current_channel = {}
        
        for line in content.split('\n'):
            line = line.strip()
            if line.startswith('#EXTINF:-1'):
                current_channel = self._parse_extinf_line(line)
            elif line and not line.startswith('#') and current_channel:
                # Skip URLs with unwanted extensions
                if not self.has_unwanted_extension(line):
                    current_channel['url'] = line
                    channels.append(current_channel)
                current_channel = {}
        
        return channels

    def _parse_extinf_line(self, line):
        """Parse EXTINF line and extract metadata"""
        attrs = dict(re.findall(r'(\S+)="([^"]*)"', line))
        channel_name = line.split(',')[-1].strip()
        
        country_code = ''
        country_match = re.match(r'^([A-Z]{2})\s*▎', channel_name)
        if country_match:
            country_code = country_match.group(1)
        
        clean_name = re.sub(r'^[A-Z]{2}\s*▎\s*|[◉•★ʀᴀᴡᴴᴰ]', '', channel_name).strip()
        
        return {
            'tvg_id': attrs.get('tvg-ID', ''),
            'tvg_name': attrs.get('tvg-name', clean_name),
            'tvg_logo': attrs.get('tvg-logo', ''),
            'group_title': attrs.get('group-title', ''),
            'display_name': clean_name,
            'country_code': country_code,
            'raw_name': channel_name
        }

    async def check_url(self, session, url, retries=2):
        """Efficient URL checking"""
        # Skip URLs with unwanted extensions
        if self.has_unwanted_extension(url):
            return False
            
        parsed_url = urlparse(url)
        domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
        headers = {
            'User-Agent': 'VLC/3.0.16 LibVLC/3.0.16',
            'Referer': domain,
        }
        
        for attempt in range(retries):
            try:
                async with self.semaphore:
                    try:
                        async with session.head(url, headers=headers, timeout=self.timeout, 
                                              allow_redirects=True) as response:
                            if response.status in (200, 206, 301, 302, 307, 308):
                                return True
                    except (aiohttp.ClientError, asyncio.TimeoutError):
                        pass
                    
                    try:
                        async with session.get(url, headers={**headers, 'Range': 'bytes=0-1'}, 
                                             timeout=self.timeout, allow_redirects=True) as response:
                            if response.status in (200, 206):
                                return True
                    except (aiohttp.ClientError, asyncio.TimeoutError):
                        pass
                
                await asyncio.sleep(0.2 * (attempt + 1))
            except Exception as e:
                self.failed_urls.append((url, f"Attempt {attempt + 1}: {str(e)}"))
                await asyncio.sleep(0.2 * (attempt + 1))
        
        return False

    async def filter_working_channels(self, session, channels):
        """Filter channels to only include working streams"""
        working_channels = []
        
        tasks = [self.check_url(session, channel['url']) for channel in channels]
        
        for i, task in tqdm(enumerate(asyncio.as_completed(tasks)), total=len(tasks), desc="Checking M3U streams"):
            is_working = await task
            if is_working:
                working_channels.append(channels[i])
        
        return working_channels

    def format_channel_data(self, channels, logos_data):
        """Format channel data into the desired JSON structure with logos from LOGOS_URL"""
        formatted_channels = []
        
        for channel in channels:
            base_id = re.sub(r'[^a-zA-Z0-9]', '', channel['tvg_name'] or channel['display_name'])
            if not base_id:
                base_id = re.sub(r'[^a-zA-Z0-9]', '', channel['raw_name'])
            
            country_code = channel['country_code']
            channel_id = f"{base_id.lower()}.{country_code.lower()}" if country_code else base_id.lower()
            
            # Get logo from logos_data
            logo_url = ""
            matching_logos = [l for l in logos_data if l["channel"] == channel_id]
            if matching_logos:
                logo_url = matching_logos[0]["url"]
            
            formatted_channels.append({
                'name': channel['display_name'],
                'id': channel_id,
                'logo': logo_url,
                'url': channel['url'],
                'categories': ['sports'],
                'country': channel['country_code']
            })
        
        return formatted_channels

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

def remove_duplicates(channels):
    """Remove duplicate channels by URL and ID"""
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

def load_existing_data():
    """Load all existing channel data from files, ensuring consistency."""
    existing_data = {
        "working_channels": [],
        "countries": {},
        "categories": {},
        "all_existing_channels": []
    }

    if os.path.exists(WORKING_CHANNELS_FILE):
        with open(WORKING_CHANNELS_FILE, "r", encoding="utf-8") as f:
            channels = json.load(f)
            channels = remove_duplicates(channels)
            for channel in channels:
                channel["country"] = channel.get("country", "Unknown")
                channel["categories"] = channel.get("categories", [])
            existing_data["working_channels"] = channels
            existing_data["all_existing_channels"].extend(channels)

    if os.path.exists(COUNTRIES_DIR):
        for filename in os.listdir(COUNTRIES_DIR):
            if filename.endswith(".json") and filename != ".json":
                country = filename[:-5]
                with open(os.path.join(COUNTRIES_DIR, filename), "r", encoding="utf-8") as f:
                    channels = json.load(f)
                    channels = remove_duplicates(channels)
                    for channel in channels:
                        channel["country"] = channel.get("country", "Unknown")
                        channel["categories"] = channel.get("categories", [])
                    existing_data["countries"][country] = channels
                    existing_data["all_existing_channels"].extend(channels)

    if os.path.exists(CATEGORIES_DIR):
        for filename in os.listdir(CATEGORIES_DIR):
            if filename.endswith(".json"):
                category = filename[:-5]
                with open(os.path.join(CATEGORIES_DIR, filename), "r", encoding="utf-8") as f:
                    channels = json.load(f)
                    channels = remove_duplicates(channels)
                    for channel in channels:
                        channel["country"] = channel.get("country", "Unknown")
                        channel["categories"] = channel.get("categories", [])
                    existing_data["categories"][category] = channels
                    existing_data["all_existing_channels"].extend(channels)

    existing_data["all_existing_channels"] = remove_duplicates(existing_data["all_existing_channels"])
    return existing_data

def clear_directories():
    """Clear all JSON files in countries and categories directories to prevent stale data."""
    for dir_path in [COUNTRIES_DIR, CATEGORIES_DIR]:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        os.makedirs(dir_path, exist_ok=True)

def save_channels(channels, country_files, category_files, append=False):
    """Save channels to files - can replace or append to existing content.
    When append=False, clears directories first to ensure no stale files remain.
    """
    if not append:
        clear_directories()
        if os.path.exists(WORKING_CHANNELS_FILE):
            os.remove(WORKING_CHANNELS_FILE)

    os.makedirs(COUNTRIES_DIR, exist_ok=True)
    os.makedirs(CATEGORIES_DIR, exist_ok=True)

    channels = remove_duplicates(channels)
    
    if append and os.path.exists(WORKING_CHANNELS_FILE):
        with open(WORKING_CHANNELS_FILE, "r", encoding="utf-8") as f:
            current_channels = json.load(f)
        current_channels.extend(channels)
        channels = remove_duplicates(current_channels)
    
    # Write (replace or updated) working channels file
    with open(WORKING_CHANNELS_FILE, "w", encoding="utf-8") as f:
        json.dump(channels, f, indent=4, ensure_ascii=False)

    # For country files
    for country, country_channels in country_files.items():
        if not country or country == "Unknown":
            continue
        safe_country = "".join(c for c in country if c.isalnum() or c in (' ', '_', '-')).rstrip()
        if not safe_country:
            continue
        
        country_channels = remove_duplicates(country_channels)
        country_file = os.path.join(COUNTRIES_DIR, f"{safe_country}.json")
        
        if append and os.path.exists(country_file):
            with open(country_file, "r", encoding="utf-8") as f:
                current_country_channels = json.load(f)
            current_country_channels.extend(country_channels)
            country_channels = remove_duplicates(current_country_channels)
        
        # Write (replace or updated)
        with open(country_file, "w", encoding="utf-8") as f:
            json.dump(country_channels, f, indent=4, ensure_ascii=False)

    # For category files
    for category, category_channels in category_files.items():
        if not category:
            continue
        safe_category = "".join(c for c in category if c.isalnum() or c in (' ', '_', '-')).rstrip()
        if not safe_category:
            continue
        
        category_channels = remove_duplicates(category_channels)
        category_file = os.path.join(CATEGORIES_DIR, f"{safe_category}.json")
        
        if append and os.path.exists(category_file):
            with open(category_file, "r", encoding="utf-8") as f:
                current_category_channels = json.load(f)
            current_category_channels.extend(category_channels)
            category_channels = remove_duplicates(current_category_channels)
        
        # Write (replace or updated)
        with open(category_file, "w", encoding="utf-8") as f:
            json.dump(category_channels, f, indent=4, ensure_ascii=False)

def update_logos_for_null_channels(channels, logos_data):
    """Update logos for channels with logo: null using logos.json data"""
    updated_count = 0
    
    for channel in channels:
        if channel.get("logo") is None or channel.get("logo") == "null" or not channel.get("logo"):
            channel_id = channel.get("id")
            if channel_id:
                matching_logos = [logo for logo in logos_data if logo["channel"] == channel_id]
                if matching_logos:
                    channel["logo"] = matching_logos[0]["url"]
                    updated_count += 1
                    logging.info(f"Updated logo for {channel_id}: {matching_logos[0]['url']}")
    
    logging.info(f"Updated logos for {updated_count} channels with logo: null")
    return channels

async def validate_channels(session, checker, all_existing_channels, iptv_channel_ids, logos_data):
    """Validate existing channels and collect only working ones."""
    valid_channels_count = 0
    valid_channels = []
    country_files = {}
    category_files = {}

    all_existing_channels = update_logos_for_null_channels(all_existing_channels, logos_data)

    async def validate_channel(channel):
        async with checker.semaphore:
            channel_url = channel.get("url")
            if not channel_url:
                return None

            # Skip URLs with unwanted extensions
            if checker.has_unwanted_extension(channel_url):
                return None

            # Ensure logo is from logos_data
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
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Validating channels {batch_start}-{batch_end}"):
            try:
                result = await future
                if result:
                    valid_channels_count += 1
            except Exception as e:
                logging.error(f"Error processing channel: {e}")

    save_channels(valid_channels, country_files, category_files, append=False)

    return valid_channels_count

async def check_iptv_channels(session, checker, channels_data, streams_dict, existing_urls, logos_data):
    """Check and add new IPTV channels that are working."""
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
            url = stream.get("url")
            if not url:
                return None

            # Skip URLs with unwanted extensions
            if checker.has_unwanted_extension(url):
                return None

            # Get logo from logos_data, prioritizing feed match
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
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Testing new IPTV channels {batch_start}-{batch_end}"):
            try:
                result = await future
                if result:
                    new_iptv_channels_count += 1
            except Exception as e:
                logging.error(f"Error processing channel: {e}")

    save_channels(new_channels, country_files, category_files, append=True)

    return new_iptv_channels_count

def get_m3u8_from_page(url_data):
    """Extract m3u8 URLs from a page."""
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
    """Check if a single m3u8 URL is valid."""
    # Skip URLs with unwanted extensions
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
    """Check multiple m3u8 URLs and return the first working one."""
    async with aiohttp.ClientSession() as session:
        tasks = [check_single_m3u8_url(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        for url, is_valid in results:
            if is_valid:
                return url
        return None

def scrape_kenya_tv_channels(logos_data):
    """Scrape Kenya TV channels and assign logos from LOGOS_URL."""
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
            if img_container:
                a_tag = img_container.find('a')
                if a_tag:
                    img_tag = a_tag.find('img')
                    if img_tag:
                        href = a_tag.get('href', '')
                        full_url = href if href.startswith('http') else KENYA_BASE_URL + href
                        channel_name = img_tag.get('alt', '')
                        channel_id = f"{re.sub(r'[^a-zA-Z0-9]', '', channel_name).lower()}.ke"
                        
                        # Get logo from logos_data
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
                        logging.info(f"[{i}/{len(tv_cards)}] Collected basic info: {channel_data['name']}")
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            m3u8_lists = list(executor.map(get_m3u8_from_page, urls_to_process))
        
        logging.info("Checking m3u8 URLs for validity...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        valid_urls = loop.run_until_complete(asyncio.gather(
            *[check_m3u8_urls(url_list) for url_list in m3u8_lists]
        ))
        
        filtered_results = []
        for channel_data, valid_url in zip(results, valid_urls):
            if valid_url:
                channel_data['url'] = valid_url
                filtered_results.append(channel_data)
            
        logging.info(f"Found {len(filtered_results)} channels with working m3u8 URLs out of {len(results)} total channels")
        logging.info(f"Completed in {time.time() - start_time:.2f} seconds")
        
        return remove_duplicates(filtered_results)
        
    except Exception as e:
        logging.error(f"Error occurred in Kenya TV scrape: {str(e)}")
        return []

async def fetch_and_process_uganda_channels(session, checker, logos_data):
    """Fetch and process Uganda channels from API, check working URLs, assign logos, and save."""
    def normalize(name):
        # Normalize by lowercasing and removing non-alphanumeric characters
        name = name.lower()
        name = re.sub(r'[^a-z0-9]', '', name)
        return name

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

    # Pre-filter Uganda logos
    ug_logos = [l for l in logos_data if l["channel"].endswith(".ug")]

    channels = []
    country_files = {"UG": []}
    category_files = {}

    # Threshold for a "good" match (adjust as needed, e.g., 0.8 means 80% similarity)
    match_threshold = 0.8

    async def process_post(post):
        firstName = str(post.get("channel_name", "").strip())
        splittedName = firstName.split(' ')
        name = ''.join(splittedName)

        if not name:
            return None
        url = post.get("channel_url")
        if not url:
            return None
            
        # Skip URLs with unwanted extensions
        if any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS):
            logging.info(f"Skipping unwanted extension URL for channel: {name}")
            return None
            
        category = post.get("category_name", "").lower().strip()
        if not category:
            category = "entertainment"  # default

        # Use regex to clean the channel name for ID construction
        base_id = re.sub(r'[^a-zA-Z0-9]', '', name).lower()
        ch_id = f"{base_id}.ug"

        # Improved logo search using fuzzy matching
        logo = ""
        best_logo_data = None
        best_score = 0
        
        # Normalize the input name for searching
        norm_inp = normalize(firstName)
        
        for logo_data in ug_logos:
            logo_channel = logo_data["channel"]
            # Normalize the key, removing the domain extension like .ug
            norm_key = normalize(logo_channel.split('.')[0])
            
            # Calculate similarity score using SequenceMatcher
            score = SequenceMatcher(None, norm_inp, norm_key).ratio()
            
            if score > best_score:
                best_score = score
                best_logo_data = logo_data
        
        if best_logo_data and best_score >= match_threshold:
            logo = best_logo_data["url"]
            logging.info(f"Logo match for {name} (ID: {ch_id}): {best_logo_data['channel']} with similarity {best_score:.2f}")

        channel = {
            "name": name,
            "id": ch_id,
            "logo": logo,
            "url": url,
            "categories": [category],
            "country": "UG"
        }

        # Check if working
        is_working = False
        for retry in range(2):
            checker.timeout = ClientTimeout(total=min(INITIAL_TIMEOUT * (retry + 1), MAX_TIMEOUT))
            _, is_working = await checker.check_single_url(session, url)
            if is_working:
                break
            await asyncio.sleep(0.1 * (retry + 1))

        if is_working:
            return channel
        return None

    tasks = [process_post(post) for post in posts]
    for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing Uganda channels"):
        try:
            result = await future
            if result:
                channels.append(result)
                country_files["UG"].append(result)
                cat = result["categories"][0]
                category_files.setdefault(cat, []).append(result)
        except Exception as e:
            logging.error(f"Error processing Uganda post: {e}")

    if channels:
        save_channels(channels, country_files, category_files, append=True)
        logging.info(f"Added {len(channels)} working Uganda channels")

    return len(channels)

async def clean_and_replace_channels(session, checker, all_channels, streams_dict, m3u_channels, logos_data):
    """Check all channels, replace non-working URLs if possible, and ensure only working channels are kept.
    
    This function guarantees:
    - All working channels (original or replaced) are retained.
    - All non-working channels without replacements are removed.
    - Directories are cleared before saving to prevent stale data.
    """
    logging.info("\n=== Step 5: Cleaning non-working channels and replacing URLs ===")
    
    all_channels = update_logos_for_null_channels(all_channels, logos_data)
    
    valid_channels = []
    replaced_channels = 0
    non_working_channels = 0
    country_files = {}
    category_files = {}

    async def find_replacement_url(channel, streams_dict, m3u_channels, session, checker):
        """Attempt to find a working replacement URL for a channel."""
        channel_id = channel.get("id")
        channel_name = channel.get("name", "").lower()

        if streams_dict and channel_id in streams_dict:
            new_url = streams_dict[channel_id].get("url")
            if new_url and not checker.has_unwanted_extension(new_url):
                for retry in range(2):
                    checker.timeout = ClientTimeout(total=min(INITIAL_TIMEOUT * (retry + 1), MAX_TIMEOUT))
                    _, is_working = await checker.check_single_url(session, new_url)
                    if is_working:
                        logging.info(f"Found working replacement URL from IPTV-org for {channel_name}: {new_url}")
                        return new_url
                    await asyncio.sleep(0.3 * (retry + 1))

        if m3u_channels:
            for m3u_channel in m3u_channels:
                m3u_name = m3u_channel.get("display_name", "").lower()
                if fuzz.ratio(channel_name, m3u_name) > 80:
                    new_url = m3u_channel.get("url")
                    if new_url and not checker.has_unwanted_extension(new_url):
                        for retry in range(2):
                            checker.timeout = ClientTimeout(total=min(INITIAL_TIMEOUT * (retry + 1), MAX_TIMEOUT))
                            _, is_working = await checker.check_single_url(session, new_url)
                            if is_working:
                                logging.info(f"Found working replacement URL from M3U for {channel_name}: {new_url}")
                                return new_url
                            await asyncio.sleep(0.3 * (retry + 1))

        logging.info(f"No working replacement URL found for {channel_name}")
        return None

    async def check_and_process_channel(channel):
        nonlocal valid_channels, non_working_channels, replaced_channels
        channel_url = channel.get("url")
        channel_name = channel.get("name", "Unknown")

        if not channel_url:
            logging.info(f"Skipping channel with no URL: {channel_name}")
            return

        # Skip URLs with unwanted extensions
        if checker.has_unwanted_extension(channel_url):
            logging.info(f"Skipping channel with unwanted extension: {channel_name} ({channel_url})")
            non_working_channels += 1
            return

        # Ensure logo is from logos_data
        channel_id = channel.get("id")
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

        logging.info(f"Channel not working: {channel_name} ({channel_url}). Attempting replacement.")
        new_url = await find_replacement_url(channel, streams_dict, m3u_channels, session, checker)
        if new_url:
            channel["url"] = new_url  # Update in place for simplicity
            valid_channels.append(channel)
            country = channel.get("country", "Unknown")
            if country and country != "Unknown":
                country_files.setdefault(country, []).append(channel)
            for cat in channel.get("categories", []):
                if cat:
                    category_files.setdefault(cat, []).append(channel)
            replaced_channels += 1
        else:
            logging.info(f"No replacement found for {channel_name}. Removing channel.")
            non_working_channels += 1

    total_channels = len(all_channels)
    batch_size = 400
    
    for batch_start in range(0, total_channels, batch_size):
        batch_end = min(batch_start + batch_size, total_channels)
        current_batch = all_channels[batch_start:batch_end]
        
        tasks = [check_and_process_channel(channel) for channel in current_batch]
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Cleaning channels {batch_start}-{batch_end}"):
            try:
                await future
            except Exception as e:
                logging.error(f"Error processing channel: {e}")

    # Save only valid channels, clearing directories first
    save_channels(valid_channels, country_files, category_files, append=False)

    logging.info(f"Replaced {replaced_channels} channels with new URLs")
    logging.info(f"Removed {non_working_channels} non-working channels (no replacement found)")
    logging.info(f"Total channels after cleaning: {len(valid_channels)}")

    return len(valid_channels), non_working_channels, replaced_channels

def sync_working_channels():
    """Sync all channels from country and category files to the main working_channels.json.
    
    This ensures consistency after cleaning, without reintroducing removed channels.
    """
    logging.info("Syncing all channels to working_channels.json...")
    
    all_channels = []
    
    if os.path.exists(COUNTRIES_DIR):
        for filename in os.listdir(COUNTRIES_DIR):
            if filename.endswith(".json"):
                country_file = os.path.join(COUNTRIES_DIR, filename)
                with open(country_file, "r", encoding="utf-8") as f:
                    channels = json.load(f)
                    all_channels.extend(remove_duplicates(channels))
    
    if os.path.exists(CATEGORIES_DIR):
        for filename in os.listdir(CATEGORIES_DIR):
            if filename.endswith(".json"):
                category_file = os.path.join(CATEGORIES_DIR, filename)
                with open(category_file, "r", encoding="utf-8") as f:
                    channels = json.load(f)
                    all_channels.extend(remove_duplicates(channels))
    
    all_channels = remove_duplicates(all_channels)
    
    with open(WORKING_CHANNELS_FILE, "w", encoding="utf-8") as f:
        json.dump(all_channels, f, indent=4, ensure_ascii=False)
    
    logging.info(f"Synced {len(all_channels)} channels to working_channels.json")

async def process_m3u_urls(session, logos_data):
    """Process M3U URLs and return count of working sports channels."""
    logging.info("\n=== Step 2: Processing M3U URLs ===")
    processor = M3UProcessor()
    all_channels = []
    
    for m3u_url in M3U_URLS:
        if not m3u_url:
            continue
            
        logging.info(f"Processing M3U URL: {m3u_url}")
        content = await processor.fetch_m3u_content(session, m3u_url)
        if content:
            channels = processor.parse_m3u(content)
            logging.info(f"Found {len(channels)} channels in {m3u_url}")
            
            sports_channels = [
                channel for channel in channels 
                if 'sport' in channel.get('group_title', '').lower() or 
                   'sport' in channel.get('display_name', '').lower() or
                   'sport' in channel.get('raw_name', '').lower() or 
                   'spor' in channel.get('group_title', '').lower() or
                   'spor' in channel.get('display_name', '').lower() or 
                   'spor' in channel.get('raw_name', '').lower() or
                   'dazn' in channel.get('group_title', '').lower() or
                   'dazn' in channel.get('display_name', '').lower() or 
                   'dazn' in channel.get('raw_name', '').lower() or
                   'sp' in channel.get('group_title', '').lower() or
                   'sp' in channel.get('display_name', '').lower() or 
                   'sp' in channel.get('raw_name', '').lower() or 
                   'spo' in channel.get('group_title', '').lower() or
                   'spo' in channel.get('display_name', '').lower() or 
                   'spo' in channel.get('raw_name', '').lower() or 
                   'xxx' in channel.get('group_title', '').lower() or
                   'xxx' in channel.get('display_name', '').lower() or 
                   'xxx' in channel.get('raw_name', '').lower() or 
                   '18+' in channel.get('group_title', '').lower() or
                   '18+' in channel.get('display_name', '').lower() or 
                   '18+' in channel.get('raw_name', '').lower()
            ]
            logging.info(f"Found {len(sports_channels)} sports channels in {m3u_url}")
            
            working_sports_channels = await processor.filter_working_channels(session, sports_channels)
            logging.info(f"Found {len(working_sports_channels)} working sports channels in {m3u_url}")
            
            formatted_channels = processor.format_channel_data(working_sports_channels, logos_data)
            all_channels.extend(formatted_channels)
    
    if all_channels:
        country_files = {}
        category_files = {}
        
        for channel in all_channels:
            country = channel.get("country", "Unknown")
            country_files.setdefault(country, []).append(channel)
            
            for category in channel.get("categories", ["sports"]):
                category_files.setdefault(category, []).append(channel)
        
        save_channels(all_channels, country_files, category_files, append=True)
        logging.info(f"Added {len(all_channels)} working sports channels from M3U URLs")
    
    return len(all_channels)

async def main():
    logging.info("Starting IPTV channel collection process...")
    
    checker = FastChecker()
    processor = M3UProcessor()
    
    async with aiohttp.ClientSession(connector=checker.connector) as session:
        # Fetch logos data first
        logos_data = await fetch_json(session, LOGOS_URL)
        logging.info(f"Loaded {len(logos_data)} logos from {LOGOS_URL}")
        
        logging.info("\n=== Step 1: Scraping Kenya TV channels ===")
        kenya_channels = scrape_kenya_tv_channels(logos_data)
        
        if kenya_channels:
            country_files = {}
            category_files = {}
            
            for channel in kenya_channels:
                country = channel.get("country", "KE")
                country_files.setdefault(country, []).append(channel)
                
                for category in channel.get("categories", ["general"]):
                    category_files.setdefault(category, []).append(channel)
            
            save_channels(kenya_channels, country_files, category_files, append=True)
            logging.info(f"Added {len(kenya_channels)} Kenya channels to working channels")

        logging.info("\n=== Step 1.5: Scraping Uganda channels ===")
        ug_channels_count = await fetch_and_process_uganda_channels(session, checker, logos_data)
        
        sports_channels_count = await process_m3u_urls(session, logos_data)
        
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

                if not os.path.exists(WORKING_CHANNELS_FILE):
                    with open(WORKING_CHANNELS_FILE, "w", encoding="utf-8") as f:
                        json.dump([], f)

                valid_channels_count = await validate_channels(
                    session, checker, all_existing_channels, iptv_channel_ids, logos_data
                )

                new_iptv_channels_count = await check_iptv_channels(
                    session, checker, channels_data, streams_dict, existing_urls, logos_data
                )

                all_channels = kenya_channels + all_existing_channels
                total_channels = valid_channels_count + new_iptv_channels_count + sports_channels_count + ug_channels_count
                logging.info(f"\nTotal working channels before cleaning: {total_channels}")
                logging.info(f"Working manual channels: {valid_channels_count}")
                logging.info(f"New working IPTV channels: {new_iptv_channels_count}")
                logging.info(f"New working sports channels: {sports_channels_count}")
                logging.info(f"New working Uganda channels: {ug_channels_count}")
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