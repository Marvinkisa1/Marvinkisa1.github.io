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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# URLs (loaded from environment variables, no defaults to ensure secrecy)
CHANNELS_URL = os.getenv("CHANNELS_URL", "")
STREAMS_URL = os.getenv("STREAMS_URL", "")
KENYA_BASE_URL = os.getenv("KENYA_BASE_URL", "")
M3U_URLS = [
    os.getenv("M3U_URL_1", ""),
    os.getenv("M3U_URL_2", "")
]

# File paths
WORKING_CHANNELS_FILE = "working_channels.json"
CATEGORIES_DIR = "categories"
COUNTRIES_DIR = "countries"

# Settings
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 100))  # Reduced for better reliability
INITIAL_TIMEOUT = 15  # Increased for thorough checking
MAX_TIMEOUT = 45  # Increased for thorough checking
RETRIES = 8  # Increased for thorough checking
BATCH_DELAY = 0.1
BATCH_SIZE = 500
USE_HEAD_METHOD = True
BYTE_RANGE_CHECK = True
KENYA_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

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

    async def check_single_url(self, session, url):
        """Thorough URL checker with multiple validation methods"""
        try:
            # First try HEAD request if enabled
            if USE_HEAD_METHOD:
                try:
                    async with session.head(url, timeout=self.timeout, allow_redirects=True) as response:
                        if response.status == 200:
                            content_type = response.headers.get('Content-Type', '').lower()
                            if any(x in content_type for x in ['video', 'mpegurl', 'application/octet-stream']):
                                return url, True
                            # Even if content-type doesn't match, it might still work
                            return url, True
                        elif response.status in [301, 302, 307, 308]:
                            # Follow redirects manually
                            redirect_url = response.headers.get('Location')
                            if redirect_url:
                                if not redirect_url.startswith('http'):
                                    # Handle relative redirects
                                    parsed = urlparse(url)
                                    redirect_url = f"{parsed.scheme}://{parsed.netloc}{redirect_url}"
                                return await self.check_single_url(session, redirect_url)
                        return url, False
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    pass  # Fall through to GET method
            
            # Try GET request with byte range
            headers = {'Range': 'bytes=0-1'} if BYTE_RANGE_CHECK else {}
            try:
                async with session.get(url, timeout=self.timeout, headers=headers, 
                                      allow_redirects=True) as response:
                    if response.status in (200, 206):
                        # For m3u8 playlists, try to parse them
                        if url.endswith('.m3u8') or 'm3u8' in response.headers.get('Content-Type', ''):
                            try:
                                content = await response.text()
                                playlist = m3u8.loads(content)
                                if playlist.segments or playlist.playlists:
                                    return url, True
                            except Exception:
                                # Even if parsing fails, the URL might still work
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
        self.timeout = aiohttp.ClientTimeout(total=15)  # Increased timeout
        self.semaphore = asyncio.Semaphore(50)
        self.unwanted_extensions = ['.mkv', '.mp4', '.avi', '.mov', '.flv', '.wmv']
        self.failed_urls = []

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
                if not any(line.lower().endswith(ext) for ext in self.unwanted_extensions):
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

    async def check_url(self, session, url, retries=RETRIES):
        """Thorough URL checking with multiple attempts and methods"""
        parsed_url = urlparse(url)
        domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
        headers = {
            'User-Agent': 'VLC/3.0.16 LibVLC/3.0.16',
            'Referer': domain,
        }
        
        for attempt in range(retries):
            try:
                async with self.semaphore:
                    # First try HEAD request
                    try:
                        async with session.head(url, headers=headers, timeout=self.timeout, 
                                              allow_redirects=True) as response:
                            if response.status in (200, 206, 301, 302, 307, 308):
                                return True
                    except (aiohttp.ClientError, asyncio.TimeoutError):
                        pass
                    
                    # Then try GET request with byte range
                    try:
                        async with session.get(url, headers={**headers, 'Range': 'bytes=0-1'}, 
                                             timeout=self.timeout, allow_redirects=True) as response:
                            if response.status in (200, 206):
                                return True
                    except (aiohttp.ClientError, asyncio.TimeoutError):
                        pass
                    
                    # For m3u8 URLs, try to parse the content
                    if url.endswith('.m3u8'):
                        try:
                            async with session.get(url, headers=headers, timeout=self.timeout, 
                                                  allow_redirects=True) as response:
                                if response.status == 200:
                                    content = await response.text()
                                    playlist = m3u8.loads(content)
                                    if playlist.segments or playlist.playlists:
                                        return True
                        except Exception:
                            pass
                
                # Wait before next attempt
                await asyncio.sleep(1 * (attempt + 1))
            except Exception as e:
                self.failed_urls.append((url, f"Attempt {attempt + 1}: {str(e)}"))
                await asyncio.sleep(1 * (attempt + 1))
        
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

    def format_channel_data(self, channels):
        """Format channel data into the desired JSON structure"""
        formatted_channels = []
        
        for channel in channels:
            base_id = re.sub(r'[^a-zA-Z0-9]', '', channel['tvg_name'] or channel['display_name'])
            if not base_id:
                base_id = re.sub(r'[^a-zA-Z0-9]', '', channel['raw_name'])
            
            country_code = channel['country_code']
            channel_id = f"{base_id.lower()}.{country_code.lower()}" if country_code else base_id.lower()
            
            formatted_channels.append({
                'name': channel['display_name'],
                'id': channel_id,
                'logo': channel['tvg_logo'],
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

def load_existing_data():
    existing_data = {
        "working_channels": [],
        "countries": {},
        "categories": {},
        "all_existing_channels": []
    }

    if os.path.exists(WORKING_CHANNELS_FILE):
        with open(WORKING_CHANNELS_FILE, "r", encoding="utf-8") as f:
            channels = json.load(f)
            for channel in channels:
                channel["country"] = channel.get("country", "Unknown")
                channel["categories"] = channel.get("categories", [])
            existing_data["working_channels"] = channels
            existing_data["all_existing_channels"].extend(channels)

    if os.path.exists(COUNTRIES_DIR):
        for filename in os.listdir(COUNTRIES_DIR):
            if filename.endswith(".json") and filename != ".json":  # Skip invalid files
                country = filename[:-5]
                with open(os.path.join(COUNTRIES_DIR, filename), "r", encoding="utf-8") as f:
                    channels = json.load(f)
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
                    for channel in channels:
                        channel["country"] = channel.get("country", "Unknown")
                        channel["categories"] = channel.get("categories", [])
                    existing_data["categories"][category] = channels
                    existing_data["all_existing_channels"].extend(channels)

    seen_urls = set()
    unique_channels = []
    for channel in existing_data["all_existing_channels"]:
        if channel.get("url") and channel["url"] not in seen_urls:
            seen_urls.add(channel["url"])
            unique_channels.append(channel)
    existing_data["all_existing_channels"] = unique_channels

    return existing_data

def save_channels(channels, working_channels_file, country_files, category_files):
    os.makedirs(COUNTRIES_DIR, exist_ok=True)
    os.makedirs(CATEGORIES_DIR, exist_ok=True)

    if os.path.exists(WORKING_CHANNELS_FILE):
        with open(WORKING_CHANNELS_FILE, "r", encoding="utf-8") as f:
            working_channels = json.load(f)
    else:
        working_channels = []
    working_channels.extend(channels)
    with open(WORKING_CHANNELS_FILE, "w", encoding="utf-8") as f:
        json.dump(working_channels, f, indent=4, ensure_ascii=False)

    for country, country_channels in country_files.items():
        safe_country = "".join(c for c in country if c.isalnum() or c in (' ', '_', '-')).rstrip()
        if not safe_country:
            continue
        country_file = os.path.join(COUNTRIES_DIR, f"{safe_country}.json")
        if os.path.exists(country_file):
            with open(country_file, "r", encoding="utf-8") as f:
                existing = json.load(f)
        else:
            existing = []
        existing.extend(country_channels)
        with open(country_file, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=4, ensure_ascii=False)

    for category, category_channels in category_files.items():
        safe_category = "".join(c for c in category if c.isalnum() or c in (' ', '_', '-')).rstrip()
        if not safe_category:
            continue
        category_file = os.path.join(CATEGORIES_DIR, f"{safe_category}.json")
        if os.path.exists(category_file):
            with open(category_file, "r", encoding="utf-8") as f:
                existing = json.load(f)
        else:
            existing = []
        existing.extend(category_channels)
        with open(category_file, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=4, ensure_ascii=False)

async def validate_channels(session, checker, all_existing_channels, iptv_channel_ids):
    valid_channels_count = 0
    batch_channels = []
    country_files = {}
    category_files = {}

    async def validate_channel(channel):
        async with checker.semaphore:
            channel_url = channel.get("url")
            if not channel_url or channel.get("id") in iptv_channel_ids:
                return None

            for retry in range(RETRIES):
                checker.timeout = ClientTimeout(total=min(INITIAL_TIMEOUT * (retry + 1), MAX_TIMEOUT))
                _, is_working = await checker.check_single_url(session, channel_url)
                if is_working:
                    channel_copy = channel.copy()
                    channel_copy["country"] = channel.get("country", "Unknown")
                    channel_copy["categories"] = channel.get("categories", [])
                    batch_channels.append(channel_copy)
                    country = channel_copy["country"]
                    country_files.setdefault(country, []).append(channel_copy)
                    for cat in channel_copy["categories"]:
                        category_files.setdefault(cat, []).append(channel_copy)
                    return channel_copy
                await asyncio.sleep(0.1 * (retry + 1))  # Increasing delay
            return None

    tasks = [validate_channel(channel) for channel in all_existing_channels]
    for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Validating channels"):
        try:
            result = await future
            if result:
                valid_channels_count += 1
            if len(batch_channels) >= BATCH_SIZE:
                save_channels(batch_channels, WORKING_CHANNELS_FILE, country_files, category_files)
                batch_channels.clear()
                country_files.clear()
                category_files.clear()
                await asyncio.sleep(BATCH_DELAY)
        except Exception as e:
            logging.error(f"Error processing channel: {e}")

    if batch_channels:
        save_channels(batch_channels, WORKING_CHANNELS_FILE, country_files, category_files)

    return valid_channels_count

async def check_iptv_channels(session, checker, channels_data, streams_dict, existing_urls):
    new_iptv_channels_count = 0
    batch_channels = []
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

            for retry in range(RETRIES):
                checker.timeout = ClientTimeout(total=min(INITIAL_TIMEOUT * (retry + 1), MAX_TIMEOUT))
                _, is_working = await checker.check_single_url(session, url)
                if is_working:
                    channel_data = {
                        "name": channel.get("name", "Unknown"),
                        "id": channel.get("id"),
                        "logo": channel.get("logo"),
                        "url": url,
                        "categories": channel.get("categories", []),
                        "country": channel.get("country", "Unknown"),
                    }
                    batch_channels.append(channel_data)
                    country_files.setdefault(channel_data["country"], []).append(channel_data)
                    for cat in channel_data["categories"]:
                        category_files.setdefault(cat, []).append(channel_data)
                    return channel_data
                await asyncio.sleep(0.1 * (retry + 1))  # Increasing delay
            return None

    tasks = [process_channel(channel) for channel in channels_to_check]
    for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Testing new IPTV channels"):
        try:
            result = await future
            if result:
                new_iptv_channels_count += 1
            if len(batch_channels) >= BATCH_SIZE:
                save_channels(batch_channels, WORKING_CHANNELS_FILE, country_files, category_files)
                batch_channels.clear()
                country_files.clear()
                category_files.clear()
                await asyncio.sleep(BATCH_DELAY)
        except Exception as e:
            logging.error(f"Error processing channel: {e}")

    if batch_channels:
        save_channels(batch_channels, WORKING_CHANNELS_FILE, country_files, category_files)

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

async def check_single_m3u8_url(session, url, timeout=30):
    """Check if a single m3u8 URL is valid"""
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status == 200:
                content = await response.text()
                playlist = m3u8.loads(content)
                if playlist.segments or playlist.playlists:
                    logging.info(f"Valid m3u8 found: {url}")
                    return url, True
                else:
                    # Even if parsing fails, the URL might still work
                    logging.info(f"m3u8 parsing failed but keeping URL: {url}")
                    return url, True
    except Exception as e:
        logging.error(f"Failed to check {url} (timeout={timeout}s): {e}")
    return url, False

async def check_m3u8_urls(urls):
    """Check multiple m3u8 URLs and return the first working one"""
    async with aiohttp.ClientSession() as session:
        tasks = [check_single_m3u8_url(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        for url, is_valid in results:
            if is_valid:
                return url
        return None

def scrape_kenya_tv_channels():
    """Scrape Kenya TV channels and return valid ones"""
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
                        
                        channel_data = {
                            "name": channel_name,
                            "id": f"{channel_name}.ke",
                            "logo": img_tag.get('src', ''),
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
        
        return filtered_results
        
    except Exception as e:
        logging.error(f"Error occurred in Kenya TV scrape: {str(e)}")
        return []

async def clean_and_replace_channels(session, checker, all_channels, streams_dict, m3u_channels):
    """Check all channels, replace non-working URLs if possible, keep all channels"""
    logging.info("\n=== Step 5: Cleaning non-working channels and replacing URLs ===")
    
    valid_channels = []
    replaced_channels = 0
    non_working_channels = 0
    country_files = {}
    category_files = {}

    async def find_replacement_url(channel, streams_dict, m3u_channels, session, checker):
        """Attempt to find a working replacement URL for a channel"""
        channel_id = channel.get("id")
        channel_name = channel.get("name", "").lower()

        # Try IPTV-org streams first
        if streams_dict and channel_id in streams_dict:
            new_url = streams_dict[channel_id].get("url")
            if new_url:
                for retry in range(RETRIES):
                    checker.timeout = ClientTimeout(total=min(INITIAL_TIMEOUT * (retry + 1), MAX_TIMEOUT))
                    _, is_working = await checker.check_single_url(session, new_url)
                    if is_working:
                        logging.info(f"Found working replacement URL from IPTV-org for {channel_name}: {new_url}")
                        return new_url
                    await asyncio.sleep(0.5 * (retry + 1))  # Longer delay

        # Try M3U channels with fuzzy matching
        if m3u_channels:
            for m3u_channel in m3u_channels:
                m3u_name = m3u_channel.get("display_name", "").lower()
                if fuzz.ratio(channel_name, m3u_name) > 80:  # Similarity threshold
                    new_url = m3u_channel.get("url")
                    for retry in range(RETRIES):
                        checker.timeout = ClientTimeout(total=min(INITIAL_TIMEOUT * (retry + 1), MAX_TIMEOUT))
                        _, is_working = await checker.check_single_url(session, new_url)
                        if is_working:
                            logging.info(f"Found working replacement URL from M3U for {channel_name}: {new_url}")
                            return new_url
                        await asyncio.sleep(0.5 * (retry + 1))  # Longer delay

        logging.info(f"No working replacement URL found for {channel_name}")
        return None

    async def check_and_process_channel(channel):
        nonlocal valid_channels, non_working_channels, replaced_channels
        channel_url = channel.get("url")
        channel_name = channel.get("name", "Unknown")

        if not channel_url:
            logging.info(f"Keeping channel with no URL (no check possible): {channel_name}")
            valid_channels.append(channel)
            country = channel.get("country", "Unknown")
            if country and country != "Unknown":
                country_files.setdefault(country, []).append(channel)
            for cat in channel.get("categories", []):
                if cat:
                    category_files.setdefault(cat, []).append(channel)
            return

        # Check if current URL is working
        is_working = False
        for retry in range(RETRIES):
            checker.timeout = ClientTimeout(total=min(INITIAL_TIMEOUT * (retry + 1), MAX_TIMEOUT))
            _, is_working = await checker.check_single_url(session, channel_url)
            if is_working:
                break
            await asyncio.sleep(0.5 * (retry + 1))  # Longer delay between retries

        if is_working:
            valid_channels.append(channel)
            country = channel.get("country", "Unknown")
            if country and country != "Unknown":
                country_files.setdefault(country, []).append(channel)
            for cat in channel.get("categories", []):
                if cat:
                    category_files.setdefault(cat, []).append(channel)
            return

        # Current URL is not working, try to find a replacement
        logging.info(f"Channel not working: {channel_name} ({channel_url}). Attempting replacement.")
        new_url = await find_replacement_url(channel, streams_dict, m3u_channels, session, checker)
        if new_url:
            channel_copy = channel.copy()
            channel_copy["url"] = new_url
            valid_channels.append(channel_copy)
            country = channel_copy.get("country", "Unknown")
            if country and country != "Unknown":
                country_files.setdefault(country, []).append(channel_copy)
            for cat in channel_copy.get("categories", []):
                if cat:
                    category_files.setdefault(cat, []).append(channel_copy)
            replaced_channels += 1
        else:
            # Keep the channel even if no replacement found
            logging.info(f"No replacement found for {channel_name}. Keeping original channel.")
            valid_channels.append(channel)
            country = channel.get("country", "Unknown")
            if country and country != "Unknown":
                country_files.setdefault(country, []).append(channel)
            for cat in channel.get("categories", []):
                if cat:
                    category_files.setdefault(cat, []).append(channel)
            non_working_channels += 1

    tasks = [check_and_process_channel(channel) for channel in all_channels]
    for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Cleaning and replacing channels"):
        try:
            await future
        except Exception as e:
            logging.error(f"Error processing channel: {e}")

    # Save updated channels
    save_channels(valid_channels, WORKING_CHANNELS_FILE, country_files, category_files)

    logging.info(f"Replaced {replaced_channels} channels with new URLs")
    logging.info(f"Kept {non_working_channels} non-working channels (no replacement found)")
    logging.info(f"Total channels after cleaning: {len(valid_channels)}")

    return len(valid_channels), non_working_channels, replaced_channels

def sync_working_channels():
    """Sync all channels from country and category files to the main working_channels.json"""
    logging.info("Syncing all channels to working_channels.json...")
    
    all_channels = []
    
    # Load channels from country files
    if os.path.exists(COUNTRIES_DIR):
        for filename in os.listdir(COUNTRIES_DIR):
            if filename.endswith(".json"):
                country_file = os.path.join(COUNTRIES_DIR, filename)
                with open(country_file, "r", encoding="utf-8") as f:
                    channels = json.load(f)
                    all_channels.extend(channels)
    
    # Load channels from category files
    if os.path.exists(CATEGORIES_DIR):
        for filename in os.listdir(CATEGORIES_DIR):
            if filename.endswith(".json"):
                category_file = os.path.join(CATEGORIES_DIR, filename)
                with open(category_file, "r", encoding="utf-8") as f:
                    channels = json.load(f)
                    all_channels.extend(channels)
    
    # Remove duplicates by URL
    seen_urls = set()
    unique_channels = []
    for channel in all_channels:
        if channel.get("url") and channel["url"] not in seen_urls:
            seen_urls.add(channel["url"])
            unique_channels.append(channel)
    
    # Save to working_channels.json
    with open(WORKING_CHANNELS_FILE, "w", encoding="utf-8") as f:
        json.dump(unique_channels, f, indent=4, ensure_ascii=False)
    
    logging.info(f"Synced {len(unique_channels)} channels to working_channels.json")

async def process_m3u_urls(session):
    """Process M3U URLs and return count of working sports channels"""
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
            
            # Filter for sports channels
            sports_channels = [
                channel for channel in channels 
                if 'sport' in channel.get('group_title', '').lower() or 
                   'sport' in channel.get('display_name', '').lower() or
                   'sport' in channel.get('raw_name', '').lower()
            ]
            logging.info(f"Found {len(sports_channels)} sports channels in {m3u_url}")
            
            # Check which sports channels are working
            working_sports_channels = await processor.filter_working_channels(session, sports_channels)
            logging.info(f"Found {len(working_sports_channels)} working sports channels in {m3u_url}")
            
            # Format and save the working sports channels
            formatted_channels = processor.format_channel_data(working_sports_channels)
            all_channels.extend(formatted_channels)
    
    if all_channels:
        country_files = {}
        category_files = {}
        
        for channel in all_channels:
            country = channel.get("country", "Unknown")
            country_files.setdefault(country, []).append(channel)
            
            for category in channel.get("categories", ["sports"]):
                category_files.setdefault(category, []).append(channel)
        
        save_channels(all_channels, WORKING_CHANNELS_FILE, country_files, category_files)
        logging.info(f"Added {len(all_channels)} working sports channels from M3U URLs")
    
    return len(all_channels)

async def main():
    logging.info("Starting IPTV channel collection process...")
    
    checker = FastChecker()
    
    async with aiohttp.ClientSession(connector=checker.connector) as session:
        logging.info("\n=== Step 1: Scraping Kenya TV channels ===")
        kenya_channels = scrape_kenya_tv_channels()
        
        if kenya_channels:
            country_files = {}
            category_files = {}
            
            for channel in kenya_channels:
                country = channel.get("country", "KE")
                country_files.setdefault(country, []).append(channel)
                
                for category in channel.get("categories", ["general"]):
                    category_files.setdefault(category, []).append(channel)
            
            save_channels(kenya_channels, WORKING_CHANNELS_FILE, country_files, category_files)
            logging.info(f"Added {len(kenya_channels)} Kenya channels to working channels")
        
        sports_channels_count = await process_m3u_urls(session)
        
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
                    session, checker, all_existing_channels, iptv_channel_ids
                )

                new_iptv_channels_count = await check_iptv_channels(
                    session, checker, channels_data, streams_dict, existing_urls
                )

                total_channels = valid_channels_count + new_iptv_channels_count + sports_channels_count
                logging.info(f"\nTotal working channels: {total_channels}")
                logging.info(f"Working manual channels: {valid_channels_count}")
                logging.info(f"New working IPTV channels: {new_iptv_channels_count}")
                logging.info(f"New working sports channels: {sports_channels_count}")
        except Exception as e:
            logging.error(f"Error in IPTV-org processing: {e}")
            streams_dict = {}
            channels_data = []

    # Step 4: Sync all channels
    logging.info("\n=== Step 4: Syncing channels ===")
    sync_working_channels()

    # Step 5: Clean non-working channels and replace URLs
    logging.info("\n=== Step 5: Cleaning non-working channels and replacing URLs ===")
    existing_data = load_existing_data()
    all_existing_channels = existing_data["all_existing_channels"]
    
    # Load M3U channels for potential replacements
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
        session, checker, all_existing_channels, streams_dict, m3u_channels
    )

    # Step 6: Sync updated channels
    logging.info("\n=== Step 6: Syncing updated channels ===")
    sync_working_channels()

    logging.info("\n=== Process completed ===")
    logging.info(f"Final count: {valid_channels_count} channels")
    logging.info(f"Non-working channels kept: {non_working_count}")
    logging.info(f"Channels replaced: {replaced_count}")
    sys.exit(0)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Script failed: {e}")
        sys.exit(1)
