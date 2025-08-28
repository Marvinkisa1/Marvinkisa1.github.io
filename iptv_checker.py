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
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 200))
INITIAL_TIMEOUT = 5
MAX_TIMEOUT = 15
RETRIES = 2
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
        """Optimized URL checker with minimal data usage"""
        try:
            if USE_HEAD_METHOD:
                try:
                    async with session.head(url, timeout=self.timeout) as response:
                        if response.status == 200:
                            content_type = response.headers.get('Content-Type', '').lower()
                            if 'video/mp2t' in content_type or 'application/vnd.apple.mpegurl' in content_type:
                                return url, True
                            return url, True
                        return url, False
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    return url, False
            
            headers = {'Range': 'bytes=0-1'} if BYTE_RANGE_CHECK else {}
            try:
                async with session.get(url, timeout=self.timeout, headers=headers) as response:
                    if response.status in (200, 206):
                        if BYTE_RANGE_CHECK:
                            return url, True
                        try:
                            content = await response.text()
                            playlist = m3u8.loads(content)
                            if playlist.segments or playlist.playlists:
                                return url, True
                        except Exception:
                            return url, False
                return url, False
            except (aiohttp.ClientError, asyncio.TimeoutError):
                return url, False
        except Exception:
            return url, False

class M3UProcessor:
    def __init__(self):
        self.connector = aiohttp.TCPConnector(limit=50, force_close=True)
        self.timeout = aiohttp.ClientTimeout(total=5)
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

    async def check_url(self, session, url, retries=2):
        """Check if URL is a working stream, mimicking VLC behavior"""
        parsed_url = urlparse(url)
        domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
        headers = {
            'User-Agent': 'VLC/3.0.16 LibVLC/3.0.16',
            'Referer': domain,
        }
        
        for attempt in range(retries + 1):
            try:
                async with self.semaphore:
                    async with session.get(url, headers={**headers, 'Range': 'bytes=0-1'}, timeout=self.timeout, allow_redirects=True) as response:
                        if response.status in (200, 206):
                            return True
                        self.failed_urls.append((url, f"GET: Status {response.status}"))
                        return False
            except aiohttp.ClientError as e:
                self.failed_urls.append((url, f"Attempt {attempt + 1}: ClientError: {str(e)}"))
            except asyncio.TimeoutError:
                self.failed_urls.append((url, f"Attempt {attempt + 1}: TimeoutError"))
            if attempt < retries:
                await asyncio.sleep(0.5)
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
            if filename.endswith(".json"):
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
                await asyncio.sleep(0.05)
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
                        'url': url,
                        "categories": channel.get("categories", []),
                        "country": channel.get("country", "Unknown"),
                    }
                    batch_channels.append(channel_data)
                    country_files.setdefault(channel_data["country"], []).append(channel_data)
                    for cat in channel_data["categories"]:
                        category_files.setdefault(cat, []).append(channel_data)
                    return channel_data
                await asyncio.sleep(0.05)
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

def sync_working_channels():
    """Sync working channels with category and country files"""
    def load_json_file(file_path):
        """Load JSON file and return its content, or empty list if file doesn't exist."""
        if os.path.exists(file_path):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logging.error(f"Error: Invalid JSON in {file_path}. Returning empty list.")
                return []
        return []

    def save_json_file(file_path, data):
        """Save data to JSON file, creating directories if needed."""
        directory = os.path.dirname(file_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

    def get_all_channels_from_dir(directory):
        """Load all channels from JSON files in the specified directory."""
        channels = []
        if os.path.exists(directory):
            for filename in os.listdir(directory):
                if filename.endswith(".json"):
                    file_path = os.path.join(directory, filename)
                    channels.extend(load_json_file(file_path))
        return channels

    def ensure_channel_fields(channel):
        """Ensure channel has required fields."""
        channel["country"] = channel.get("country", "Unknown")
        channel["categories"] = channel.get("categories", [])
        return channel

    working_channels = load_json_file(WORKING_CHANNELS_FILE)
    if not working_channels:
        logging.info(f"No channels found in {WORKING_CHANNELS_FILE}. Exiting.")
        return

    category_channels = {}
    country_channels = {}

    for filename in os.listdir(CATEGORIES_DIR) if os.path.exists(CATEGORIES_DIR) else []:
        if filename.endswith(".json"):
            category = filename[:-5]
            category_channels[category] = load_json_file(os.path.join(CATEGORIES_DIR, filename))

    for filename in os.listdir(COUNTRIES_DIR) if os.path.exists(COUNTRIES_DIR) else []:
        if filename.endswith(".json"):
            country = filename[:-5]
            country_channels[country] = load_json_file(os.path.join(COUNTRIES_DIR, filename))

    added_to_categories = 0
    added_to_countries = 0

    for channel in working_channels:
        channel = ensure_channel_fields(channel)
        url = channel.get("url")
        if not url:
            logging.info(f"Skipping channel with missing URL: {channel.get('name', 'Unknown')}")
            continue

        categories = channel["categories"]
        for category in categories:
            if category:
                category_file = os.path.join(CATEGORIES_DIR, f"{category}.json")
                if category not in category_channels:
                    category_channels[category] = load_json_file(category_file)
                
                category_urls = {ch.get("url") for ch in category_channels[category] if ch.get("url")}
                if url not in category_urls:
                    category_channels[category].append(channel.copy())
                    added_to_categories += 1

        country = channel["country"]
        if country:
            country_file = os.path.join(COUNTRIES_DIR, f"{country}.json")
            if country not in country_channels:
                country_channels[country] = load_json_file(country_file)
            
            country_urls = {ch.get("url") for ch in country_channels[country] if ch.get("url")}
            if url not in country_urls:
                country_channels[country].append(channel.copy())
                added_to_countries += 1

    for category, channels in category_channels.items():
        if channels:
            save_json_file(os.path.join(CATEGORIES_DIR, f"{category}.json"), channels)

    for country, channels in country_channels.items():
        if channels:
            save_json_file(os.path.join(COUNTRIES_DIR, f"{country}.json"), channels)

    working_channels = sorted(
        working_channels,
        key=lambda x: x.get("country", "Unknown").lower()
    )
    save_json_file(WORKING_CHANNELS_FILE, working_channels)

    logging.info(f"Added {added_to_categories} channels to category files.")
    logging.info(f"Added {added_to_countries} channels to country files.")
    logging.info(f"Updated {WORKING_CHANNELS_FILE} with {len(working_channels)} channels, sorted by country.")

async def process_m3u_urls(session):
    """Process M3U URLs to extract and check sports channels"""
    logging.info("\n=== Step 4: Processing M3U URLs for sports channels ===")
    
    processor = M3UProcessor()
    all_sports_channels = []
    total_sports_channels = 0
    
    for m3u_url in M3U_URLS:
        if not m3u_url:
            logging.error("M3U URL is not set. Skipping.")
            continue
        
        logging.info(f"\nProcessing M3U URL: {m3u_url}")
        
        m3u_content = await processor.fetch_m3u_content(session, m3u_url)
        
        if not m3u_content:
            logging.error(f"Failed to fetch M3U content from {m3u_url}")
            continue
        
        channels = processor.parse_m3u(m3u_content)
        
        total_streams = len(channels)
        logging.info(f"Found {total_streams} streams from {m3u_url}")
        
        sports_channels = [
            channel for channel in channels 
            if any(keyword in channel['display_name'].lower() for keyword in [
                'sport', 'sports', 'football', 'soccer', 'tennis', 'basketball', 
                'golf', 'cricket', 'hockey', 'baseball', 'rugby', 'f1', 
                'motorsport', 'racing', 'boxing', 'ufc', 'wwe'
            ])
        ]
        
        logging.info(f"Found {len(sports_channels)} sports channels from {m3u_url}")
        
        existing_data = load_existing_data()
        existing_urls = {ch.get("url") for ch in existing_data["all_existing_channels"] if ch.get("url")}
        
        unique_sports_channels = [ch for ch in sports_channels if ch['url'] not in existing_urls]
        logging.info(f"Found {len(unique_sports_channels)} new sports channels not in existing data")
        
        if not unique_sports_channels:
            logging.info("No new sports channels to process from this URL")
            continue
        
        logging.info(f"Checking which sports streams are working from {m3u_url}...")
        working_sports_channels = await processor.filter_working_channels(session, unique_sports_channels)
        
        formatted_data = processor.format_channel_data(working_sports_channels)
        all_sports_channels.extend(formatted_data)
        total_sports_channels += len(working_sports_channels)
        
        logging.info(f"Found {len(working_sports_channels)} working sports channels from {m3u_url}")
    
    if all_sports_channels:
        country_files = {}
        category_files = {}
        
        for channel in all_sports_channels:
            country = channel.get("country", "Unknown")
            country_files.setdefault(country, []).append(channel)
            
            for category in channel.get("categories", ["sports"]):
                category_files.setdefault(category, []).append(channel)
        
        save_channels(all_sports_channels, WORKING_CHANNELS_FILE, country_files, category_files)
        
        logging.info(f"\nFound {total_sports_channels} working sports channels from all M3U URLs")
        
        if processor.failed_urls:
            for url, reason in processor.failed_urls[:10]:
                logging.info(f"Failed URL: {url} - {reason}")
        
        return total_sports_channels
    
    return 0

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
    
    logging.info("\n=== Step 4: Syncing channels ===")
    sync_working_channels()
    
    logging.info("\n=== Process completed ===")
    sys.exit(0)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Script failed: {e}")
        sys.exit(1)