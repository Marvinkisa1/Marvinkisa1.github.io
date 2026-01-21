import aiohttp
import asyncio
import json
import os
import m3u8
from tqdm import tqdm
from aiohttp import ClientTimeout, TCPConnector
from urllib.parse import urlparse, urljoin, quote
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
import html  # For decoding entities like & to &
from datetime import date, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# URLs (loaded from environment variables, no defaults to ensure secrecy)
CHANNELS_URL = os.getenv("CHANNELS_URL", "https://iptv-org.github.io/api/channels.json")
STREAMS_URL = os.getenv("STREAMS_URL", "https://iptv-org.github.io/api/streams.json")
LOGOS_URL = os.getenv("LOGOS_URL", "https://iptv-org.github.io/api/logos.json")
KENYA_BASE_URL = os.getenv("KENYA_BASE_URL", "")
# Properly encoded Uganda API URL
UGANDA_API_URL = "https://apps.moochatplus.net/bash/api/api.php?get_posts&page=1&count=100&api_key=cda11bx8aITlKsXCpNB7yVLnOdEGqg342ZFrQzJRetkSoUMi9w"
M3U_URLS = [
    os.getenv("M3U_URL_1", ""),
    os.getenv("M3U_URL_2", "")
]

# Additional M3U for news and XXX to get more channels
ADDITIONAL_M3U = [
    "https://iptv-org.github.io/iptv/categories/news.m3u",
    "https://iptv-org.github.io/iptv/categories/xxx.m3u",
    "https://raw.githubusercontent.com/ipstreet312/freeiptv/refs/heads/master/all.m3u",
    "https://raw.githubusercontent.com/abusaeeidx/IPTV-Scraper-Zilla/refs/heads/main/combined-playlist.m3u"
]

# File paths
WORKING_CHANNELS_BASE = "working_channels"
CATEGORIES_DIR = "categories"
COUNTRIES_DIR = "countries"

# Settings - Optimized for speed but still thorough
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 50))  # Reduced for better reliability
INITIAL_TIMEOUT = 10  # Reduced initial timeout
MAX_TIMEOUT = 15  # Reduced maximum timeout
RETRIES = 2  # Reduced retries for efficiency
BATCH_DELAY = 0.05  # Reduced delay
BATCH_SIZE = 100  # Reduced batch size for better control
USE_HEAD_METHOD = False  # Disabled HEAD method as it often causes issues with streams
BYTE_RANGE_CHECK = False  # Disabled for broader compatibility
KENYA_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0'
}
MAX_CHANNELS_PER_FILE = 4000

# Unwanted extensions for filtering
UNWANTED_EXTENSIONS = ['.mkv', '.mp4', '.avi', '.mov', '.flv', '.wmv']

# Scraper headers
SCRAPER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0'
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


def scrape_daily_m3u_urls(max_working=5):
    """Scrape daily working M3U URLs from world-iptv.club."""
    logging.info("Starting daily M3U URL scraper...")

    # Get current date in DD-MM-YYYY format
    current_date = date.today().strftime("%d-%m-%Y")
    # Fetch the category page
    url = 'https://world-iptv.club/category/iptv/'
    try:
        response = requests.get(url, headers=SCRAPER_HEADERS, timeout=30)
        if response.status_code != 200:
            logging.error(f"Failed to fetch the page: {response.status_code}")
            return []
    except Exception as e:
        logging.error(f"Error fetching category page: {e}")
        return []
    content = response.text
    # Regex to find all href attributes (general, to catch more)
    pattern = r'<a\s+[^>]*href="([^"]*)"'
    matches = re.findall(pattern, content, re.IGNORECASE)
    # Convert to full URLs, filter for those containing 'm3u', and remove duplicates
    urls = []
    seen = set()
    for match in matches:
        if 'm3u' in match.lower():
            if match.startswith('/'):
                full_url = 'https://world-iptv.club' + match
            elif match.startswith('http'):
                full_url = match
            else:
                continue  # Skip invalid

            if full_url not in seen:
                seen.add(full_url)
                urls.append(full_url)
    # Filter for URLs containing the current date like -DD-MM-YYYY/
    current_urls = [u for u in urls if f'-{current_date}/' in u]
    # If none for current, try previous day
    prev_date = None
    if not current_urls:
        prev_date = (date.today() - timedelta(days=1)).strftime("%d-%m-%Y")
        current_urls = [u for u in urls if f'-{prev_date}/' in u]
    # Get up to the first 5
    top_5_urls = current_urls[:5]
    if not top_5_urls:
        fallback_date = (date.today() - timedelta(days=2)).strftime("%d-%m-%Y")
        logging.warning(f"No URLs found for recent dates: {current_date}, {prev_date or 'N/A'}, or {fallback_date}")
        return []
    date_used = current_date if f'-{current_date}/' in top_5_urls[0] else prev_date
    logging.info(f"Using date: {date_used}")
    logging.info("Scraped playlist pages:")
    for i, link in enumerate(top_5_urls, 1):
        logging.info(f"{i}. {link}")
    # Now, visit each and extract .m3u / get.php links
    working_m3u = []
    for page_url in top_5_urls:
        logging.info(f"\nFetching {page_url}...")
        try:
            resp = requests.get(page_url, headers=SCRAPER_HEADERS, timeout=30)
            if resp.status_code != 200:
                logging.warning(f"Failed to fetch page: {resp.status_code}")
                continue
        except Exception as e:
            logging.error(f"Error fetching page: {e}")
            continue

        page_content = resp.text

        # Broader pattern for M3U: .m3u or get.php?...type=m3u/m3u_plus/m3u8
        m3u_pattern = r'(?:\.m3u|get\.php\?.*?type=(?:m3u|m3u_plus|m3u8))'

        # Extract from hrefs
        href_pattern = r'<a\s+[^>]*href="([^"]*)"'
        all_hrefs = re.findall(href_pattern, page_content, re.IGNORECASE)
        href_m3u = [html.unescape(h) for h in all_hrefs if re.search(m3u_pattern, h, re.IGNORECASE)]

        # Fallback/Enhance: Extract from raw text
        text_pattern = r'https?://[^\s<>"]+'
        all_urls_in_text = re.findall(text_pattern, page_content)
        text_m3u = [html.unescape(u) for u in all_urls_in_text if re.search(m3u_pattern, u, re.IGNORECASE)]

        # Union, make full, dedupe
        m3u_matches = list(set(href_m3u + text_m3u))
        m3u_matches = [urljoin(page_url, m) if not m.startswith('http') else m for m in m3u_matches]
        m3u_matches = list(dict.fromkeys(m3u_matches))  # Preserve order, remove dups

        logging.info(f"Found {len(m3u_matches)} potential M3U/stream links on this page.")

        for idx, m3u_match in enumerate(m3u_matches):
            full_m3u = m3u_match  # Already full and decoded

            logging.info(f" Testing {idx + 1}: {full_m3u}")

            # Test if working: GET and check status and non-empty content
            try:
                m3u_resp = requests.get(full_m3u, headers=SCRAPER_HEADERS, timeout=30, stream=True)
                status = m3u_resp.status_code
                content_len = len(m3u_resp.content)
                content_preview = m3u_resp.text[:100] + '...' if len(m3u_resp.text) > 100 else m3u_resp.text

                logging.info(f" Status: {status}, Length: {content_len}")
                if content_len <= 100:  # Only preview if small/relevant
                    logging.info(f" Preview: {content_preview}")

                # Enhanced check: M3U signature + optional content-type
                is_m3u = status == 200 and content_len > 50 and '#EXT' in m3u_resp.text
                if is_m3u or 'm3u' in m3u_resp.headers.get('content-type', '').lower():
                    working_m3u.append(full_m3u)
                    logging.info(f" -> WORKING!")
                    if len(working_m3u) >= max_working:
                        break
                else:
                    logging.info(f" -> Not working (status, size, or format issue)")
            except Exception as e:
                logging.error(f" -> Error: {e}")

        if len(working_m3u) >= max_working:
            break
    if working_m3u:
        logging.info(f"Scraped {len(working_m3u)} working M3U URLs:")
        for i, m3u in enumerate(working_m3u, 1):
            logging.info(f"{i}. {m3u}")
    else:
        logging.warning("No working M3U URLs found after testing all.")
    return working_m3u


class FastChecker:
    def __init__(self):
        self.connector = TCPConnector(
            limit=MAX_CONCURRENT,
            force_close=True,
            enable_cleanup_closed=True,
            ttl_dns_cache=300,
            ssl=False  # Try without SSL verification
        )
        self.timeout = ClientTimeout(total=INITIAL_TIMEOUT)
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    def has_unwanted_extension(self, url):
        """Check if URL has unwanted video file extension"""
        if not url:
            return False
        return any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS)

    async def check_single_url(self, session, url):
        """Improved URL checker that's more accurate for streaming URLs"""
        # Skip URLs with unwanted extensions
        if self.has_unwanted_extension(url):
            return url, False

        try:
            # For streaming URLs, we need to be more lenient
            # Many streaming servers don't follow standard HTTP patterns
            
            # Check if it's an m3u8 URL
            is_m3u8 = url.endswith('.m3u8') or '.m3u8?' in url
            
            # Try GET request with streaming-friendly settings
            async with session.get(url, timeout=self.timeout, allow_redirects=True,
                                   headers={
                                       'Accept': '*/*',
                                       'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                                       'Connection': 'keep-alive',
                                       'Accept-Encoding': 'gzip, deflate',
                                       'Range': 'bytes=0-1024'  # Request only first 1KB
                                   }, ssl=False) as response:
                
                # For m3u8 files, we need different validation
                if is_m3u8:
                    if response.status in [200, 206]:
                        try:
                            content = await response.text()
                            # Check for m3u8 markers
                            if '#EXTM3U' in content or '#EXTINF' in content:
                                logging.debug(f"Valid m3u8 found: {url}")
                                return url, True
                            # Some m3u8 might be binary or encrypted but still valid
                            elif len(content) > 0:
                                # Could be a valid m3u8 with different encoding
                                logging.debug(f"Non-standard m3u8 but status OK: {url}")
                                return url, True
                        except:
                            # Binary content, might still be valid
                            logging.debug(f"Binary m3u8 content: {url}")
                            return url, True
                    elif response.status in [301, 302, 307, 308]:
                        # Redirects for m3u8 are common
                        location = response.headers.get('Location', '')
                        if location:
                            logging.debug(f"M3U8 redirect to: {location}")
                            return url, True
                
                # For non-m3u8 URLs
                if response.status == 200:
                    # For streaming URLs, even empty responses might be valid
                    content_type = response.headers.get('Content-Type', '').lower()
                    
                    # Check if it's a video/audio stream
                    if 'video/' in content_type or 'audio/' in content_type:
                        return url, True
                    
                    # Check if it's an application stream
                    if 'application/' in content_type:
                        if 'octet-stream' in content_type or 'mpegurl' in content_type:
                            return url, True
                    
                    # Read a small amount to check content
                    try:
                        content = await response.content.read(512)
                        if len(content) > 0:
                            # Try to decode as text to check for HTML
                            try:
                                content_str = content.decode('utf-8', errors='ignore').lower()
                                # Check for HTML error pages
                                html_indicators = ['<!doctype', '<html', '<head', '<body', '<title>']
                                if any(indicator in content_str for indicator in html_indicators):
                                    # Check if it's a small HTML (likely error page)
                                    if len(content_str) < 5000:
                                        logging.debug(f"Detected HTML error page for {url}")
                                        return url, False
                                else:
                                    # Not HTML, likely a stream
                                    return url, True
                            except:
                                # Binary content, likely a stream
                                return url, True
                    except:
                        # Failed to read content but status is 200
                        return url, True
                
                # Accept 206 Partial Content for streams
                elif response.status == 206:
                    return url, True
                
                # Accept redirects for streaming URLs
                elif response.status in [301, 302, 307, 308]:
                    return url, True
                
                return url, False

        except asyncio.TimeoutError:
            logging.debug(f"Timeout checking {url}")
            return url, False
        except aiohttp.ClientError as e:
            logging.debug(f"Client error checking {url}: {e}")
            return url, False
        except Exception as e:
            logging.debug(f"Error checking {url}: {e}")
            return url, False


class M3UProcessor:
    def __init__(self):
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
            async with session.get(m3u_url, timeout=ClientTimeout(total=30),
                                   headers={
                                       'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                                       'Accept': '*/*',
                                       'Accept-Encoding': 'gzip, deflate'
                                   }, ssl=False) as response:
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
            if line.startswith('#EXTINF'):
                current_channel = self._parse_extinf_line(line)
            elif line and not line.startswith('#') and current_channel:
                # Skip URLs with unwanted extensions
                if not self.has_unwanted_extension(line):
                    current_channel['url'] = line
                    channels.append(current_channel)
                current_channel = {}

        return channels

    def _parse_extinf_line(self, line):
        """Parse EXTINF line and extract metadata with improved country and name extraction"""
        attrs = dict(re.findall(r'(\S+)="([^"]*)"', line))
        channel_name = line.split(',')[-1].strip()

        country_code = ''
        clean_name = channel_name

        # Try multiple patterns for country code extraction
        patterns = [
            r'^\|([A-Z]{2})\|',  # |US| Channel Name
            r'^([A-Z]{2})/',  # US/ Channel Name
            r'^([A-Z]{2})\s',  # US Channel Name
            r'group-title="[^"]*?([A-Z]{2})[^"]*"',  # Extract from group-title
        ]

        for pattern in patterns:
            match = re.search(pattern, channel_name, re.IGNORECASE)
            if match:
                country_code = match.group(1).upper()
                # Remove country prefix from name
                clean_name = re.sub(pattern, '', channel_name, flags=re.IGNORECASE).strip()
                break

        # If no country in name, check group-title
        if not country_code and 'group-title' in attrs:
            group_match = re.search(r'([A-Z]{2})', attrs['group-title'])
            if group_match:
                country_code = group_match.group(1).upper()

        # Clean up name - remove any remaining country prefixes
        clean_name = re.sub(r'^[A-Z]{2}[\/\|\s]+', '', clean_name).strip()

        return {
            'tvg_id': attrs.get('tvg-ID', attrs.get('tvg-id', attrs.get('channel-id', ''))),
            'tvg_name': attrs.get('tvg-name', attrs.get('tvg_name', '')),
            'tvg_logo': attrs.get('tvg-logo', attrs.get('tvg_logo', '')),
            'group_title': attrs.get('group-title', attrs.get('group_title', '')),
            'tvg_chno': attrs.get('tvg-chno', ''),
            'display_name': clean_name,
            'country_code': country_code,
            'raw_name': channel_name
        }

    def _extract_categories(self, group_title):
        """Extract categories from group-title"""
        if not group_title:
            return ['general']

        # Clean and split group title
        categories = []
        parts = [p.strip().lower() for p in group_title.split('/') if p.strip()]

        # Remove country codes and emojis
        for part in parts:
            # Skip if it's just a country code
            if re.match(r'^[a-z]{2}$', part) or re.match(r'^[🇦-🇿]{2}$', part):
                continue
            # Skip if it's just an emoji or symbol
            if part in ['|', '🇫🇷', '🇺🇸', '🇬🇧', '🇩🇪', '🇪🇸', '🇮🇹', '🇯🇵', '🇨🇳']:
                continue

            # Clean the category name
            clean_part = re.sub(r'[^\w\s-]', '', part).strip()
            if clean_part:
                categories.append(clean_part)

        if not categories:
            return ['general']

        return categories

    def format_channel_data(self, channels, logos_data):
        """Format channel data into the desired JSON structure"""
        formatted_channels = []

        for channel in channels:
            # Generate channel ID
            if channel['tvg_id']:
                channel_id = channel['tvg_id'].lower()
            elif channel['tvg_name']:
                base_id = re.sub(r'[^a-zA-Z0-9]', '', channel['tvg_name']).lower()
                channel_id = base_id if base_id else ''
            else:
                base_id = re.sub(r'[^a-zA-Z0-9]', '', channel['display_name']).lower()
                channel_id = base_id if base_id else ''

            # Add country code to ID if available
            if channel['country_code'] and channel_id:
                channel_id = f"{channel_id}.{channel['country_code'].lower()}"

            # If still no ID, generate from display name
            if not channel_id:
                base_id = re.sub(r'[^a-zA-Z0-9]', '', channel['display_name']).lower()
                if channel['country_code']:
                    channel_id = f"{base_id}.{channel['country_code'].lower()}"
                else:
                    channel_id = base_id

            # Get logo
            logo_url = channel.get('tvg_logo', '')
            if not logo_url and channel_id:
                matching_logos = [l for l in logos_data if l["channel"] == channel_id]
                if matching_logos:
                    logo_url = matching_logos[0]["url"]

            # Get categories
            categories = self._extract_categories(channel['group_title'])

            formatted_channels.append({
                'name': channel['display_name'],
                'id': channel_id,
                'logo': logo_url,
                'url': channel['url'],
                'categories': categories,
                'country': channel['country_code']
            })

        return formatted_channels


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


async def fetch_json(session, url):
    try:
        async with session.get(url, headers={
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*"
        }, ssl=False) as response:
            response.raise_for_status()
            text = await response.text()
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                # Try to fix common JSON issues
                text = text.strip()
                if text.startswith('{') and text.endswith('}'):
                    return json.loads(text)
                elif text.startswith('[') and text.endswith(']'):
                    return json.loads(text)
                else:
                    logging.error(f"Invalid JSON format for {url}")
                    return []
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
    return []


def load_existing_data():
    """Load all existing channel data from files, ensuring consistency."""
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
    """Clear all JSON files in countries and categories directories to prevent stale data."""
    delete_split_files(WORKING_CHANNELS_BASE)
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

    os.makedirs(COUNTRIES_DIR, exist_ok=True)
    os.makedirs(CATEGORIES_DIR, exist_ok=True)
    channels = remove_duplicates(channels)

    if append:
        existing_working = load_split_json(WORKING_CHANNELS_BASE)
        existing_working.extend(channels)
        channels = remove_duplicates(existing_working)

    # Write (replace or updated) working channels file
    # Always start with working_channels1.json if we have channels
    if channels:
        if len(channels) <= MAX_CHANNELS_PER_FILE:
            # Save as working_channels1.json instead of working_channels.json
            with open(WORKING_CHANNELS_BASE + "1.json", 'w', encoding='utf-8') as f:
                json.dump(channels, f, indent=4, ensure_ascii=False)
        else:
            # Split into multiple files starting with working_channels1.json
            part_num = 1
            for i in range(0, len(channels), MAX_CHANNELS_PER_FILE):
                chunk = channels[i:i + MAX_CHANNELS_PER_FILE]
                part_file = f"{WORKING_CHANNELS_BASE}{part_num}.json"
                with open(part_file, 'w', encoding='utf-8') as f:
                    json.dump(chunk, f, indent=4, ensure_ascii=False)
                part_num += 1

    # For country files
    for country, country_channels in country_files.items():
        if not country or country == "Unknown":
            continue
        safe_country = "".join(c for c in country if c.isalnum() or c in (' ', '*', '-')).rstrip()
        if not safe_country:
            continue

        country_channels = remove_duplicates(country_channels)
        country_base = os.path.join(COUNTRIES_DIR, safe_country)

        if append:
            existing_country = load_split_json(country_base)
            existing_country.extend(country_channels)
            country_channels = remove_duplicates(existing_country)

        # Write (replace or updated)
        save_split_json(country_base, country_channels)

    # For category files
    for category, category_channels in category_files.items():
        if not category:
            continue
        safe_category = "".join(c for c in category if c.isalnum() or c in (' ', '*', '-')).rstrip()
        if not safe_category:
            continue

        category_channels = remove_duplicates(category_channels)
        category_base = os.path.join(CATEGORIES_DIR, safe_category)

        if append:
            existing_category = load_split_json(category_base)
            existing_category.extend(category_channels)
            category_channels = remove_duplicates(existing_category)

        # Write (replace or updated)
        save_split_json(category_base, category_channels)


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
            
            # Check if working
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
    batch_size = BATCH_SIZE

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
            
            # Check if working
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
    batch_size = BATCH_SIZE

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
    """Extract m3u8 URLs from a page."""
    url, index = url_data
    try:
        response = requests.get(url, headers=KENYA_HEADERS, timeout=15)
        m3u8_pattern = r'(https?://[^\s"]+\.m3u8)'
        m3u8_links = re.findall(m3u8_pattern, response.text)

        valid_m3u8_links = [link for link in m3u8_links if 'youtube' not in link.lower()]

        logging.info(f"[{index}] Processed linked page: {url} - Found {len(valid_m3u8_links)} valid m3u8 links")
        return valid_m3u8_links
    except Exception as e:
        logging.error(f"[{index}] Error processing {url}: {str(e)}")
        return []


async def check_single_m3u8_url(session, url, timeout=20):
    """Check if a single m3u8 URL is valid."""
    # Skip URLs with unwanted extensions
    if any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS):
        return url, False

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout),
                               headers={
                                   'User-Agent': 'Mozilla/5.0',
                                   'Accept-Encoding': 'gzip, deflate'
                               }, ssl=False) as response:
            if response.status == 200:
                content = await response.text()
                # For m3u8, we're more lenient - just check if we got content
                if content and ('#EXTM3U' in content or '#EXTINF' in content or len(content) > 10):
                    logging.info(f"Valid m3u8 found: {url}")
                    return url, True
                else:
                    # Even if content is weird, status 200 for m3u8 is often valid
                    logging.info(f"M3U8 with status 200: {url}")
                    return url, True
    except Exception as e:
        logging.error(f"Failed to check {url} (timeout={timeout}s): {e}")
    return url, False


async def check_m3u8_urls(urls):
    """Check multiple m3u8 URLs and return the first working one."""
    async with aiohttp.ClientSession(connector=TCPConnector(ssl=False)) as session:
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

        response = requests.get(KENYA_BASE_URL, headers=KENYA_HEADERS, timeout=15)
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
        # Simple and general: lowercase and remove non-alphanumeric (no specific stripping)
        name = name.lower()
        name = re.sub(r'[^a-z0-9]', '', name)
        return name

    def get_score(a, b):
        # Substring-boosted for dynamic names (e.g., logo embedded in channel name with prefixes/suffixes)
        if a in b or b in a:
            return 1.0
        else:
            return SequenceMatcher(None, a, b).ratio()

    logging.info("Starting Uganda channels fetch...")
    
    # Use the properly encoded URL
    api_url = UGANDA_API_URL
    
    # Log the URL being accessed
    logging.info(f"Fetching Uganda API from: {api_url}")
    
    # Try using requests instead of aiohttp for the Uganda API to avoid Brotli issue
    try:
        logging.info("Trying Uganda API with requests library to avoid Brotli issue...")
        response = requests.get(api_url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate',  # Only accept gzip and deflate, not br
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache'
        }, timeout=30, verify=False)
        
        logging.info(f"Uganda API response status: {response.status_code}")
        if response.status_code == 200:
            text = response.text
            logging.info(f"Uganda API response length: {len(text)} chars")
            if len(text) > 500:
                logging.info(f"Uganda API response first 500 chars: {text[:500]}")
            
            try:
                data = json.loads(text)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse Uganda API response as JSON: {e}")
                # Try to fix common JSON issues
                text = text.strip()
                if text.startswith('{') and text.endswith('}'):
                    data = json.loads(text)
                elif text.startswith('[') and text.endswith(']'):
                    data = json.loads(text)
                else:
                    # Try to extract JSON from text
                    json_match = re.search(r'(\{.*\}|\[.*\])', text, re.DOTALL)
                    if json_match:
                        try:
                            data = json.loads(json_match.group(0))
                        except:
                            logging.error("Could not extract valid JSON from response")
                            return 0
                    else:
                        logging.error("No valid JSON found in response")
                        return 0
            
            posts = data.get("posts", [])
            if not posts and isinstance(data, list):
                posts = data  # Sometimes the API returns an array directly
            
            logging.info(f"Fetched {len(posts)} posts from Uganda API")
        else:
            logging.error(f"Failed to fetch Uganda API: Status {response.status_code}")
            return 0
    except requests.exceptions.Timeout:
        logging.error("Uganda API request timed out after 30 seconds")
        return 0
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error fetching Uganda API: {e}")
        return 0
    except Exception as e:
        logging.error(f"Error fetching Uganda API: {e}")
        return 0

    # Pre-filter Uganda logos as dicts for easy URL access
    ug_logos = [l for l in logos_data if str(l["channel"]).lower().endswith('.ug')]
    channels = []
    country_files = {"UG": []}
    category_files = {}
    # Threshold for a "good" match (0.8 for high confidence, dynamic)
    match_threshold = 0.8

    async def process_post(post):
        if not isinstance(post, dict):
            logging.warning(f"Skipping non-dict post: {post}")
            return None
            
        name = str(post.get("channel_name", post.get("name", "")).strip())
        if not name:
            return None
        url = post.get("channel_url", post.get("url", ""))
        if not url:
            return None

        # Skip URLs with unwanted extensions
        if any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS):
            logging.info(f"Skipping unwanted extension URL for channel: {name}")
            return None

        category = post.get("category_name", post.get("category", "")).lower().strip()
        if not category:
            category = "entertainment"  # default

        # Improved logo search using substring-boosted fuzzy matching
        logo = ""
        best_logo_data = None
        best_score = 0

        # Normalize the input name for searching
        norm_inp = normalize(name)

        for logo_data in ug_logos:
            logo_channel = logo_data["channel"]
            # Normalize the key, removing the domain extension like .ug
            norm_key = normalize(logo_channel.split('.')[0])

            # Calculate boosted similarity score
            score = get_score(norm_inp, norm_key)

            if score > best_score:
                best_score = score
                best_logo_data = logo_data

        ch_id = None
        if best_logo_data and best_score >= match_threshold:
            logo = best_logo_data["url"]  # Assign the logo URL
            ch_id = best_logo_data['channel']  # Use matched logo channel as ID
            logging.info(f"Logo match for {name} (ID: {ch_id}): {logo} with score {best_score:.2f}")
        else:
            # Fallback to normalized ID (no logo)
            base_id = norm_inp
            ch_id = f"{base_id}.ug"
            logging.info(f"No good logo match for {name} (best score: {best_score:.2f}) | No logo assigned | Fallback ID: {ch_id}")

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
                for retry in range(RETRIES):
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
                        for retry in range(RETRIES):
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
    batch_size = BATCH_SIZE

    for batch_start in range(0, total_channels, batch_size):
        batch_end = min(batch_start + batch_size, total_channels)
        current_batch = all_channels[batch_start:batch_end]

        tasks = [check_and_process_channel(channel) for channel in current_batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Error processing channel: {result}")

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

    # Save as working_channels1.json if we have channels
    if all_channels:
        if len(all_channels) <= MAX_CHANNELS_PER_FILE:
            with open(WORKING_CHANNELS_BASE + "1.json", 'w', encoding='utf-8') as f:
                json.dump(all_channels, f, indent=4, ensure_ascii=False)
        else:
            part_num = 1
            for i in range(0, len(all_channels), MAX_CHANNELS_PER_FILE):
                chunk = all_channels[i:i + MAX_CHANNELS_PER_FILE]
                part_file = f"{WORKING_CHANNELS_BASE}{part_num}.json"
                with open(part_file, 'w', encoding='utf-8') as f:
                    json.dump(chunk, f, indent=4, ensure_ascii=False)
                part_num += 1

    logging.info(f"Synced {len(all_channels)} channels to working_channels")


async def process_m3u_urls(session, logos_data, checker, m3u_urls):
    """Process M3U URLs and return count of working channels (all, not just sports)."""
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

            # Process channels in smaller batches to avoid overwhelming the system
            working_channels = []
            total_channels = len(channels)
            
            for batch_start in range(0, total_channels, BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, total_channels)
                current_batch = channels[batch_start:batch_end]
                
                # Create tasks for current batch
                check_tasks = []
                for channel in current_batch:
                    url = channel.get('url')
                    if url and not processor.has_unwanted_extension(url):
                        check_tasks.append(checker.check_single_url(session, url))
                    else:
                        check_tasks.append(asyncio.sleep(0))  # Placeholder for skipped channels
                
                # Check current batch
                check_results = await asyncio.gather(*check_tasks)
                
                # Add working channels from this batch
                for i, (url, is_working) in enumerate(check_results):
                    if is_working and current_batch[i]:
                        working_channels.append(current_batch[i])
                
                # Small delay between batches
                await asyncio.sleep(BATCH_DELAY)

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
    global M3U_URLS  # To update the module-level variable

    logging.info("Starting IPTV channel collection process...")

    # Step 0: Scrape daily M3U URLs and update M3U_URLS
    logging.info("\n=== Step 0: Scraping daily M3U URLs ===")
    scraped_m3u = scrape_daily_m3u_urls(max_working=5)
    M3U_URLS = scraped_m3u + ADDITIONAL_M3U  # Add news, xxx, and the new M3U URLs
    logging.info(f"Updated M3U_URLS with {len(M3U_URLS)} URLs (scraped + additional)")

    checker = FastChecker()

    async with aiohttp.ClientSession(connector=checker.connector) as session:
        # Fetch logos data first
        logging.info("Fetching logos data...")
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

        logging.info("\n=== Step 2: Processing M3U URLs ===")
        m3u_channels_count = await process_m3u_urls(session, logos_data, checker, M3U_URLS)

        logging.info("\n=== Step 3: Checking IPTV-org channels ===")
        try:
            if not CHANNELS_URL or not STREAMS_URL:
                logging.error("CHANNELS_URL or STREAMS_URL is not set. Skipping IPTV-org channels.")
                streams_dict = {}
                channels_data = []
            else:
                logging.info("Fetching IPTV-org data...")
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
        import traceback
        traceback.print_exc()
        sys.exit(1)