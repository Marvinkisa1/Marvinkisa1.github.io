import aiohttp
import asyncio
import json
import os
import m3u8
from tqdm import tqdm
from aiohttp import ClientTimeout, TCPConnector
from urllib.parse import urlparse, urljoin, parse_qs
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
import html
from datetime import date, timedelta
from typing import List, Dict, Set, Tuple, Optional
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('iptv_collector.log')
    ]
)

# URLs
CHANNELS_URL = os.getenv("CHANNELS_URL", "https://iptv-org.github.io/api/channels.json")
STREAMS_URL = os.getenv("STREAMS_URL", "https://iptv-org.github.io/api/streams.json")
LOGOS_URL = os.getenv("LOGOS_URL", "https://iptv-org.github.io/api/logos.json")
KENYA_BASE_URL = os.getenv("KENYA_BASE_URL", "")
UGANDA_API_URL = "https://apps.moochatplus.net/bash/api/api.php?get_posts&page=1&count=100&api_key=cda11bx8aITlKsXCpNB7yVLnOdEGqg342ZFrQzJRetkSoUMi9w"
M3U_URLS = [
    os.getenv("M3U_URL_1", ""),
    os.getenv("M3U_URL_2", "")
]

# Additional M3U
ADDITIONAL_M3U = [
    "https://raw.githubusercontent.com/ipstreet312/freeiptv/refs/heads/master/all.m3u",
    "https://raw.githubusercontent.com/abusaeeidx/IPTV-Scraper-Zilla/refs/heads/main/combined-playlist.m3u"
]

# File paths
WORKING_CHANNELS_BASE = "working_channels"
CATEGORIES_DIR = "categories"
COUNTRIES_DIR = "countries"
FAILED_CHANNELS_FILE = "failed_channels.json"

# Settings
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 100))
INITIAL_TIMEOUT = 25
MAX_TIMEOUT = 40
RETRIES = 3
BATCH_DELAY = 0.1
BATCH_SIZE = 400
USE_HEAD_METHOD = True
KENYA_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
MAX_CHANNELS_PER_FILE = 4000
MIN_STREAM_SIZE = 100  # Minimum bytes to consider a stream valid

# Unwanted extensions - Direct video/audio files to reject
UNWANTED_EXTENSIONS = [
    '.mkv', '.mp4', '.avi', '.mov', '.flv', '.wmv', 
    '.mp3', '.aac', '.wav', '.wma', '.ogg', '.flac',  # Audio files
    '.webm', '.m4v', '.3gp', '.vob', '.ogv', '.mpg', '.mpeg',  # More video formats
    '.wma', '.m4a', '.opus', '.mk3d', '.mka', '.mks'  # More media formats
]

# Scraper headers
SCRAPER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Referer': 'https://world-iptv.club/',
    'Origin': 'https://world-iptv.club'
}

def delete_split_files(base_name: str):
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

def load_split_json(base_name: str) -> List[Dict]:
    """Load data from split JSON files or the base file."""
    ext = '.json'
    all_data = []
    
    # First try the base file
    if os.path.exists(base_name + ext):
        try:
            with open(base_name + ext, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_data.extend(data)
        except Exception as e:
            logging.error(f"Error loading {base_name}.json: {e}")
    
    # Then try split files
    part = 1
    while True:
        part_file = f"{base_name}{part}{ext}"
        if not os.path.exists(part_file):
            break
        try:
            with open(part_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_data.extend(data)
        except Exception as e:
            logging.error(f"Error loading {part_file}: {e}")
        part += 1
    
    return all_data

def save_split_json(base_name: str, data: List[Dict]):
    """Save data to JSON, splitting if exceeds MAX_CHANNELS_PER_FILE."""
    if not data:
        return
    
    ext = '.json'
    
    # Delete old files first
    delete_split_files(base_name)
    
    if len(data) <= MAX_CHANNELS_PER_FILE:
        with open(base_name + ext, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info(f"Saved {len(data)} channels to {base_name}.json")
    else:
        part_num = 1
        for i in range(0, len(data), MAX_CHANNELS_PER_FILE):
            chunk = data[i:i + MAX_CHANNELS_PER_FILE]
            part_file = f"{base_name}{part_num}{ext}"
            with open(part_file, 'w', encoding='utf-8') as f:
                json.dump(chunk, f, indent=4, ensure_ascii=False)
            logging.info(f"Saved {len(chunk)} channels to {part_file}")
            part_num += 1

def scrape_daily_m3u_urls(max_working: int = 5) -> List[str]:
    """Scrape daily working M3U URLs from world-iptv.club."""
    logging.info("Starting daily M3U URL scraper...")
    
    current_date = date.today().strftime("%d-%m-%Y")
    
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
    pattern = r'<a\s+[^>]*href=[\'"]([^\'"]+)[\'"]'
    matches = re.findall(pattern, content, re.IGNORECASE)
    
    urls = []
    seen = set()
    for match in matches:
        if 'm3u' in match.lower():
            if match.startswith('/'):
                full_url = 'https://world-iptv.club' + match
            elif match.startswith('http'):
                full_url = match
            else:
                continue
            
            if full_url not in seen:
                seen.add(full_url)
                urls.append(full_url)
    
    current_urls = [u for u in urls if f'-{current_date}/' in u]
    
    prev_date = None
    if not current_urls:
        prev_date = (date.today() - timedelta(days=1)).strftime("%d-%m-%Y")
        current_urls = [u for u in urls if f'-{prev_date}/' in u]
    
    top_5_urls = current_urls[:20]
    
    if not top_5_urls:
        fallback_date = (date.today() - timedelta(days=2)).strftime("%d-%m-%Y")
        logging.warning(f"No URLs found for recent dates: {current_date}, {prev_date or 'N/A'}, or {fallback_date}")
        return []
    
    date_used = current_date if f'-{current_date}/' in top_5_urls[0] else prev_date
    logging.info(f"Using date: {date_used}")
    
    working_m3u = []
    for page_url in top_5_urls:
        logging.info(f"\nFetching {page_url}...")
        try:
            resp = requests.get(page_url, headers=SCRAPER_HEADERS, timeout=30)
            if resp.status_code != 200:
                continue
        except Exception:
            continue
        
        page_content = resp.text
        m3u_pattern = r'(?:\.m3u|get\.php\?.*?type=(?:m3u|m3u_plus|m3u8))'
        href_pattern = r'<a\s+[^>]*href=[\'"]([^\'"]+)[\'"]'
        all_hrefs = re.findall(href_pattern, page_content, re.IGNORECASE)
        href_m3u = [html.unescape(h) for h in all_hrefs if re.search(m3u_pattern, h, re.IGNORECASE)]
        
        text_pattern = r'https?://[^\s<>"\']+'
        all_urls_in_text = re.findall(text_pattern, page_content)
        text_m3u = [html.unescape(u) for u in all_urls_in_text if re.search(m3u_pattern, u, re.IGNORECASE)]
        
        m3u_matches = list(set(href_m3u + text_m3u))
        m3u_matches = [urljoin(page_url, m) if not m.startswith('http') else m for m in m3u_matches]
        m3u_matches = list(dict.fromkeys(m3u_matches))
        
        for m3u_match in m3u_matches:
            full_m3u = m3u_match
            try:
                m3u_resp = requests.get(full_m3u, headers=SCRAPER_HEADERS, timeout=30, stream=True)
                if m3u_resp.status_code == 200 and len(m3u_resp.content) > 100:
                    if '#EXT' in m3u_resp.text:
                        working_m3u.append(full_m3u)
                        if len(working_m3u) >= max_working:
                            break
            except Exception:
                continue
        
        if len(working_m3u) >= max_working:
            break
    
    if working_m3u:
        logging.info(f"Scraped {len(working_m3u)} working M3U URLs")
    else:
        logging.warning("No working M3U URLs found")
    
    return working_m3u

class FastChecker:
    def __init__(self):
        self.connector = TCPConnector(
            limit=MAX_CONCURRENT,
            force_close=True,
            enable_cleanup_closed=True,
            ttl_dns_cache=300,
            ssl=False
        )
        self.timeout = ClientTimeout(total=INITIAL_TIMEOUT)
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.session_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Referer': 'https://world-iptv.club/',
            'Origin': 'https://world-iptv.club'
        }
        self.working_cache = {}
        self.failed_cache = {}
        
    def has_unwanted_extension(self, url: str) -> bool:
        """Check if URL has unwanted video/audio file extension."""
        if not url:
            return True
            
        url_lower = url.lower()
        # Remove query parameters for extension checking
        url_without_query = url_lower.split('?')[0].split('#')[0]
        
        # Check for direct video/audio file extensions
        return any(url_without_query.endswith(ext) for ext in UNWANTED_EXTENSIONS)
    
    def is_valid_stream_url(self, url: str) -> bool:
        """Check if URL is a valid stream URL - SIMPLE VERSION."""
        if not url:
            return False
        
        # Only check for unwanted extensions
        # If it doesn't have unwanted extensions, check it
        return not self.has_unwanted_extension(url)
    
    async def validate_m3u8_content(self, content: str, url: str) -> bool:
        """Validate m3u8 content thoroughly."""
        if not content:
            return False
        
        # Must have M3U header
        if '#EXTM3U' not in content:
            # Some streams might not have standard headers but still work
            if '#EXTINF:' in content:
                return True
            return False
        
        try:
            # Try to parse the playlist
            playlist = m3u8.loads(content)
            
            # Check if it's a valid playlist
            if playlist.is_variant:
                return bool(playlist.playlists)
            else:
                return bool(playlist.segments)
                
        except Exception:
            # Check for stream content
            stream_indicators = ['#EXTINF:', '#EXT-X-STREAM-INF:', '.ts']
            return any(indicator in content for indicator in stream_indicators)
    
    async def check_stream_directly(self, session: aiohttp.ClientSession, url: str, timeout: int) -> Tuple[bool, Optional[str]]:
        """Check a stream URL directly with thorough validation."""
        try:
            async with session.get(
                url,
                timeout=ClientTimeout(total=timeout),
                headers=self.session_headers,
                allow_redirects=True,
                raise_for_status=False
            ) as response:
                
                # Check status code
                if response.status not in [200, 206, 302, 307]:
                    return False, f"HTTP {response.status}"
                
                # Handle redirects
                if response.status in [302, 307]:
                    redirect_url = response.headers.get('Location')
                    if redirect_url:
                        # Follow redirect
                        async with session.get(
                            redirect_url,
                            timeout=ClientTimeout(total=timeout),
                            headers=self.session_headers,
                            allow_redirects=True,
                            raise_for_status=False
                        ) as redirect_response:
                            if redirect_response.status != 200:
                                return False, f"Redirect failed: HTTP {redirect_response.status}"
                            response = redirect_response
                    else:
                        return False, "Redirect without location"
                
                # Read first chunk to check content
                chunk = await response.content.read(32768)
                if not chunk:
                    return False, "Empty response"
                
                if len(chunk) < MIN_STREAM_SIZE:
                    # Check if it's a valid but small response
                    try:
                        text = chunk.decode('utf-8', errors='ignore')
                        if '#EXTM3U' in text or '#EXTINF:' in text:
                            return True, "Small but valid playlist"
                    except:
                        pass
                    return False, f"Too small ({len(chunk)} bytes)"
                
                # Check for common error patterns
                try:
                    text = chunk.decode('utf-8', errors='ignore').lower()
                    
                    # Error page indicators
                    error_patterns = [
                        '<html', '<!doctype', '<body', '<head', '<title>',
                        'error', '404', 'not found', 'forbidden', 'access denied',
                        'cloudflare', 'nginx', 'apache'
                    ]
                    
                    # Count error indicators
                    error_count = sum(1 for pattern in error_patterns if pattern in text)
                    if error_count >= 2:
                        return False, f"Error page detected"
                    
                    # Check if it's m3u8/m3u content
                    if '.m3u8' in url.lower() or '.m3u' in url.lower() or '#EXTM3U' in text.upper():
                        if not await self.validate_m3u8_content(text.upper(), url):
                            return False, "Invalid m3u8 content"
                        return True, "Valid m3u8 stream"
                    
                    # Check for other playlist formats
                    if '#EXTINF' in text.upper():
                        return True, "M3U playlist"
                    
                    # For non-M3U content, check if it looks like binary data (likely media stream)
                    if len(chunk) > 1024:
                        non_printable = sum(1 for b in chunk[:1024] if b < 32 and b not in [9, 10, 13])
                        if non_printable > 700:  # Mostly binary data
                            return True, "Binary stream data"
                    
                except UnicodeDecodeError:
                    # Binary content - likely a media stream or segment
                    if len(chunk) >= 4096:
                        return True, "Binary stream data"
                    return False, "Small binary file"
                
                # If we get here and it's not an error page, accept it
                return True, "Stream data"
                
        except asyncio.TimeoutError:
            return False, "Timeout"
        except aiohttp.ClientError as e:
            return False, f"Client error: {str(e)}"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    async def check_single_url(self, session: aiohttp.ClientSession, url: str) -> Tuple[str, bool, Optional[str]]:
        """Check a single URL with multiple strategies."""
        # Skip URLs with unwanted extensions
        if self.has_unwanted_extension(url):
            return url, False, "Unwanted extension"
        
        # Check cache first
        if url in self.working_cache:
            return url, True, "Cached"
        if url in self.failed_cache:
            return url, False, self.failed_cache[url]
        
        for attempt in range(RETRIES + 1):
            try:
                current_timeout = min(INITIAL_TIMEOUT * (attempt + 1), MAX_TIMEOUT)
                
                # Use GET with thorough validation
                is_working, reason = await self.check_stream_directly(session, url, current_timeout)
                
                if is_working:
                    self.working_cache[url] = True
                    return url, True, reason
                else:
                    # Cache failures only on final attempt
                    if attempt == RETRIES:
                        self.failed_cache[url] = reason
                    await asyncio.sleep(0.5 * (attempt + 1))
                    
            except Exception as e:
                if attempt == RETRIES:
                    self.failed_cache[url] = str(e)
                await asyncio.sleep(0.5 * (attempt + 1))
        
        return url, False, self.failed_cache.get(url, "All attempts failed")

class M3UProcessor:
    def __init__(self):
        self.unwanted_extensions = UNWANTED_EXTENSIONS
        
    def has_unwanted_extension(self, url: str) -> bool:
        """Check if URL has unwanted video/audio file extension."""
        if not url:
            return True
            
        url_lower = url.lower()
        # Remove query parameters for extension checking
        url_without_query = url_lower.split('?')[0].split('#')[0]
        
        return any(url_without_query.endswith(ext) for ext in self.unwanted_extensions)
    
    def is_valid_stream_url(self, url: str) -> bool:
        """Check if URL is a valid stream URL (not direct video/audio file)."""
        if not url:
            return False
        
        # First, reject unwanted extensions
        if self.has_unwanted_extension(url):
            return False
        
        url_lower = url.lower()
        
        # Accept streaming formats
        streaming_formats = ['.m3u8', '.m3u']
        if any(url_lower.endswith(fmt) or fmt in url_lower for fmt in streaming_formats):
            return True
        
        # Accept URLs with streaming patterns
        streaming_patterns = [
            '/live.m3u8', '/stream.m3u8', '/playlist.m3u8', 
            '/chunklist.m3u8', '/index.m3u8', '/hls/', '/live/',
            'type=m3u8', 'type=m3u', 'type=m3u_plus', 'format=m3u8', 
            'stream=true', 'live=true'
        ]
        
        if any(pattern in url_lower for pattern in streaming_patterns):
            return True
        
        # Special handling for provider URLs (like get.php with M3U parameters)
        if '/get.php?' in url_lower:
            try:
                parsed = urlparse(url_lower)
                query_params = parse_qs(parsed.query)
                # Check if it has type parameter indicating M3U content
                if any(param in query_params for param in ['type', 'format']):
                    for param in ['type', 'format']:
                        if param in query_params:
                            param_value = query_params[param][0].lower()
                            if any(m3u_type in param_value for m3u_type in ['m3u', 'm3u8', 'm3u_plus']):
                                return True
            except Exception:
                pass
        
        # Special handling for .ts files
        if '.ts' in url_lower:
            # Only accept if clearly HLS segment
            if any(x in url_lower for x in ['m3u8', 'hls', '/seg', '/chunk', 'segment']):
                return True
            return False
        
        # Check for other streaming indicators
        url_path = urlparse(url_lower).path
        if any(keyword in url_path for keyword in ['/live.', '/stream.', '/manifest.']):
            return True
        
        return False
    
    async def fetch_m3u_content(self, session: aiohttp.ClientSession, m3u_url: str) -> Optional[str]:
        """Fetch M3U content from URL."""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Referer': 'https://world-iptv.club/',
                'Origin': 'https://world-iptv.club'
            }
            
            async with session.get(m3u_url, timeout=ClientTimeout(total=INITIAL_TIMEOUT), headers=headers) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logging.debug(f"Failed to fetch M3U {m3u_url}: HTTP {response.status}")
                    return None
        except Exception as e:
            logging.debug(f"Error fetching M3U {m3u_url}: {e}")
            return None
    
    def parse_m3u(self, content: str) -> List[Dict]:
        """Parse M3U content and extract channel information, filtering out non-stream URLs."""
        channels = []
        current_channel = {}
        
        for line in content.split('\n'):
            line = line.strip()
            if line.startswith('#EXTINF:-1') or line.startswith('#EXTINF:'):
                current_channel = self._parse_extinf_line(line)
            elif line and not line.startswith('#') and current_channel:
                # Try to accept the URL and validate later
                current_channel['url'] = line
                channels.append(current_channel)
                current_channel = {}
        
        logging.info(f"Parsed M3U: {len(channels)} URLs found (will be validated later)")
        return channels
    
    def _parse_extinf_line(self, line: str) -> Dict:
        """Parse EXTINF line and extract metadata."""
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
    
    def _extract_categories(self, group_title: str) -> List[str]:
        """Extract categories from group-title."""
        if not group_title:
            return ['general']
        parts = [p.strip().lower() for p in group_title.split('/') if p.strip()]
        if len(parts) > 1 and re.match(r'^[a-z]{2}$', parts[0]):
            return parts[1:]
        return parts
    
    def format_channel_data(self, channels: List[Dict], logos_data: List[Dict]) -> List[Dict]:
        """Format channel data into JSON structure."""
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

def remove_duplicates(channels: List[Dict]) -> List[Dict]:
    """Remove duplicate channels by URL and ID."""
    seen_urls = set()
    seen_ids = set()
    unique_channels = []
    
    for channel in channels:
        channel_url = channel.get("url")
        channel_id = channel.get("id")
        
        if not channel_url or not channel_id:
            continue
        
        # Use normalized URL for comparison (lowercase, remove trailing slash)
        normalized_url = channel_url.lower().rstrip('/')
        url_hash = hashlib.md5(normalized_url.encode()).hexdigest()
        
        if url_hash not in seen_urls and channel_id not in seen_ids:
            seen_urls.add(url_hash)
            seen_ids.add(channel_id)
            unique_channels.append(channel)
        else:
            logging.debug(f"Removed duplicate: {channel.get('name')} - {channel_url}")
    
    return unique_channels

async def fetch_json(session: aiohttp.ClientSession, url: str) -> List[Dict]:
    """Fetch JSON data from URL."""
    try:
        async with session.get(url, headers=SCRAPER_HEADERS) as response:
            if response.status == 200:
                text = await response.text()
                return json.loads(text)
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
    return []

def load_existing_data() -> Dict:
    """Load all existing channel data from files."""
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
            if filename.endswith(".json"):
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
    """Clear all JSON files in countries and categories directories."""
    delete_split_files(WORKING_CHANNELS_BASE)
    for dir_path in [COUNTRIES_DIR, CATEGORIES_DIR]:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        os.makedirs(dir_path, exist_ok=True)

def save_channels(channels: List[Dict], country_files: Dict, category_files: Dict, append: bool = False):
    """Save channels to files."""
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

def update_logos_for_null_channels(channels: List[Dict], logos_data: List[Dict]) -> List[Dict]:
    """Update logos for channels with null logos."""
    updated_count = 0
    
    for channel in channels:
        if not channel.get("logo") or channel.get("logo") in [None, "null", ""]:
            channel_id = channel.get("id")
            if channel_id:
                matching_logos = [logo for logo in logos_data if logo["channel"] == channel_id]
                if matching_logos:
                    channel["logo"] = matching_logos[0]["url"]
                    updated_count += 1
    
    logging.info(f"Updated logos for {updated_count} channels")
    return channels

async def validate_channels(session: aiohttp.ClientSession, checker: FastChecker, 
                          all_existing_channels: List[Dict], iptv_channel_ids: Set, 
                          logos_data: List[Dict]) -> int:
    """Validate existing channels and collect only working ones."""
    valid_channels_count = 0
    valid_channels = []
    country_files = {}
    category_files = {}
    
    all_existing_channels = update_logos_for_null_channels(all_existing_channels, logos_data)
    
    async def validate_channel(channel: Dict) -> Optional[Dict]:
        async with checker.semaphore:
            channel_url = channel.get("url")
            if not channel_url:
                return None
            
            if checker.has_unwanted_extension(channel_url):
                return None
            
            ch_id = channel.get("id")
            if ch_id:
                matching_logos = [l for l in logos_data if l["channel"] == ch_id]
                if matching_logos:
                    channel["logo"] = matching_logos[0]["url"]
            
            url, is_working, reason = await checker.check_single_url(session, channel_url)
            if is_working:
                channel_copy = channel.copy()
                channel_copy["country"] = channel.get("country", "Unknown")
                channel_copy["categories"] = channel.get("categories", [])
                return channel_copy
            return None
    
    total_channels = len(all_existing_channels)
    
    with tqdm(total=total_channels, desc="Validating channels") as pbar:
        batch_size = BATCH_SIZE
        
        for batch_start in range(0, total_channels, batch_size):
            batch_end = min(batch_start + batch_size, total_channels)
            current_batch = all_existing_channels[batch_start:batch_end]
            
            tasks = [validate_channel(channel) for channel in current_batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    continue
                elif result:
                    valid_channels.append(result)
                    country = result.get("country", "Unknown")
                    if country and country != "Unknown":
                        country_files.setdefault(country, []).append(result)
                    for cat in result.get("categories", []):
                        if cat:
                            category_files.setdefault(cat, []).append(result)
                    valid_channels_count += 1
            
            pbar.update(len(current_batch))
            await asyncio.sleep(BATCH_DELAY)
    
    save_channels(valid_channels, country_files, category_files, append=False)
    logging.info(f"Validated {valid_channels_count} working channels")
    return valid_channels_count

async def check_iptv_channels(session: aiohttp.ClientSession, checker: FastChecker,
                            channels_data: List[Dict], streams_dict: Dict,
                            existing_urls: Set, logos_data: List[Dict]) -> int:
    """Check and add new IPTV channels that are working."""
    new_iptv_channels_count = 0
    new_channels = []
    country_files = {}
    category_files = {}
    
    channels_to_check = [
        channel for channel in channels_data
        if channel.get("id") in streams_dict and 
        streams_dict[channel["id"]].get("url") not in existing_urls
    ]
    
    async def process_channel(channel: Dict) -> Optional[Dict]:
        async with checker.semaphore:
            stream = streams_dict[channel["id"]]
            url = stream.get("url")
            if not url:
                return None
            
            if checker.has_unwanted_extension(url):
                return None
            
            logo_url = ""
            ch_id = channel.get("id")
            feed = stream.get("feed")
            
            matching_logos = [l for l in logos_data if l["channel"] == ch_id and l.get("feed") == feed]
            if matching_logos:
                logo_url = matching_logos[0]["url"]
            else:
                channel_logos = [l for l in logos_data if l["channel"] == ch_id]
                if channel_logos:
                    logo_url = channel_logos[0]["url"]
            
            url, is_working, reason = await checker.check_single_url(session, url)
            if is_working:
                channel_data = {
                    "name": channel.get("name", "Unknown"),
                    "id": channel.get("id"),
                    "logo": logo_url,
                    "url": url,
                    "categories": channel.get("categories", []),
                    "country": channel.get("country", "Unknown"),
                }
                return channel_data
            return None
    
    total_channels = len(channels_to_check)
    
    with tqdm(total=total_channels, desc="Checking IPTV channels") as pbar:
        batch_size = BATCH_SIZE
        
        for batch_start in range(0, total_channels, batch_size):
            batch_end = min(batch_start + batch_size, total_channels)
            current_batch = channels_to_check[batch_start:batch_end]
            
            tasks = [process_channel(channel) for channel in current_batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    continue
                elif result:
                    new_channels.append(result)
                    country = result.get("country", "Unknown")
                    if country and country != "Unknown":
                        country_files.setdefault(country, []).append(result)
                    for cat in result.get("categories", []):
                        if cat:
                            category_files.setdefault(cat, []).append(result)
                    new_iptv_channels_count += 1
            
            pbar.update(len(current_batch))
            await asyncio.sleep(BATCH_DELAY)
    
    save_channels(new_channels, country_files, category_files, append=True)
    logging.info(f"Added {new_iptv_channels_count} new IPTV channels")
    return new_iptv_channels_count

def get_m3u8_from_page(url_data: Tuple[str, int]) -> List[str]:
    """Extract m3u8 URLs from a page."""
    url, index = url_data
    try:
        response = requests.get(url, headers=KENYA_HEADERS, timeout=10)
        m3u8_pattern = r'(https?://[^\s\'"]+\.m3u8)'
        m3u8_links = re.findall(m3u8_pattern, response.text)
        valid_m3u8_links = [link for link in m3u8_links if 'youtube' not in link.lower()]
        return valid_m3u8_links
    except Exception as e:
        logging.error(f"Error processing {url}: {e}")
        return []

async def check_single_m3u8_url(session: aiohttp.ClientSession, url: str, timeout: int = 15) -> Tuple[str, bool]:
    """Check if a single m3u8 URL is valid."""
    if any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS):
        return url, False
    
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status == 200:
                content = await response.text()
                if '#EXTM3U' in content:
                    try:
                        playlist = m3u8.loads(content)
                        if playlist.segments or playlist.playlists:
                            return url, True
                    except:
                        if any(x in content for x in ['#EXTINF:', '#EXT-X-STREAM-INF:', '.ts']):
                            return url, True
    except Exception:
        pass
    return url, False

async def check_m3u8_urls(urls: List[str]) -> Optional[str]:
    """Check multiple m3u8 URLs and return the first working one."""
    async with aiohttp.ClientSession() as session:
        tasks = [check_single_m3u8_url(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        for url, is_valid in results:
            if is_valid:
                return url
        return None

async def scrape_kenya_tv_channels(logos_data: List[Dict]) -> List[Dict]:
    """Scrape Kenya TV channels and assign logos from LOGOS_URL."""
    start_time = time.time()
    logging.info("Starting Kenya TV scrape...")
    
    try:
        if not KENYA_BASE_URL:
            logging.error("KENYA_BASE_URL is not set")
            return []
        
        response = requests.get(KENYA_BASE_URL, headers=KENYA_HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        main_tag = soup.find('main')
        if not main_tag:
            return []
        
        section = main_tag.find('section', class_='tv-grid-container')
        if not section:
            return []
        
        tv_cards = section.find_all('article', class_='tv-card')
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
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            m3u8_lists = list(executor.map(get_m3u8_from_page, urls_to_process))
        
        valid_urls = await asyncio.gather(
            *[check_m3u8_urls(url_list) for url_list in m3u8_lists]
        )
        
        filtered_results = []
        for channel_data, valid_url in zip(results, valid_urls):
            if valid_url:
                channel_data["url"] = valid_url
                filtered_results.append(channel_data)
        
        logging.info(f"Found {len(filtered_results)} working Kenya channels in {time.time() - start_time:.2f}s")
        return remove_duplicates(filtered_results)
    
    except Exception as e:
        logging.error(f"Error in Kenya TV scrape: {e}")
        return []

async def fetch_and_process_uganda_channels(session: aiohttp.ClientSession, 
                                          checker: FastChecker, 
                                          logos_data: List[Dict]) -> int:
    """Fetch and process Uganda channels from API."""
    def normalize(name: str) -> str:
        name = name.lower()
        name = re.sub(r'[^a-z0-9]', '', name)
        return name
    
    logging.info("Starting Uganda channels fetch...")
    
    try:
        async with session.get(UGANDA_API_URL) as response:
            if response.status == 200:
                data = await response.json()
                posts = data.get("posts", [])
            else:
                logging.error(f"Failed to fetch Uganda API: {response.status}")
                return 0
    except Exception as e:
        logging.error(f"Error fetching Uganda API: {e}")
        return 0
    
    ug_logos = [l for l in logos_data if str(l["channel"]).lower().endswith('.ug')]
    channels = []
    country_files = {"UG": []}
    category_files = {}
    
    async def process_post(post: Dict) -> Optional[Dict]:
        name = str(post.get("channel_name", "").strip())
        if not name:
            return None
        
        url = post.get("channel_url")
        if not url:
            return None
        
        if any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS):
            return None
        
        category = post.get("category_name", "").lower().strip() or "entertainment"
        
        logo = ""
        ch_id = None
        best_score = 0
        norm_inp = normalize(name)
        
        for logo_data in ug_logos:
            logo_channel = logo_data["channel"]
            norm_key = normalize(logo_channel.split('.')[0])
            score = SequenceMatcher(None, norm_inp, norm_key).ratio()
            
            if score > best_score:
                best_score = score
                if best_score >= 0.8:
                    logo = logo_data["url"]
                    ch_id = logo_data['channel']
        
        if not ch_id:
            base_id = norm_inp
            ch_id = f"{base_id}.ug"
        
        channel = {
            "name": name,
            "id": ch_id,
            "logo": logo,
            "url": url,
            "categories": [category],
            "country": "UG"
        }
        
        url, is_working, reason = await checker.check_single_url(session, url)
        if is_working:
            return channel
        return None
    
    tasks = [process_post(post) for post in posts]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for result in results:
        if isinstance(result, Exception):
            continue
        elif result:
            channels.append(result)
            country_files["UG"].append(result)
            cat = result["categories"][0]
            category_files.setdefault(cat, []).append(result)
    
    if channels:
        save_channels(channels, country_files, category_files, append=True)
        logging.info(f"Added {len(channels)} working Uganda channels")
    
    return len(channels)

async def clean_and_replace_channels(session: aiohttp.ClientSession, checker: FastChecker,
                                   all_channels: List[Dict], streams_dict: Dict,
                                   m3u_channels: List[Dict], logos_data: List[Dict]) -> Tuple[int, int, int]:
    """Clean non-working channels and replace URLs if possible."""
    logging.info("\n=== Step 5: Cleaning non-working channels and replacing URLs ===")
    
    all_channels = update_logos_for_null_channels(all_channels, logos_data)
    
    valid_channels = []
    replaced_channels = 0
    non_working_channels = 0
    country_files = {}
    category_files = {}
    
    # Track failed channels for debugging
    failed_channels_info = []
    
    async def find_replacement_url(channel: Dict, streams_dict: Dict, 
                                 m3u_channels: List[Dict], 
                                 session: aiohttp.ClientSession, 
                                 checker: FastChecker) -> Optional[str]:
        channel_id = channel.get("id")
        channel_name = channel.get("name", "").lower()
        
        # Check IPTV-org streams
        if streams_dict and channel_id in streams_dict:
            new_url = streams_dict[channel_id].get("url")
            if new_url and not checker.has_unwanted_extension(new_url):
                url, is_working, reason = await checker.check_single_url(session, new_url)
                if is_working:
                    logging.debug(f"Found replacement from IPTV-org for {channel_name}")
                    return new_url
        
        # Check M3U channels by name similarity
        if m3u_channels:
            for m3u_channel in m3u_channels:
                m3u_name = m3u_channel.get("display_name", "").lower()
                if fuzz.ratio(channel_name, m3u_name) > 80:
                    new_url = m3u_channel.get("url")
                    if new_url and not checker.has_unwanted_extension(new_url):
                        url, is_working, reason = await checker.check_single_url(session, new_url)
                        if is_working:
                            logging.debug(f"Found replacement from M3U for {channel_name}")
                            return new_url
        
        return None
    
    async def check_and_process_channel(channel: Dict):
        nonlocal valid_channels, non_working_channels, replaced_channels
        
        channel_url = channel.get("url")
        channel_name = channel.get("name", "Unknown")
        
        if not channel_url:
            return
        
        if checker.has_unwanted_extension(channel_url):
            non_working_channels += 1
            return
        
        channel_id = channel.get("id")
        if channel_id:
            matching_logos = [logo for logo in logos_data if logo["channel"] == channel_id]
            if matching_logos:
                channel["logo"] = matching_logos[0]["url"]
        
        url, is_working, reason = await checker.check_single_url(session, channel_url)
        
        if is_working:
            valid_channels.append(channel)
            country = channel.get("country", "Unknown")
            if country and country != "Unknown":
                country_files.setdefault(country, []).append(channel)
            for cat in channel.get("categories", []):
                if cat:
                    category_files.setdefault(cat, []).append(channel)
        else:
            logging.debug(f"Channel not working: {channel_name} - {reason}")
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
            else:
                non_working_channels += 1
                failed_channels_info.append({
                    "name": channel_name,
                    "id": channel_id,
                    "url": channel_url,
                    "reason": reason
                })
    
    total_channels = len(all_channels)
    
    with tqdm(total=total_channels, desc="Cleaning channels") as pbar:
        batch_size = BATCH_SIZE
        
        for batch_start in range(0, total_channels, batch_size):
            batch_end = min(batch_start + batch_size, total_channels)
            current_batch = all_channels[batch_start:batch_end]
            
            tasks = [check_and_process_channel(channel) for channel in current_batch]
            await asyncio.gather(*tasks)
            
            pbar.update(len(current_batch))
            await asyncio.sleep(BATCH_DELAY)
    
    # Save failed channels for debugging
    if failed_channels_info:
        with open(FAILED_CHANNELS_FILE, 'w', encoding='utf-8') as f:
            json.dump(failed_channels_info, f, indent=4, ensure_ascii=False)
        logging.info(f"Saved {len(failed_channels_info)} failed channels to {FAILED_CHANNELS_FILE}")
    
    # IMPORTANT: Use append=False to replace all files
    save_channels(valid_channels, country_files, category_files, append=False)
    
    logging.info(f"Replaced {replaced_channels} channels with new URLs")
    logging.info(f"Removed {non_working_channels} non-working channels")
    logging.info(f"Total channels after cleaning: {len(valid_channels)}")
    
    return len(valid_channels), non_working_channels, replaced_channels

def sync_working_channels():
    """Sync all channels from country and category files to the main working_channels.json."""
    logging.info("Syncing all channels to working_channels...")
    
    all_channels = []
    
    if os.path.exists(COUNTRIES_DIR):
        for filename in os.listdir(COUNTRIES_DIR):
            if filename.endswith(".json"):
                base = os.path.join(COUNTRIES_DIR, filename[:-5])
                channels = load_split_json(base)
                all_channels.extend(channels)
    
    if os.path.exists(CATEGORIES_DIR):
        for filename in os.listdir(CATEGORIES_DIR):
            if filename.endswith(".json"):
                base = os.path.join(CATEGORIES_DIR, filename[:-5])
                channels = load_split_json(base)
                all_channels.extend(channels)
    
    all_channels = remove_duplicates(all_channels)
    save_split_json(WORKING_CHANNELS_BASE, all_channels)
    
    logging.info(f"Synced {len(all_channels)} channels to working_channels")
    return len(all_channels)

async def process_m3u_urls(session: aiohttp.ClientSession, logos_data: List[Dict],
                          checker: FastChecker, m3u_urls: List[str]) -> int:
    """Process M3U URLs and return count of working channels."""
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
            
            check_tasks = [checker.check_single_url(session, channel['url']) 
                          for channel in channels if 'url' in channel]
            check_results = await asyncio.gather(*check_tasks)
            
            working_channels = []
            for i, (url, is_working, reason) in enumerate(check_results):
                if is_working and i < len(channels):
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

async def perform_final_validation(session: aiohttp.ClientSession, 
                                 checker: FastChecker, 
                                 max_workers: int = 50) -> Tuple[int, int]:
    """Perform final validation on all collected channels."""
    logging.info("\n=== Step 7: Final validation of all channels ===")
    
    all_channels = load_split_json(WORKING_CHANNELS_BASE)
    
    if not all_channels:
        logging.warning("No channels to validate")
        return 0, 0
    
    logging.info(f"Validating {len(all_channels)} channels...")
    
    working_channels = []
    failed_channels_info = []
    
    semaphore = asyncio.Semaphore(max_workers)
    
    async def validate_channel(channel: Dict) -> Optional[Dict]:
        async with semaphore:
            url = channel.get("url")
            if not url:
                return None
            
            # Use more aggressive checking for final validation
            url, is_working, reason = await checker.check_single_url(session, url)
            
            if is_working:
                return channel
            else:
                # Track why it failed
                failed_channels_info.append({
                    "name": channel.get("name", "Unknown"),
                    "id": channel.get("id"),
                    "url": url,
                    "reason": reason
                })
                return None
    
    with tqdm(total=len(all_channels), desc="Final validation") as pbar:
        batch_size = 100
        
        for batch_start in range(0, len(all_channels), batch_size):
            batch_end = min(batch_start + batch_size, len(all_channels))
            batch = all_channels[batch_start:batch_end]
            
            tasks = [validate_channel(channel) for channel in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    continue
                elif result:
                    working_channels.append(result)
            
            pbar.update(len(batch))
            await asyncio.sleep(0.2)
    
    # Save only working channels
    if working_channels:
        country_files = {}
        category_files = {}
        
        for channel in working_channels:
            country = channel.get("country", "Unknown")
            if country and country != "Unknown":
                country_files.setdefault(country, []).append(channel)
            
            for cat in channel.get("categories", []):
                if cat:
                    category_files.setdefault(cat, []).append(channel)
        
        # IMPORTANT: Use append=False to replace all files
        save_channels(working_channels, country_files, category_files, append=False)
        
        logging.info(f"Final validation complete: {len(working_channels)} working channels, {len(failed_channels_info)} failed")
        
        if failed_channels_info:
            # Save failed channels for debugging
            failed_file = "final_failed_channels.json"
            with open(failed_file, 'w', encoding='utf-8') as f:
                json.dump(failed_channels_info, f, indent=4, ensure_ascii=False)
            logging.info(f"Saved failed channels to {failed_file}")
    
    return len(working_channels), len(failed_channels_info)

async def main():
    global M3U_URLS
    
    logging.info("Starting IPTV channel collection process...")
    
    # Step 0: Scrape daily M3U URLs
    logging.info("\n=== Step 0: Scraping daily M3U URLs ===")
    scraped_m3u = scrape_daily_m3u_urls(max_working=15)
    M3U_URLS = scraped_m3u + ADDITIONAL_M3U
    logging.info(f"Updated M3U_URLS with {len(M3U_URLS)} URLs")
    
    checker = FastChecker()
    
    async with aiohttp.ClientSession(
        connector=checker.connector,
        headers=checker.session_headers
    ) as session:
        # Fetch logos data first
        logos_data = await fetch_json(session, LOGOS_URL)
        logging.info(f"Loaded {len(logos_data)} logos")
        
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
        
        logging.info("\n=== Step 2: Processing M3U URLs ===")
        m3u_channels_count = await process_m3u_urls(session, logos_data, checker, M3U_URLS)
        
        logging.info("\n=== Step 3: Checking IPTV-org channels ===")
        try:
            if not CHANNELS_URL or not STREAMS_URL:
                logging.error("CHANNELS_URL or STREAMS_URL is not set")
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
                
                total_channels = (valid_channels_count + new_iptv_channels_count + 
                                m3u_channels_count + ug_channels_count + len(kenya_channels))
                logging.info(f"\nTotal working channels before cleaning: {total_channels}")
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
        
        logging.info("\n=== Step 7: Final validation ===")
        final_working_count, final_failed_count = await perform_final_validation(session, checker, max_workers=50)
        
        logging.info("\n=== Process completed ===")
        logging.info(f"Final count: {final_working_count} verified working channels")
        logging.info(f"Total removed non-working channels: {non_working_count + final_failed_count}")
        logging.info(f"Channels replaced: {replaced_count}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Script failed: {e}", exc_info=True)
        sys.exit(1)