import os
from datetime import date, timedelta

# ====================== API URLs ======================
CHANNELS_URL = os.getenv("CHANNELS_URL", "https://iptv-org.github.io/api/channels.json")
STREAMS_URL = os.getenv("STREAMS_URL", "https://iptv-org.github.io/api/streams.json")
LOGOS_URL = os.getenv("LOGOS_URL", "https://iptv-org.github.io/api/logos.json")

KENYA_BASE_URL = os.getenv("KENYA_BASE_URL", "")
UGANDA_API_URL = "https://apps.moochatplus.net/bash/api/api.php?get_posts&page=1&count=100&api_key=cda11bx8aITlKsXCpNB7yVLnOdEGqg342ZFrQzJRetkSoUMi9w"


#world-iptv scrap
WORLD_IPTV_BASE_URLS = [
    "https://world-iptv.club/iptv/",
    "https://ninoiptv.com/home/"
]
WORLD_IPTV_LINKS_PER_SITE = 1
WORLD_IPTV_MAX_TOTAL = 2


# M3U Sources
M3U_URLS = [
    os.getenv("M3U_URL_1", ""),
    os.getenv("M3U_URL_2", "")
]

ADDITIONAL_M3U = [
    "https://raw.githubusercontent.com/abusaeeidx/IPTV-Scraper-Zilla/refs/heads/main/combined-playlist.m3u",
    'https://raw.githubusercontent.com/bugsfreeweb/LiveTVCollector/refs/heads/main/LiveTV/Collection/LiveTV.m3u'
]

# Adult channels source
ADULT_BASE_URL = "https://adult-tv-channels.click"
ADULT_SEARCH_URL = f"{ADULT_BASE_URL}/search.json"
ADULT_REQUEST_DELAY = 1  # seconds between page requests (be gentle)


# ====================== File Paths ======================
WORKING_CHANNELS_BASE = "working_channels"
CATEGORIES_DIR = "categories"
COUNTRIES_DIR = "countries"
FAILED_CHANNELS_FILE = "failed_channels.json"
CATEGORIES_SUMMARY_FILE = "categories.json"
LAST_UPDATE_FILE = "last_update.json"  # NEW: Track last update info

# ====================== Settings ======================
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 200))  # INCREASED from 120 to 200
INITIAL_TIMEOUT = 10  # REDUCED from 25 to 10 for speed
MAX_TIMEOUT = 20  # REDUCED from 40 to 20 for speed
RETRIES = 2  # REDUCED from 3 to 2 for speed
BATCH_SIZE = 1000  # INCREASED from 400 to 1000
BATCH_DELAY = 0.1
MAX_CHANNELS_PER_FILE = 4000
MIN_STREAM_SIZE = 100

# NEW: Performance settings for GitHub Actions
URL_CHECK_TIMEOUT = 5  # Very short timeout per URL
DNS_CACHE_TTL = 600  # Cache DNS for 10 minutes
MAX_CONCURRENT_CHECKS = 200  # High concurrency for powerful runners

# ====================== Filters ======================
UNWANTED_EXTENSIONS = [
    '.mkv', '.mp4', '.avi', '.mov', '.flv', '.wmv',
    '.mp3', '.aac', '.wav', '.m4a', '.webm', '.m4v'
]

ADULT_KEYWORDS = [
    'adult', '18+', 'xxx', 'porn', 'sex', 'erotic', 'hentai',
    'blowjob', 'fuck', 'cum', 'milf', 'teen', 'lesbian', 'ebony'
]

FAKE_HINTS = [
    'mycamtv', 'mysuperhot.com', 'qr code', 'scan to watch',
    'static image', 'pay to watch', 'MYCAMTV'
]

# ====================== Headers ======================
SCRAPER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

KENYA_HEADERS = SCRAPER_HEADERS.copy()

# Progress logging interval
M3U_PROGRESS_INTERVAL = 100   # log every 100 streams checked