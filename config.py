import os
from datetime import date, timedelta

# ====================== API URLs ======================
CHANNELS_URL = os.getenv("CHANNELS_URL", "https://iptv-org.github.io/api/channels.json")
STREAMS_URL = os.getenv("STREAMS_URL", "https://iptv-org.github.io/api/streams.json")
LOGOS_URL = os.getenv("LOGOS_URL", "https://iptv-org.github.io/api/logos.json")

KENYA_BASE_URL = os.getenv("KENYA_BASE_URL", "")
UGANDA_API_URL = "https://apps.moochatplus.net/bash/api/api.php?get_posts&page=1&count=100&api_key=cda11bx8aITlKsXCpNB7yVLnOdEGqg342ZFrQzJRetkSoUMi9w"

# M3U Sources
M3U_URLS = [
    os.getenv("M3U_URL_1", ""),
    os.getenv("M3U_URL_2", "")
]

ADDITIONAL_M3U = [
    "https://raw.githubusercontent.com/abusaeeidx/IPTV-Scraper-Zilla/refs/heads/main/combined-playlist.m3u",
"http://bgdc.live:25461/get.php?username=nmullen144&password=73970570&type=m3u_plus"
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

# ====================== Settings ======================
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 80))
INITIAL_TIMEOUT = 25
MAX_TIMEOUT = 40
RETRIES = 3
BATCH_SIZE = 400
BATCH_DELAY = 0.1
MAX_CHANNELS_PER_FILE = 4000
MIN_STREAM_SIZE = 100

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
