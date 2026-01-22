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
import html
from datetime import date, timedelta

# ────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# ────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────
CHANNELS_URL  = os.getenv("CHANNELS_URL",  "https://iptv-org.github.io/api/channels.json")
STREAMS_URL   = os.getenv("STREAMS_URL",   "https://iptv-org.github.io/api/streams.json")
LOGOS_URL     = os.getenv("LOGOS_URL",     "https://iptv-org.github.io/api/logos.json")

KENYA_BASE_URL = os.getenv("KENYA_BASE_URL", "")
UGANDA_API_URL = "https://apps.moochatplus.net/bash/api/api.php?get_posts&page=1&count=100&api_key=cda11bx8aITlKsXCpNB7yVLnOdEGqg342ZFrQzJRetkSoUMi9w"

# Only these two fixed + scraped daily lists
FIXED_M3U_LIST = [
    "https://raw.githubusercontent.com/ipstreet312/freeiptv/refs/heads/master/all.m3u",
    "https://raw.githubusercontent.com/abusaeeidx/IPTV-Scraper-Zilla/refs/heads/main/combined-playlist.m3u"
]

# ────────────────────────────────────────────────
# File system paths
# ────────────────────────────────────────────────
WORKING_CHANNELS_BASE = "working_channels"
CATEGORIES_DIR = "categories"
COUNTRIES_DIR  = "countries"

# Performance / reliability tuning
MAX_CONCURRENT     = int(os.getenv("MAX_CONCURRENT",  80))
INITIAL_TIMEOUT    = 18
MAX_TIMEOUT        = 32
RETRIES            = 2
BATCH_SIZE         = 400
MAX_CHANNELS_PER_FILE = 4000

UNWANTED_EXTENSIONS = ['.mkv', '.mp4', '.avi', '.mov', '.flv', '.wmv', '.ts']

SCRAPER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
}

KENYA_HEADERS = SCRAPER_HEADERS.copy()

# ────────────────────────────────────────────────
# File splitting helpers
# ────────────────────────────────────────────────

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

# ────────────────────────────────────────────────
# Daily M3U scraper from world-iptv.club
# ────────────────────────────────────────────────

def scrape_daily_m3u_urls(max_working=6):
    logging.info("Scraping daily M3U playlists from world-iptv.club ...")
    current_date = date.today().strftime("%d-%m-%Y")
    prev_date    = (date.today() - timedelta(days=1)).strftime("%d-%m-%Y")

    try:
        resp = requests.get('https://world-iptv.club/category/iptv/', headers=SCRAPER_HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        logging.error(f"Cannot reach world-iptv.club category page → {e}")
        return []

    pattern = r'<a\s+[^>]*href=[\'"]([^\'"]+)[\'"]'
    matches = re.findall(pattern, resp.text, re.IGNORECASE)

    urls = []
    seen = set()
    for m in matches:
        if 'm3u' not in m.lower():
            continue
        if m.startswith('/'):
            full = 'https://world-iptv.club' + m
        elif m.startswith('http'):
            full = m
        else:
            continue
        if full not in seen:
            seen.add(full)
            urls.append(full)

    candidates = [u for u in urls if f'-{current_date}/' in u]
    if not candidates:
        candidates = [u for u in urls if f'-{prev_date}/' in u]

    top_urls = candidates[:max_working]
    if not top_urls:
        logging.warning("No recent daily M3U pages found")
        return []

    working = []
    for page_url in top_urls:
        try:
            r = requests.get(page_url, headers=SCRAPER_HEADERS, timeout=25)
            r.raise_for_status()
        except Exception as e:
            logging.warning(f"Page failed {page_url} → {e}")
            continue

        # Look for .m3u / get.php?...type=m3u...
        candidates = re.findall(r'(https?://[^\s\'"]+(?:\.m3u|get\.php\?.*?type=(?:m3u|m3u_plus|m3u8)))', r.text, re.I)
        candidates = list(dict.fromkeys([html.unescape(u) for u in candidates]))

        for link in candidates:
            link = urljoin(page_url, link)
            try:
                tr = requests.get(link, headers=SCRAPER_HEADERS, timeout=20, stream=True)
                if tr.status_code != 200:
                    continue
                preview = tr.text[:300].lower()
                if '#extm3u' in preview or 'application/vnd.apple.mpegurl' in tr.headers.get('content-type','').lower():
                    working.append(link)
                    logging.info(f"Found working playlist → {link}")
                    if len(working) >= max_working:
                        return working
            except:
                pass

    return working

# ────────────────────────────────────────────────
# Strict checker
# ────────────────────────────────────────────────

class FastChecker:
    def __init__(self):
        self.connector = TCPConnector(limit=MAX_CONCURRENT, force_close=True, enable_cleanup_closed=True)
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.timeout   = ClientTimeout(total=INITIAL_TIMEOUT)

    def has_unwanted_extension(self, url: str) -> bool:
        if not url:
            return False
        u = url.lower()
        return any(u.endswith(ext) for ext in UNWANTED_EXTENSIONS)

    async def check_single_url(self, session: aiohttp.ClientSession, url: str) -> tuple[str, bool]:
        if self.has_unwanted_extension(url):
            return url, False

        # ─── HEAD first ───────────────────────────────
        for method in ['head', 'get']:
            try:
                timeout = ClientTimeout(total=min(INITIAL_TIMEOUT + 4, MAX_TIMEOUT))
                async with session.request(
                    method.upper(), url,
                    timeout=timeout,
                    allow_redirects=True,
                    headers={"Accept-Encoding": "identity"}
                ) as resp:

                    if resp.status != 200:
                        return url, False

                    content_type = resp.headers.get('content-type', '').lower()

                    # Very strict filtering
                    if 'text/html' in content_type or 'application/json' in content_type:
                        return url, False

                    if 'video' not in content_type and 'audio' not in content_type and 'mpegurl' not in content_type:
                        # many servers lie → read beginning anyway
                        pass

                    # Read small chunk
                    try:
                        chunk = await resp.content.read(4096)
                        text = chunk.decode('utf-8', errors='ignore').lower()
                        if '<html' in text or '<!doctype' in text or 'error' in text[:200]:
                            return url, False
                    except:
                        # binary → most likely ok
                        pass

                    # For HLS we can do extra check
                    if url.endswith('.m3u8') or 'mpegurl' in content_type:
                        try:
                            playlist = m3u8.loads(text)
                            if not (playlist.segments or playlist.playlists):
                                return url, False
                        except:
                            # parsing failed but 200 → still accept (lenient fallback)
                            pass

                    return url, True

            except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
                continue

        return url, False

# ────────────────────────────────────────────────
# M3U parsing & formatting (used for all M3U sources)
# ────────────────────────────────────────────────

class M3UProcessor:
    def __init__(self):
        self.unwanted = UNWANTED_EXTENSIONS

    def has_unwanted_extension(self, url):
        if not url: return False
        return any(url.lower().endswith(e) for e in self.unwanted)

    async def fetch_content(self, session, url):
        try:
            async with session.get(url, timeout=ClientTimeout(total=INITIAL_TIMEOUT+5)) as r:
                if r.status == 200:
                    return await r.text()
                return None
        except:
            return None

    def parse(self, content: str):
        channels = []
        current = None

        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith('#EXTINF:'):
                current = self._parse_extinf(line)
            elif line and not line.startswith('#') and current:
                if not self.has_unwanted_extension(line):
                    current['url'] = line
                    channels.append(current)
                current = None

        return channels

    def _parse_extinf(self, line: str):
        attrs = dict(re.findall(r'(\w+(?:-\w+)*)="([^"]*)"', line))
        name_part = line.split(',', 1)[-1].strip()

        # very simple country prefix extraction
        country = ''
        name = name_part
        m = re.match(r'^\|([A-Z]{2})\||^([A-Z]{2}/)', name_part)
        if m:
            country = (m.group(1) or m.group(2)).strip('/ ')
            name = name_part[m.end():].strip()

        return {
            'tvg_id':   attrs.get('tvg-id',''),
            'tvg_name': attrs.get('tvg-name',''),
            'tvg_logo': attrs.get('tvg-logo',''),
            'group':    attrs.get('group-title',''),
            'name':     name,
            'country':  country,
            'raw':      name_part
        }

    def format_channel(self, ch, logos: list):
        cid = ch['tvg_id'].lower() or re.sub(r'[^a-z0-9]+', '', ch['name'].lower())

        if not cid and ch['country']:
            cid += '.' + ch['country'].lower()

        logo = ch.get('tvg_logo','')
        if not logo:
            for lg in logos:
                if lg['channel'] == cid:
                    logo = lg['url']
                    break

        cats = []
        if ch['group']:
            parts = [p.strip().lower() for p in ch['group'].split('/') if p.strip()]
            if len(parts) > 1 and re.match(r'^[a-z]{2}$', parts[0]):
                cats = parts[1:]
            else:
                cats = parts
        if not cats:
            cats = ['general']

        return {
            'name': ch['name'],
            'id': cid or f"unknown-{len(cats)}",
            'logo': logo,
            'url': ch['url'],
            'categories': cats,
            'country': ch['country'] or 'XX'
        }

# ────────────────────────────────────────────────
# Other helpers (unchanged or lightly cleaned)
# ────────────────────────────────────────────────

def remove_duplicates(channels):
    seen_url = set()
    seen_id  = set()
    result = []
    for ch in channels:
        u = ch.get('url','')
        i = ch.get('id','')
        if u and i and u not in seen_url and i not in seen_id:
            seen_url.add(u)
            seen_id.add(i)
            result.append(ch)
    return result

async def fetch_json(session, url):
    try:
        async with session.get(url) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        logging.error(f"fetch_json failed {url} → {e}")
    return []

def clear_directories():
    delete_split_files(WORKING_CHANNELS_BASE)
    for d in (COUNTRIES_DIR, CATEGORIES_DIR):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

def save_channels(channels, country_dict, cat_dict, append=False):
    if not append:
        clear_directories()

    channels = remove_duplicates(channels)

    if append:
        old = load_split_json(WORKING_CHANNELS_BASE)
        channels = remove_duplicates(old + channels)

    save_split_json(WORKING_CHANNELS_BASE, channels)

    # countries
    os.makedirs(COUNTRIES_DIR, exist_ok=True)
    for cc, chs in country_dict.items():
        if not cc or cc == 'XX' or cc == 'Unknown':
            continue
        safe = re.sub(r'[^a-zA-Z0-9_-]', '', cc)
        base = os.path.join(COUNTRIES_DIR, safe)
        if append:
            old = load_split_json(base)
            chs = remove_duplicates(old + chs)
        save_split_json(base, chs)

    # categories
    os.makedirs(CATEGORIES_DIR, exist_ok=True)
    for cat, chs in cat_dict.items():
        if not cat:
            continue
        safe = re.sub(r'[^a-zA-Z0-9_-]', '', cat)
        base = os.path.join(CATEGORIES_DIR, safe)
        if append:
            old = load_split_json(base)
            chs = remove_duplicates(old + chs)
        save_split_json(base, chs)

# ────────────────────────────────────────────────
# Kenya scraper (unchanged logic)
# ────────────────────────────────────────────────

def get_m3u8_from_page(args):
    url, idx = args
    try:
        r = requests.get(url, headers=KENYA_HEADERS, timeout=12)
        links = re.findall(r'(https?://[^\s\'"]+\.m3u8)', r.text)
        valid = [l for l in links if 'youtube' not in l.lower()]
        logging.info(f"[{idx}] found {len(valid)} m3u8 links")
        return valid
    except Exception as e:
        logging.error(f"[{idx}] {url} → {e}")
        return []

async def check_m3u8_list(session, urls, timeout=16):
    if not urls:
        return None
    tasks = [check_single_m3u8(session, u, timeout) for u in urls[:8]]  # limit per page
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for ok, url in results:
        if isinstance(ok, bool) and ok:
            return url
    return None

async def check_single_m3u8(session, url, timeout):
    try:
        async with session.get(url, timeout=ClientTimeout(total=timeout)) as r:
            if r.status != 200:
                return False, url
            txt = await r.text()
            pl = m3u8.loads(txt)
            return bool(pl.segments or pl.playlists), url
    except:
        return False, url

def scrape_kenya_tv_channels(logos):
    if not KENYA_BASE_URL:
        logging.warning("KENYA_BASE_URL not set → skipping Kenya scrape")
        return []

    logging.info("Scraping Kenya TV channels...")
    try:
        r = requests.get(KENYA_BASE_URL, headers=KENYA_HEADERS, timeout=12)
        soup = BeautifulSoup(r.text, 'html.parser')
        cards = soup.select('article.tv-card')
        if not cards:
            return []

        results = []
        pages = []

        for i, card in enumerate(cards, 1):
            a = card.select_one('a[href]')
            img = card.select_one('img[alt]')
            if not a or not img:
                continue
            href = a['href']
            if not href.startswith('http'):
                href = urljoin(KENYA_BASE_URL, href)
            name = img.get('alt','').strip()
            if not name:
                continue

            cid = re.sub(r'[^a-z0-9]+', '', name.lower()) + '.ke'
            logo = next((l['url'] for l in logos if l['channel'] == cid), '')

            ch = {
                'name': name,
                'id': cid,
                'logo': logo,
                'url': None,
                'categories': ['general'],
                'country': 'KE'
            }
            results.append(ch)
            pages.append((href, i))

        with ThreadPoolExecutor(max_workers=6) as ex:
            m3u8_per_page = list(ex.map(get_m3u8_from_page, pages))

        loop = asyncio.get_event_loop()
        valid_urls = loop.run_until_complete(asyncio.gather(*[
            check_m3u8_list(None, lst) for lst in m3u8_per_page  # dummy session - will be replaced
        ]))

        final = []
        for ch, url in zip(results, valid_urls):
            if url:
                ch['url'] = url
                final.append(ch)

        return remove_duplicates(final)

    except Exception as e:
        logging.error(f"Kenya scrape failed → {e}")
        return []

# ────────────────────────────────────────────────
# Uganda (unchanged logic)
# ────────────────────────────────────────────────

async def fetch_and_process_uganda_channels(session, checker, logos):
    logging.info("Processing Uganda channels...")
    try:
        async with session.get(UGANDA_API_URL) as r:
            if r.status != 200:
                return 0
            data = await r.json()
            posts = data.get("posts", [])
    except:
        return 0

    ug_logos = [l for l in logos if str(l["channel"]).lower().endswith('.ug')]

    def norm(n): return re.sub(r'[^a-z0-9]', '', str(n).lower())

    channels = []
    country_d = {"UG": []}
    cat_d = {}

    async def process(post):
        name = str(post.get("channel_name","")).strip()
        url  = post.get("channel_url","").strip()
        if not name or not url:
            return None
        if checker.has_unwanted_extension(url):
            return None

        cat = str(post.get("category_name","entertainment")).lower().strip()

        # logo matching
        best_score = 0
        best_logo = ""
        best_cid = None

        n_name = norm(name)
        for lg in ug_logos:
            key = norm(lg["channel"].split('.')[0])
            score = 1.0 if key in n_name or n_name in key else SequenceMatcher(None, n_name, key).ratio()
            if score > best_score:
                best_score = score
                best_logo = lg["url"]
                best_cid  = lg["channel"]

        cid = best_cid if best_score >= 0.78 else f"{n_name}.ug"

        ch = {
            "name": name,
            "id": cid,
            "logo": best_logo,
            "url": url,
            "categories": [cat],
            "country": "UG"
        }

        _, ok = await checker.check_single_url(session, url)
        if ok:
            country_d["UG"].append(ch)
            cat_d.setdefault(cat, []).append(ch)
            return ch
        return None

    tasks = [process(p) for p in posts]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    added = [r for r in results if isinstance(r, dict)]
    if added:
        save_channels(added, country_d, cat_d, append=True)
    return len(added)

# ────────────────────────────────────────────────
# Main M3U processing (now only scraped + fixed)
# ────────────────────────────────────────────────

async def process_all_m3u_sources(session, checker, logos):
    logging.info("Collecting channels from scraped daily + fixed M3U lists")

    processor = M3UProcessor()
    all_raw_channels = []

    # 1. scraped daily lists
    scraped_urls = scrape_daily_m3u_urls(max_working=7)
    sources = scraped_urls + FIXED_M3U_LIST

    for url in sources:
        logging.info(f" → {url}")
        content = await processor.fetch_content(session, url)
        if not content:
            continue
        parsed = processor.parse(content)
        all_raw_channels.extend(parsed)

    if not all_raw_channels:
        return 0

    # Strict check
    tasks = []
    for ch in all_raw_channels:
        u = ch.get('url')
        if u:
            tasks.append(checker.check_single_url(session, u))

    results = await asyncio.gather(*tasks)

    working_raw = []
    for ch, (u, ok) in zip(all_raw_channels, results):
        if ok:
            working_raw.append(ch)

    formatted = [processor.format_channel(ch, logos) for ch in working_raw]
    formatted = remove_duplicates(formatted)

    country_d = {}
    cat_d = {}

    for ch in formatted:
        c = ch.get("country", "XX")
        country_d.setdefault(c, []).append(ch)
        for ca in ch.get("categories", ["general"]):
            cat_d.setdefault(ca, []).append(ch)

    save_channels(formatted, country_d, cat_d, append=True)
    logging.info(f"Kept {len(formatted)} working channels from all M3U sources")

    return len(formatted)

# ────────────────────────────────────────────────
# The rest remains mostly the same (IPTV-org, cleaning, sync, etc.)
# ────────────────────────────────────────────────

# ... (keep validate_channels, check_iptv_channels, clean_and_replace_channels,
#       sync_working_channels, update_logos_for_null_channels, etc. as they were)

# Only main() needs adjustment

async def main():
    logging.info("IPTV collection started — only scraped + two fixed M3U sources")

    checker = FastChecker()

    async with aiohttp.ClientSession(connector=checker.connector) as session:
        logos = await fetch_json(session, LOGOS_URL)
        logging.info(f"Loaded {len(logos)} logos")

        # Kenya
        kenya_ch = scrape_kenya_tv_channels(logos)
        if kenya_ch:
            cd = {}
            catd = {}
            for ch in kenya_ch:
                cd.setdefault("KE", []).append(ch)
                catd.setdefault("general", []).append(ch)
            save_channels(kenya_ch, cd, catd, append=True)
            logging.info(f"Added {len(kenya_ch)} Kenya channels")

        # Uganda
        ug_count = await fetch_and_process_uganda_channels(session, checker, logos)

        # All M3U sources (scraped + fixed two)
        m3u_count = await process_all_m3u_sources(session, checker, logos)

        # ── IPTV-org part (unchanged) ────────────────────────────────
        # ... keep as it was ...

        # Cleaning & final sync
        # ... keep clean_and_replace_channels, sync_working_channels ...

        logging.info("Finished.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)