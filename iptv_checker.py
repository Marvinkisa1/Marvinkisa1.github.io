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
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ────────────────────────────────────────────────
# Logging & Globals
# ────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CHANNELS_URL = os.getenv("CHANNELS_URL", "https://iptv-org.github.io/api/channels.json")
STREAMS_URL  = os.getenv("STREAMS_URL",  "https://iptv-org.github.io/api/streams.json")
LOGOS_URL    = os.getenv("LOGOS_URL",    "https://iptv-org.github.io/api/logos.json")
KENYA_BASE_URL = os.getenv("KENYA_BASE_URL", "")
UGANDA_API_URL = "https://apps.moochatplus.net/bash/api/api.php?get_posts&page=1&count=100&api_key=cda11bx8aITlKsXCpNB7yVLnOdEGqg342ZFrQzJRetkSoUMi9w"

M3U_URLS = [
    os.getenv("M3U_URL_1", ""),
    os.getenv("M3U_URL_2", "")
]

ADDITIONAL_M3U = [
    "https://raw.githubusercontent.com/ipstreet312/freeiptv/refs/heads/master/all.m3u",
    "https://raw.githubusercontent.com/abusaeeidx/IPTV-Scraper-Zilla/refs/heads/main/combined-playlist.m3u"
]

WORKING_CHANNELS_BASE = "working_channels"
CATEGORIES_DIR = "categories"
COUNTRIES_DIR = "countries"

MAX_CONCURRENT    = int(os.getenv("MAX_CONCURRENT", 100))
INITIAL_TIMEOUT   = 20
MAX_TIMEOUT       = 30
RETRIES           = 2
BATCH_DELAY       = 0.1
USE_HEAD_METHOD   = False           # Final setting — HEAD often unreliable
KENYA_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
MAX_CHANNELS_PER_FILE = 4000

UNWANTED_EXTENSIONS = ['.mkv', '.mp4', '.avi', '.mov', '.flv', '.wmv']

SCRAPER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

BASE_URL = "https://world-iptv.club"
CATEGORY_URL = f"{BASE_URL}/category/iptv/"
M3U_KEYWORDS = (".m3u", ".m3u8", "m3u", "get.php", "playlist", "iptv", "username=", "password=")
DAYS_BACK = 3

# ────────────────────────────────────────────────
# File / Directory Helpers
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
    if not data: return
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

def create_session():
    session = requests.Session()
    session.headers.update(SCRAPER_HEADERS)
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429,500,502,503,504], allowed_methods=["GET","HEAD"])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# ────────────────────────────────────────────────
# Daily M3U scraper (unchanged logic)
# ────────────────────────────────────────────────

def extract_post_datetime(html_text):
    soup = BeautifulSoup(html_text, "lxml")
    meta = soup.find("meta", property="article:published_time") or soup.find("meta", property="article:modified_time")
    if not meta or not meta.get("content"): return None
    try:
        return datetime.fromisoformat(meta["content"])
    except ValueError:
        return None

def is_recent(post_dt, days=DAYS_BACK):
    if not post_dt: return False
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return post_dt >= cutoff

def extract_candidate_m3u_links(page_url, html_text):
    soup = BeautifulSoup(html_text, "lxml")
    found = set()
    for a in soup.select("a[href]"):
        href = html.unescape(a["href"])
        if any(k in href.lower() for k in M3U_KEYWORDS):
            found.add(urljoin(page_url, href))
    for text in soup.stripped_strings:
        if text.startswith("http") and any(k in text.lower() for k in M3U_KEYWORDS):
            found.add(text)
    return list(found)

def is_working_m3u(session, url):
    try:
        head = session.head(url, allow_redirects=True, timeout=10)
        if head.status_code != 200: return False
        get = session.get(url, stream=True, timeout=15)
        chunk = next(get.iter_content(chunk_size=2048), b"")
        if b"#EXT" in chunk: return True
        ctype = get.headers.get("content-type", "").lower()
        return "mpegurl" in ctype or "m3u" in ctype
    except Exception:
        return False

def scrape_daily_m3u_urls(max_working=5):
    logging.info("Starting daily M3U scraper")
    session = create_session()
    working_m3u = []
    page = 1
    stop = False
    while not stop:
        page_url = CATEGORY_URL if page == 1 else f"{CATEGORY_URL}page/{page}/"
        try:
            resp = session.get(page_url, timeout=30)
            if resp.status_code != 200: break
        except Exception:
            break
        soup = BeautifulSoup(resp.text, "lxml")
        post_links = [urljoin(BASE_URL, a["href"]) for a in soup.select("a[href]") if "/iptv" in a["href"]]
        if not post_links: break
        for post_url in post_links:
            try:
                post_resp = session.get(post_url, timeout=30)
                if post_resp.status_code != 200: continue
            except Exception:
                continue
            post_dt = extract_post_datetime(post_resp.text)
            if post_dt and not is_recent(post_dt):
                stop = True
                break
            if not post_dt: continue
            candidates = extract_candidate_m3u_links(post_url, post_resp.text)
            for m3u in candidates:
                if m3u in working_m3u: continue
                if is_working_m3u(session, m3u):
                    working_m3u.append(m3u)
                if len(working_m3u) >= max_working:
                    return working_m3u
        page += 1
    return working_m3u

# ────────────────────────────────────────────────
# FastChecker with CACHE – single point of truth for checks
# ────────────────────────────────────────────────

class FastChecker:
    def __init__(self):
        self.connector = TCPConnector(limit=MAX_CONCURRENT, force_close=True, enable_cleanup_closed=True, ttl_dns_cache=300)
        self.timeout = ClientTimeout(total=INITIAL_TIMEOUT)
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.result_cache = {}  # url (normalized) → bool

    def has_unwanted_extension(self, url):
        return bool(url) and any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS)

    def is_html_response(self, text):
        markers = ("<html", "<!doctype", "<body", "<script", "<head")
        return any(m in text for m in markers)

    def is_valid_m3u8(self, text):
        if "#EXTM3U" not in text: return False
        for line in text.splitlines():
            line = line.strip()
            if line and not line.startswith("#"): return True
        return False

    def extract_first_segment(self, text):
        for line in text.splitlines():
            line = line.strip()
            if line and not line.startswith("#"): return line
        return None

    async def probe_segment(self, session, playlist_url, segment):
        seg_url = urljoin(playlist_url, segment)
        try:
            async with session.get(seg_url, timeout=ClientTimeout(total=6), allow_redirects=True) as r:
                return r.status == 200
        except Exception:
            return False

    async def check_single_url(self, session, url):
        url = url.strip()
        if url in self.result_cache:
            return url, self.result_cache[url]

        if not url or self.has_unwanted_extension(url):
            self.result_cache[url] = False
            return url, False

        async with self.semaphore:
            try:
                async with session.get(url, timeout=self.timeout, allow_redirects=True) as resp:
                    if resp.status != 200:
                        self.result_cache[url] = False
                        return url, False

                    ct = resp.headers.get("Content-Type", "").lower()
                    raw = await resp.content.read(8192)
                    text = raw.decode("utf-8", errors="ignore").lower()

                    if self.is_html_response(text):
                        self.result_cache[url] = False
                        return url, False

                    if url.endswith(".m3u8") or "m3u8" in ct:
                        if not self.is_valid_m3u8(text):
                            self.result_cache[url] = False
                            return url, False
                        seg = self.extract_first_segment(text)
                        if seg and not await self.probe_segment(session, url, seg):
                            self.result_cache[url] = False
                            return url, False

                    self.result_cache[url] = True
                    return url, True
            except Exception:
                self.result_cache[url] = False
                return url, False

# ────────────────────────────────────────────────
# M3U Processor
# ────────────────────────────────────────────────

class M3UProcessor:
    def __init__(self):
        self.unwanted_extensions = UNWANTED_EXTENSIONS

    async def fetch_m3u_content(self, session, m3u_url):
        try:
            async with session.get(m3u_url, timeout=ClientTimeout(total=INITIAL_TIMEOUT)) as r:
                if r.status == 200:
                    return await r.text()
        except Exception:
            pass
        return None

    def parse_m3u(self, content):
        channels = []
        current = {}
        for line in content.split('\n'):
            line = line.strip()
            if line.startswith('#EXTINF:-1'):
                current = self._parse_extinf(line)
            elif line and not line.startswith('#') and current:
                if not any(line.lower().endswith(e) for e in self.unwanted_extensions):
                    current['url'] = line
                    channels.append(current)
                current = {}
        return channels

    def _parse_extinf(self, line):
        attrs = dict(re.findall(r'(\S+)="([^"]*)"', line))
        name = line.split(',')[-1].strip()
        cc = ''
        clean = name
        m = re.match(r'^(?:\|([A-Z]{2})\|)|(?:([A-Z]{2}/ ?))', name)
        if m:
            cc = m.group(1) or (m.group(2) or '').strip('/ ')
            clean = name[m.end():].strip()
        return {
            'tvg_id': attrs.get('tvg-ID',''),
            'tvg_name': attrs.get('tvg-name',''),
            'tvg_logo': attrs.get('tvg-logo',''),
            'group_title': attrs.get('group-title',''),
            'display_name': clean,
            'country_code': cc,
            'raw_name': name
        }

    def _extract_categories(self, gt):
        if not gt: return ['general']
        parts = [p.strip().lower() for p in gt.split('/') if p.strip()]
        return parts[1:] if len(parts)>1 and re.match(r'^[a-z]{2}$', parts[0]) else parts

    def format_channel_data(self, channels, logos):
        res = []
        for ch in channels:
            cid = ch['tvg_id'].lower() if ch['tvg_id'] else \
                  f"{re.sub(r'[^a-z0-9]','', (ch['display_name'] or ch['raw_name'])).lower()}.{ch['country_code'].lower()}" if ch['country_code'] else \
                  re.sub(r'[^a-z0-9]','', (ch['display_name'] or ch['raw_name'])).lower()

            logo = ch.get('tvg_logo','')
            if not logo:
                for l in logos:
                    if l["channel"] == cid:
                        logo = l["url"]
                        break

            res.append({
                'name': ch['display_name'],
                'id': cid,
                'logo': logo,
                'url': ch['url'],
                'categories': self._extract_categories(ch['group_title']),
                'country': ch['country_code'] or 'Unknown'
            })
        return res

def remove_duplicates(channels):
    seen_url = set()
    seen_id  = set()
    unique = []
    for ch in channels:
        u = ch.get("url")
        i = ch.get("id")
        if u and i and u not in seen_url and i not in seen_id:
            seen_url.add(u)
            seen_id.add(i)
            unique.append(ch)
    return unique

async def fetch_json(session, url):
    try:
        async with session.get(url, headers={"Accept-Encoding": "gzip"}) as r:
            r.raise_for_status()
            return json.loads(await r.text())
    except Exception:
        return []

def load_existing_data():
    d = {"working_channels": load_split_json(WORKING_CHANNELS_BASE), "countries": {}, "categories": {}, "all_existing_channels": []}
    d["all_existing_channels"].extend(d["working_channels"])

    for dirpath, attr in [(COUNTRIES_DIR, "countries"), (CATEGORIES_DIR, "categories")]:
        if os.path.exists(dirpath):
            for fn in os.listdir(dirpath):
                if fn.endswith(".json"):
                    name = fn[:-5]
                    chs = load_split_json(os.path.join(dirpath, name))
                    d[attr][name] = chs
                    d["all_existing_channels"].extend(chs)

    d["all_existing_channels"] = remove_duplicates(d["all_existing_channels"])
    return d

def clear_directories():
    delete_split_files(WORKING_CHANNELS_BASE)
    for d in [COUNTRIES_DIR, CATEGORIES_DIR]:
        if os.path.exists(d): shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

def save_channels(channels, country_files, category_files, append=False):
    if not append: clear_directories()
    channels = remove_duplicates(channels)
    if append:
        ex = load_split_json(WORKING_CHANNELS_BASE)
        ex.extend(channels)
        channels = remove_duplicates(ex)
    save_split_json(WORKING_CHANNELS_BASE, channels)

    os.makedirs(COUNTRIES_DIR, exist_ok=True)
    os.makedirs(CATEGORIES_DIR, exist_ok=True)

    for c, chs in country_files.items():
        if not c or c == "Unknown": continue
        safe = "".join(x for x in c if x.isalnum() or x in ' _-').rstrip()
        if not safe: continue
        base = os.path.join(COUNTRIES_DIR, safe)
        if append:
            ex = load_split_json(base)
            ex.extend(chs)
            chs = remove_duplicates(ex)
        save_split_json(base, chs)

    for cat, chs in category_files.items():
        if not cat: continue
        safe = "".join(x for x in cat if x.isalnum() or x in ' _-').rstrip()
        if not safe: continue
        base = os.path.join(CATEGORIES_DIR, safe)
        if append:
            ex = load_split_json(base)
            ex.extend(chs)
            chs = remove_duplicates(ex)
        save_split_json(base, chs)

def update_logos_for_null_channels(channels, logos_data):
    cnt = 0
    for ch in channels:
        logo = ch.get("logo","")
        if not logo or logo in ("null", "None", None):
            cid = ch.get("id")
            if cid:
                for lg in logos_data:
                    if lg["channel"] == cid:
                        ch["logo"] = lg["url"]
                        cnt += 1
                        break
    if cnt: logging.info(f"Updated {cnt} missing logos")
    return channels

# ────────────────────────────────────────────────
# Core validation / processing functions (outer semaphore removed)
# ────────────────────────────────────────────────

async def validate_channels(session, checker, all_channels, iptv_ids, logos):
    valid = []
    cf = {}
    catf = {}
    all_channels = update_logos_for_null_channels(all_channels, logos)

    async def task(ch):
        url = ch.get("url","").strip()
        if not url or checker.has_unwanted_extension(url): return
        _, ok = await checker.check_single_url(session, url)
        if ok:
            copy = ch.copy()
            country = copy.get("country", "Unknown")
            cats = copy.get("categories", [])
            valid.append(copy)
            cf.setdefault(country, []).append(copy)
            for c in cats: catf.setdefault(c, []).append(copy)

    total = len(all_channels)
    bs = 100
    for i in range(0, total, bs):
        batch = all_channels[i:i+bs]
        await asyncio.gather(*(task(ch) for ch in batch), return_exceptions=True)

    save_channels(valid, cf, catf, append=False)
    return len(valid)

async def check_iptv_channels(session, checker, channels_data, streams_dict, existing_urls, logos):
    new_count = 0
    new_ch = []
    cf = {}
    catf = {}

    to_check = [ch for ch in channels_data if ch.get("id") in streams_dict and streams_dict[ch["id"]].get("url") not in existing_urls]

    async def task(ch):
        nonlocal new_count
        stream = streams_dict[ch["id"]]
        url = stream.get("url","").strip()
        if not url or checker.has_unwanted_extension(url): return
        feed = stream.get("feed")
        logo = ""
        matches = [l for l in logos if l["channel"] == ch["id"] and l.get("feed") == feed]
        if matches:
            logo = matches[0]["url"]
        else:
            matches = [l for l in logos if l["channel"] == ch["id"]]
            if matches: logo = matches[0]["url"]

        _, ok = await checker.check_single_url(session, url)
        if ok:
            nonlocal new_ch, cf, catf
            entry = {
                "name": ch.get("name","Unknown"),
                "id": ch["id"],
                "logo": logo,
                "url": url,
                "categories": ch.get("categories",[]),
                "country": ch.get("country","Unknown")
            }
            new_ch.append(entry)
            cf.setdefault(entry["country"], []).append(entry)
            for c in entry["categories"]: catf.setdefault(c, []).append(entry)
            new_count += 1

    bs = 100
    total = len(to_check)
    for i in range(0, total, bs):
        batch = to_check[i:i+bs]
        await asyncio.gather(*(task(ch) for ch in batch), return_exceptions=True)

    if new_ch:
        save_channels(new_ch, cf, catf, append=True)
    return new_count

async def process_m3u_urls(session, logos, checker, m3u_urls):
    processor = M3UProcessor()
    all_new = []

    for url in m3u_urls:
        if not url: continue
        content = await processor.fetch_m3u_content(session, url)
        if not content: continue
        parsed = processor.parse_m3u(content)
        if not parsed: continue

        checks = [checker.check_single_url(session, ch['url']) for ch in parsed if ch.get('url')]
        results = await asyncio.gather(*checks, return_exceptions=True)

        working = []
        for i, res in enumerate(results):
            if isinstance(res, Exception): continue
            _, ok = res
            if ok: working.append(parsed[i])

        if working:
            formatted = processor.format_channel_data(working, logos)
            all_new.extend(formatted)

    if all_new:
        cf = {}
        catf = {}
        for ch in all_new:
            cf.setdefault(ch["country"], []).append(ch)
            for c in ch.get("categories", ["general"]):
                catf.setdefault(c, []).append(ch)
        save_channels(all_new, cf, catf, append=True)

    return len(all_new)

# ────────────────────────────────────────────────
# Kenya & Uganda scrapers (outer sem removed where applicable)
# ────────────────────────────────────────────────

def get_m3u8_from_page(url_data):
    url, idx = url_data
    try:
        r = requests.get(url, headers=KENYA_HEADERS, timeout=10)
        links = re.findall(r'(https?://[^\s\'"]+\.m3u8)', r.text)
        valid = [lnk for lnk in links if 'youtube' not in lnk.lower()]
        logging.info(f"[{idx}] Found {len(valid)} m3u8 links")
        return valid
    except Exception as e:
        logging.error(f"[{idx}] {e}")
        return []

async def check_single_m3u8_url(session, url, timeout=15):
    if any(url.lower().endswith(e) for e in UNWANTED_EXTENSIONS): return url, False
    try:
        async with session.get(url, timeout=ClientTimeout(total=timeout)) as r:
            if r.status != 200: return url, False
            txt = await r.text()
            pl = m3u8.loads(txt)
            if pl.segments or pl.playlists:
                return url, True
            return url, True  # keep even if empty – conservative
    except Exception:
        return url, False

async def check_m3u8_urls(urls):
    async with aiohttp.ClientSession() as s:
        tasks = [check_single_m3u8_url(s, u) for u in urls]
        results = await asyncio.gather(*tasks)
        for u, ok in results:
            if ok: return u
    return None

async def scrape_kenya_tv_channels(logos_data):
    if not KENYA_BASE_URL: return []
    start = time.time()
    logging.info("Scraping Kenya TV channels...")
    try:
        r = requests.get(KENYA_BASE_URL, headers=KENYA_HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        main = soup.find('main')
        if not main: return []
        section = main.find('section', class_='tv-grid-container')
        if not section: return []
        cards = section.find_all('article', class_='tv-card')
        results = []
        to_process = []
        for i, card in enumerate(cards, 1):
            imgc = card.find('div', class_='img-container')
            if not imgc: continue
            a = imgc.find('a')
            img = imgc.find('img')
            if not a or not img: continue
            href = a.get('href','')
            full = href if href.startswith('http') else KENYA_BASE_URL + href
            name = img.get('alt','').strip()
            if not name: continue
            cid = f"{re.sub(r'[^a-z0-9]','',name.lower())}.ke"
            logo = next((l["url"] for l in logos_data if l["channel"] == cid), "")
            ch = {"name":name, "id":cid, "logo":logo, "url":None, "categories":["general"], "country":"KE"}
            results.append(ch)
            to_process.append((full, i))

        with ThreadPoolExecutor(max_workers=5) as ex:
            m3u_lists = list(ex.map(get_m3u8_from_page, to_process))

        valid_urls = await asyncio.gather(*[check_m3u8_urls(lst) for lst in m3u_lists])

        final = []
        for ch, vu in zip(results, valid_urls):
            if vu:
                ch["url"] = vu
                final.append(ch)

        logging.info(f"Kenya: {len(final)} working channels ({time.time()-start:.1f}s)")
        return remove_duplicates(final)
    except Exception as e:
        logging.error(f"Kenya scrape failed: {e}")
        return []

async def fetch_and_process_uganda_channels(session, checker, logos_data):
    def norm(n): return re.sub(r'[^a-z0-9]','', n.lower())
    def score(a,b): return 1.0 if a in b or b in a else SequenceMatcher(None,a,b).ratio()

    logging.info("Fetching Uganda channels...")
    try:
        async with session.get(UGANDA_API_URL) as r:
            if r.status != 200: return 0
            data = await r.json()
        posts = data.get("posts", [])
        if not posts: return 0
    except Exception as e:
        logging.error(f"Uganda API error: {e}")
        return 0

    ug_logos = [l for l in logos_data if str(l["channel"]).lower().endswith('.ug')]
    added = 0
    channels = []
    cf = {"UG": []}
    catf = {}

    async def task(post):
        nonlocal added
        name = str(post.get("channel_name","")).strip()
        url  = post.get("channel_url","").strip()
        if not name or not url: return
        if checker.has_unwanted_extension(url): return

        cat = post.get("category_name","").lower().strip() or "entertainment"

        best_score = 0
        best_logo = ""
        best_cid = None
        n = norm(name)
        for lg in ug_logos:
            key = norm(lg["channel"].split('.')[0])
            sc = score(n, key)
            if sc > best_score:
                best_score = sc
                best_logo = lg["url"]
                best_cid  = lg["channel"]

        cid = best_cid if best_score >= 0.8 else f"{n}.ug"

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
            channels.append(ch)
            cf["UG"].append(ch)
            catf.setdefault(cat, []).append(ch)
            added += 1

    await asyncio.gather(*(task(p) for p in posts), return_exceptions=True)

    if channels:
        save_channels(channels, cf, catf, append=True)
        logging.info(f"Added {added} Uganda channels")
    return added

# ────────────────────────────────────────────────
# Cleaning & replacement (outer sem removed)
# ────────────────────────────────────────────────

async def clean_and_replace_channels(session, checker, all_channels, streams_dict, m3u_channels_raw, logos):
    logging.info("Cleaning & replacing non-working channels...")
    all_channels = update_logos_for_null_channels(all_channels, logos)

    valid = []
    replaced = 0
    removed = 0
    cf = {}
    catf = {}

    async def find_replacement(ch, streams, m3u_list, s, ck):
        cid = ch.get("id")
        name = ch.get("name","").lower()
        if cid in streams:
            u = streams[cid].get("url","").strip()
            if u and not ck.has_unwanted_extension(u):
                _, ok = await ck.check_single_url(s, u)
                if ok: return u
        for mc in m3u_list:
            if fuzz.ratio(name, mc.get("display_name","").lower()) > 80:
                u = mc.get("url","").strip()
                if u and not ck.has_unwanted_extension(u):
                    _, ok = await ck.check_single_url(s, u)
                    if ok: return u
        return None

    async def task(ch):
        nonlocal replaced, removed
        url = ch.get("url","").strip()
        name = ch.get("name","Unknown")
        cid = ch.get("id","no-id")
        if not url:
            removed += 1
            return
        if checker.has_unwanted_extension(url):
            removed += 1
            return

        _, ok = await checker.check_single_url(session, url)
        if ok:
            valid.append(ch)
            c = ch.get("country","Unknown")
            if c != "Unknown": cf.setdefault(c, []).append(ch)
            for cat in ch.get("categories",[]):
                if cat: catf.setdefault(cat, []).append(ch)
            return

        logging.info(f"Not working: {name} → looking for replacement")
        new_url = await find_replacement(ch, streams_dict, m3u_channels_raw, session, checker)
        if new_url:
            ch["url"] = new_url
            valid.append(ch)
            c = ch.get("country","Unknown")
            if c != "Unknown": cf.setdefault(c, []).append(ch)
            for cat in ch.get("categories",[]):
                if cat: catf.setdefault(cat, []).append(ch)
            replaced += 1
            logging.info(f"Replaced → {new_url}")
        else:
            removed += 1

    total = len(all_channels)
    bs = 80
    for i in range(0, total, bs):
        batch = all_channels[i:i+bs]
        await asyncio.gather(*(task(ch) for ch in batch), return_exceptions=True)

    save_channels(valid, cf, catf, append=False)
    logging.info(f"After clean: {len(valid)} kept | {removed} removed | {replaced} replaced")
    return len(valid), removed, replaced

def sync_working_channels():
    all_ch = []
    for d, name in [(COUNTRIES_DIR, "countries"), (CATEGORIES_DIR, "categories")]:
        if os.path.exists(d):
            for fn in os.listdir(d):
                if fn.endswith(".json"):
                    base = os.path.join(d, fn[:-5])
                    chs = load_split_json(base)
                    all_ch.extend(chs)
    all_ch = remove_duplicates(all_ch)
    save_split_json(WORKING_CHANNELS_BASE, all_ch)
    logging.info(f"Synced {len(all_ch)} channels to working_channels")

# ────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────

async def main():
    logging.info("IPTV collection started")

    logging.info("Step 0: Scraping recent M3U links")
    scraped = scrape_daily_m3u_urls(max_working=5)
    global M3U_URLS
    M3U_URLS = scraped + [u for u in ADDITIONAL_M3U if u]

    checker = FastChecker()

    async with aiohttp.ClientSession(connector=checker.connector) as session:
        logos = await fetch_json(session, LOGOS_URL)
        logging.info(f"Loaded {len(logos)} logos")

        logging.info("Step 1: Kenya channels")
        kenya = await scrape_kenya_tv_channels(logos)
        if kenya:
            cf = {"KE": kenya}
            catf = {}
            for ch in kenya:
                for c in ch.get("categories",["general"]):
                    catf.setdefault(c, []).append(ch)
            save_channels(kenya, cf, catf, append=True)
            logging.info(f"Added {len(kenya)} Kenya channels")

        logging.info("Step 1.5: Uganda channels")
        ug_count = await fetch_and_process_uganda_channels(session, checker, logos)

        logging.info("Step 2: M3U processing")
        m3u_count = await process_m3u_urls(session, logos, checker, M3U_URLS)

        logging.info("Step 3: IPTV-org channels")
        channels_data = []
        streams_dict = {}
        try:
            channels_data, streams_raw = await asyncio.gather(
                fetch_json(session, CHANNELS_URL),
                fetch_json(session, STREAMS_URL)
            )
            streams_dict = {s["channel"]: s for s in streams_raw if s.get("channel")}
        except Exception as e:
            logging.error(f"IPTV-org fetch error: {e}")

        existing = load_existing_data()
        all_existing = existing["all_existing_channels"]
        exist_urls = {ch["url"] for ch in all_existing if ch.get("url")}

        valid_old_count = await validate_channels(session, checker, all_existing, set(streams_dict), logos)
        new_iptv_count = await check_iptv_channels(session, checker, channels_data, streams_dict, exist_urls, logos)

        logging.info("Step 4: Sync")
        sync_working_channels()

        logging.info("Step 5: Final clean & replace")
        existing = load_existing_data()  # reload after additions
        all_existing = existing["all_existing_channels"]

        m3u_raw = []
        processor = M3UProcessor()
        for u in M3U_URLS:
            if not u: continue
            txt = await processor.fetch_m3u_content(session, u)
            if txt:
                m3u_raw.extend(processor.parse_m3u(txt))

        final_count, rem, rep = await clean_and_replace_channels(
            session, checker, all_existing, streams_dict, m3u_raw, logos
        )

        logging.info("Step 6: Final sync")
        sync_working_channels()

        logging.info("Done.")
        logging.info(f"Final working channels: {final_count}")
        logging.info(f"Removed: {rem} | Replaced: {rep}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Main failed: {e}", exc_info=True)
        sys.exit(1)