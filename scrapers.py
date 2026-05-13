import asyncio
import time
import requests
import logging
import re
import html
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
from difflib import SequenceMatcher

import aiohttp

from config import (
    KENYA_BASE_URL, UGANDA_API_URL, SCRAPER_HEADERS, 
    KENYA_HEADERS, UNWANTED_EXTENSIONS
)
from utils import remove_duplicates, normalize_name
from storage import save_channels
from checker import FastChecker


# ====================== Helper Functions for Kenya Scraper ======================
def get_m3u8_from_page(url_data: Tuple[str, int]) -> List[str]:
    url, index = url_data
    try:
        response = requests.get(url, headers=KENYA_HEADERS, timeout=10)
        m3u8_pattern = r'(https?://[^\s\'"]+\.m3u8)'
        m3u8_links = re.findall(m3u8_pattern, response.text)
        valid_m3u8_links = [link for link in m3u8_links if 'youtube' not in link.lower()]
        return valid_m3u8_links
    except Exception as e:
        # logging.error(f"Error processing {url}: {e}")  # avoid circular import
        return []


async def check_single_m3u8_url(session: aiohttp.ClientSession, url: str, timeout: int = 15) -> Tuple[str, bool]:
    if any(url.lower().endswith(ext) for ext in UNWANTED_EXTENSIONS):
        return url, False
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status == 200:
                content = await response.text()
                if '#EXTM3U' in content or '#EXTINF' in content:
                    return url, True
    except Exception:
        pass
    return url, False


async def check_m3u8_urls(urls: List[str]) -> Optional[str]:
    async with aiohttp.ClientSession() as session:
        tasks = [check_single_m3u8_url(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        for url, is_valid in results:
            if is_valid:
                return url
        return None
# ============================================================================


async def scrape_kenya_tv_channels(logos_data: List[Dict]) -> List[Dict]:
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
            matching_logos = [l for l in logos_data if l.get("channel") == channel_id]
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
    
    ug_logos = [l for l in logos_data if str(l.get("channel", "")).lower().endswith('.ug')]
    channels = []
    country_files = {"UG": []}
    category_files = {}
    
    async def process_post(post: Dict) -> Optional[Dict]:
        name = str(post.get("channel_name", "")).strip()
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
            logo_channel = logo_data.get("channel", "")
            norm_key = normalize(logo_channel.split('.')[0])
            score = SequenceMatcher(None, norm_inp, norm_key).ratio()
            if score > best_score:
                best_score = score
                if best_score >= 0.8:
                    logo = logo_data.get("url", "")
                    ch_id = logo_data.get('channel')
        
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
        
        checked_url, is_working, reason = await checker.check_single_url(session, url)
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