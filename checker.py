import asyncio
import json
import os
import logging
from typing import Tuple, Optional
import aiohttp
from aiohttp import ClientTimeout, TCPConnector

from config import (MAX_CONCURRENT, INITIAL_TIMEOUT, MAX_TIMEOUT, RETRIES, MIN_STREAM_SIZE, UNWANTED_EXTENSIONS)
from utils import is_useless_stream

logger = logging.getLogger(__name__)
CACHE_FILE = "working_cache.json"


class FastChecker:
    def __init__(self):
        self.connector = TCPConnector(
            limit=MAX_CONCURRENT,
            force_close=True,      # Keep this for faster resource cleanup
            ssl=False,
            ttl_dns_cache=600,
            # keepalive_timeout removed - conflicts with force_close=True
        )
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        self.working_cache = {}
        self.failed_cache = {}
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.load_cache()

    def load_cache(self):
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.working_cache = data.get('working', {})
                logger.info(f"✅ Loaded {len(self.working_cache)} cached working URLs")
            except Exception as e:
                logger.warning(f"Cache load failed: {e}")

    def save_cache(self):
        try:
            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump({'working': dict(list(self.working_cache.items())[-40000:])}, f)
        except Exception as e:
            logger.warning(f"Cache save failed: {e}")

    def has_unwanted_extension(self, url: str) -> bool:
        if not url:
            return True
        lower = url.lower().split('?')[0]
        return any(lower.endswith(ext) for ext in UNWANTED_EXTENSIONS)

    async def check_single_url(self, session: aiohttp.ClientSession, url: str) -> Tuple[str, bool, Optional[str]]:
        async with self.semaphore:
            if url in self.working_cache:
                return url, True, "Cached"
            if url in self.failed_cache:
                return url, False, self.failed_cache[url]

            for attempt in range(RETRIES + 1):
                try:
                    timeout = min(INITIAL_TIMEOUT * (attempt + 1), MAX_TIMEOUT)
                    is_working, reason = await self._check_directly(session, url, timeout)
                    if is_working:
                        self.working_cache[url] = True
                        if len(self.working_cache) % 1000 == 0:
                            self.save_cache()
                        return url, True, reason
                    if attempt == RETRIES:
                        self.failed_cache[url] = reason
                    await asyncio.sleep(0.2 * (attempt + 1))
                except Exception as e:
                    if attempt == RETRIES:
                        self.failed_cache[url] = str(e)[:80]
                    await asyncio.sleep(0.2 * (attempt + 1))
            return url, False, self.failed_cache.get(url)

    async def _check_directly(self, session: aiohttp.ClientSession, url: str, timeout: int):
        try:
            async with session.get(url, timeout=ClientTimeout(total=timeout),
                                 headers=self.headers, allow_redirects=True) as resp:
                if resp.status not in [200, 206, 302, 307]:
                    return False, f"HTTP {resp.status}"

                chunk = await resp.content.read(65536)
                text = chunk.decode('utf-8', errors='ignore')

                if is_useless_stream(text):
                    return False, "Fake Stream"
                if len(chunk) < MIN_STREAM_SIZE and '#EXT' not in text:
                    return False, "Too small"
                if any(x in text for x in ['#EXTINF', '#EXT-X-STREAM-INF', '.ts']):
                    return True, "Valid"
                return True, "OK"
        except asyncio.TimeoutError:
            return False, "Timeout"
        except Exception as e:
            return False, str(e)[:80]

    def get_cache_stats(self):
        return {
            'working_cached': len(self.working_cache),
            'failed_cached': len(self.failed_cache)
        }

    def clear_cache(self):
        self.save_cache()
        self.working_cache.clear()
        self.failed_cache.clear()