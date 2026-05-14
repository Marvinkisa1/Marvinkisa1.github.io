import asyncio
from typing import Tuple, Optional
import aiohttp
from aiohttp import ClientTimeout, TCPConnector

from config import (MAX_CONCURRENT, INITIAL_TIMEOUT, MAX_TIMEOUT, RETRIES, MIN_STREAM_SIZE, UNWANTED_EXTENSIONS)
from utils import is_useless_stream


class FastChecker:
    def __init__(self):
        self.connector = TCPConnector(limit=MAX_CONCURRENT, force_close=True, ssl=False)
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        self.working_cache = {}
        self.failed_cache = {}
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    def has_unwanted_extension(self, url: str) -> bool:
        if not url:
            return True
        lower = url.lower().split('?')[0]
        return any(lower.endswith(ext) for ext in UNWANTED_EXTENSIONS)

    async def check_single_url(self, session: aiohttp.ClientSession, url: str) -> Tuple[str, bool, Optional[str]]:
        async with self.semaphore:  # Limits concurrent checks to MAX_CONCURRENT
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
                        return url, True, reason
                    if attempt == RETRIES:
                        self.failed_cache[url] = reason
                    await asyncio.sleep(0.5 * (attempt + 1))
                except Exception as e:
                    if attempt == RETRIES:
                        self.failed_cache[url] = str(e)
                    await asyncio.sleep(0.5 * (attempt + 1))
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
                    return False, "MyCamTV / Fake Stream"

                if len(chunk) < MIN_STREAM_SIZE and '#EXT' not in text:
                    return False, "Too small"

                if any(x in text for x in ['#EXTINF', '#EXT-X-STREAM-INF', '.ts']):
                    return True, "Valid Stream"
                return True, "OK"
        except asyncio.TimeoutError:
            return False, "Timeout"
        except Exception as e:
            return False, str(e)