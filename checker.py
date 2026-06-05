import asyncio
from typing import Tuple, Optional
import aiohttp
from aiohttp import ClientTimeout, TCPConnector

from config import (MAX_CONCURRENT, INITIAL_TIMEOUT, MAX_TIMEOUT, RETRIES, MIN_STREAM_SIZE, UNWANTED_EXTENSIONS, URL_CHECK_TIMEOUT, DNS_CACHE_TTL)
from utils import is_useless_stream


class FastChecker:
    def __init__(self):
        # Optimized for GitHub Actions with higher limits
        self.connector = TCPConnector(
            limit=MAX_CONCURRENT * 2,  # Double the connection limit
            limit_per_host=50,  # Aggressive per-host limit
            force_close=True, 
            ssl=False,  # Skip SSL for speed
            ttl_dns_cache=DNS_CACHE_TTL,  # Cache DNS
            enable_cleanup_closed=True
        )
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
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
            # Quick cache check
            if url in self.working_cache:
                return url, True, "Cached"
            if url in self.failed_cache:
                return url, False, self.failed_cache[url]

            # Try with aggressive timeouts first
            for attempt in range(RETRIES + 1):
                try:
                    # Use shorter timeouts for speed, increasing on retry
                    timeout = min(URL_CHECK_TIMEOUT * (attempt + 1), MAX_TIMEOUT)
                    is_working, reason = await self._check_directly(session, url, timeout)
                    if is_working:
                        self.working_cache[url] = True
                        return url, True, reason
                    if attempt == RETRIES:
                        self.failed_cache[url] = reason
                    # Minimal delay between retries for speed
                    await asyncio.sleep(0.1 * (attempt + 1))
                except Exception as e:
                    if attempt == RETRIES:
                        self.failed_cache[url] = str(e)
                    await asyncio.sleep(0.1 * (attempt + 1))
            return url, False, self.failed_cache.get(url)

    async def _check_directly(self, session: aiohttp.ClientSession, url: str, timeout: int):
        try:
            async with session.get(
                url, 
                timeout=ClientTimeout(total=timeout),
                headers=self.headers, 
                allow_redirects=True
            ) as resp:
                # Accept more status codes
                if resp.status not in [200, 206, 302, 307]:
                    return False, f"HTTP {resp.status}"

                # Read smaller chunk for speed (reduced from 65536)
                chunk = await resp.content.read(32768)  # 32KB instead of 64KB
                text = chunk.decode('utf-8', errors='ignore')

                if is_useless_stream(text):
                    return False, "MyCamTV / Fake Stream"

                if len(chunk) < MIN_STREAM_SIZE and '#EXT' not in text:
                    return False, "Too small"

                # Quick check for valid stream markers
                if any(x in text for x in ['#EXTINF', '#EXT-X-STREAM-INF', '.ts', '#EXTM3U']):
                    return True, "Valid Stream"
                return True, "OK"
        except asyncio.TimeoutError:
            return False, "Timeout"
        except aiohttp.ClientError as e:
            return False, f"Client Error: {str(e)}"
        except Exception as e:
            return False, str(e)

    def clear_cache(self):
        """Clear caches to free memory"""
        self.working_cache.clear()
        self.failed_cache.clear()
    
    def get_cache_stats(self):  
        """Get cache statistics"""
        return {
            'working_cached': len(self.working_cache),
            'failed_cached': len(self.failed_cache)
        }