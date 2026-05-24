import re
import time
import logging
import random
from urllib.parse import urljoin, urlparse
from collections import deque
from bs4 import BeautifulSoup

# pip install curl-cffi beautifulsoup4 lxml
from curl_cffi.requests import Session

logger = logging.getLogger(__name__)


class WorldIPTVScraper:
    def __init__(self, base_urls=None):
        if base_urls is None:
            base_urls = [
                "https://world-iptv.club/iptv/",
                "https://ninoiptv.com/home/"
            ]
        self.base_urls = base_urls
        self.current_base_url = None
        self.domain = None
        
        # Strong User-Agents
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        ]
        
        self.session = Session(
            impersonate="chrome",
            verify=False
        )
        
        self.session.headers.update({
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0",
            "Referer": "https://www.google.com/",
        })
        
        # Regex patterns
        self.m3u_pattern = re.compile(r'https?://[^\s"\'<>]+\.(?:m3u|m3u8)(?:\?[^\s"\'<>]*)?', re.IGNORECASE)
        self.xtream_pattern = re.compile(r'https?://[^\s"\'<>]+/get\.php\?[^"\']*', re.IGNORECASE)
        self.portal_pattern = re.compile(r'https?://[^\s"\'<>]+(?::\d+)?/c/[^"\']+', re.IGNORECASE)
        
        self.seen_domains = set()

    def get_domain_from_url(self, url):
        try:
            parsed = urlparse(url)
            return parsed.netloc.split(':')[0]
        except:
            return url

    def is_duplicate_domain(self, url):
        domain = self.get_domain_from_url(url)
        if domain in self.seen_domains:
            return True
        self.seen_domains.add(domain)
        return False

    def set_base_url(self, url):
        self.current_base_url = url
        self.domain = urlparse(url).netloc
        logger.info(f"Crawling: {url} (domain: {self.domain})")

    def get_page(self, url, timeout=20):
        """Improved page fetching with curl_cffi"""
        for attempt in range(4):
            try:
                if attempt > 0:
                    self.session.headers.update({"User-Agent": random.choice(self.user_agents)})
                    time.sleep(random.uniform(2.5, 5.5))
                
                response = self.session.get(url, timeout=timeout)
                
                if response.status_code == 403:
                    logger.warning(f"403 Forbidden on {url} (attempt {attempt+1})")
                    time.sleep(3 + attempt * 2)
                    continue
                    
                if response.status_code == 200:
                    return response.text
                
                logger.warning(f"Status {response.status_code} on {url}")
                
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/4 failed for {url}: {str(e)[:100]}")
                time.sleep(2 + attempt)
        
        return None

    def get_article_links(self, html, current_url):
        soup = BeautifulSoup(html, 'html.parser')
        article_links = set()
        
        for title_tag in soup.find_all(['h1', 'h2', 'h3'], class_=re.compile(r'title|entry-title|post-title', re.I)):
            link = title_tag.find('a', href=True)
            if link:
                full_url = urljoin(current_url, link['href'])
                if urlparse(full_url).netloc == self.domain:
                    article_links.add(full_url.split('#')[0])
        
        for a in soup.find_all('a', href=True):
            href = a['href'].lower()
            if any(x in href for x in ['/20', '-may-', '-apr-', '-mar-', '-jun-', '-jul-', '-aug-']):
                full_url = urljoin(current_url, a['href'])
                if urlparse(full_url).netloc == self.domain:
                    article_links.add(full_url.split('#')[0])
        
        return list(article_links)

    def get_internal_links(self, html, current_url):
        soup = BeautifulSoup(html, 'html.parser')
        internal = set()
        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            if not href or href.startswith(('#', 'javascript:')):
                continue
            full_url = urljoin(current_url, href)
            if urlparse(full_url).netloc == self.domain:
                internal.add(full_url.split('#')[0])
        return list(internal)

    def extract_target_links(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()
        links = set()

        for match in self.m3u_pattern.findall(text):
            links.add(match.split('#')[0] if '#' in match else match)
        for match in self.xtream_pattern.findall(text):
            links.add(match)
        for match in self.portal_pattern.findall(text):
            links.add(match)

        for tag in soup.find_all(['pre', 'code', 'p', 'div']):
            if tag.string:
                for match in self.m3u_pattern.findall(tag.string):
                    links.add(match.split('#')[0] if '#' in match else match)
                for match in self.xtream_pattern.findall(tag.string):
                    links.add(match)

        for a in soup.find_all('a', href=True):
            href = a['href']
            if any(x in href.lower() for x in ['.m3u', '.m3u8', 'get.php', '/c/']):
                if href.startswith('http'):
                    links.add(href.split('#')[0] if '#' in href else href)

        return list(links)

    def is_m3u_working(self, url, timeout=10):
        try:
            resp = self.session.get(url.split('#')[0].split('?')[0], timeout=timeout, stream=True)
            if resp.status_code != 200:
                return False, f"HTTP {resp.status_code}"
            content = resp.content[:2048].decode('utf-8', errors='ignore').lower()
            return '#extm3u' in content, "Working"
        except Exception as e:
            return False, f"Error: {str(e)[:60]}"

    def is_xtream_working(self, url, timeout=8):
        try:
            test_url = url
            if 'get.php' in url and 'type=' not in url:
                test_url += '&type=m3u_plus' if '?' in url else '?type=m3u_plus'
            resp = self.session.get(test_url, timeout=timeout)
            if resp.status_code == 200:
                content = resp.text.lower()
                if '#extm3u' in content or len(content) > 400:
                    return True, "Working"
            return False, f"HTTP {resp.status_code}"
        except Exception as e:
            return False, f"Failed: {str(e)[:60]}"

    def crawl_site(self, links_per_site=1, max_working_total=6):
        all_found_working = []
        total_targets_checked = 0
        
        for base_url in self.base_urls:
            self.seen_domains = set()
            if len(all_found_working) >= max_working_total:
                break
                
            self.set_base_url(base_url)
            visited_pages = set()
            queue = deque([base_url])
            found_working_this_site = []
            
            while queue and len(found_working_this_site) < links_per_site and len(all_found_working) < max_working_total:
                current_url = queue.popleft()
                if current_url in visited_pages:
                    continue
                
                visited_pages.add(current_url)
                
                html = self.get_page(current_url)
                if not html:
                    continue

                # Extract article & internal links
                if current_url == base_url or '/page/' in current_url:
                    article_links = self.get_article_links(html, current_url)
                    for link in article_links[:15]:
                        if link not in visited_pages:
                            queue.append(link)

                internal_links = self.get_internal_links(html, current_url)
                for link in internal_links[:25]:
                    if link not in visited_pages:
                        queue.append(link)

                # Extract streaming links
                target_links = self.extract_target_links(html)
                
                for link in target_links:
                    if len(found_working_this_site) >= links_per_site or len(all_found_working) >= max_working_total:
                        break
                        
                    if self.is_duplicate_domain(link):
                        continue
                        
                    total_targets_checked += 1
                    link_lower = link.lower()
                    
                    if '.m3u' in link_lower or '.m3u8' in link_lower:
                        is_working, status = self.is_m3u_working(link)
                        if is_working:
                            entry = {"url": link, "type": "m3u", "status": status, 
                                     "source": base_url, "page": current_url}
                            found_working_this_site.append(entry)
                            all_found_working.append(entry)
                            logger.info(f"✅ WORKING M3U: {link[:90]}")
                            
                    elif 'get.php' in link_lower:
                        is_working, status = self.is_xtream_working(link)
                        if is_working:
                            entry = {"url": link, "type": "xtream", "status": status, 
                                     "source": base_url, "page": current_url}
                            found_working_this_site.append(entry)
                            all_found_working.append(entry)
                            logger.info(f"✅ WORKING XTREAM: {link[:90]}")
                    
                    time.sleep(random.uniform(0.8, 1.8))
            
            logger.info(f"Site '{base_url}' → {len(found_working_this_site)} working links")
        
        logger.info(f"World IPTV Scraper finished. Total working: {len(all_found_working)}")
        return all_found_working


def scrape_world_iptv_channels():
    logger.info("🌍 Starting World IPTV Scraper...")
    scraper = WorldIPTVScraper()
    working_links = scraper.crawl_site(links_per_site=1, max_working_total=8)
    urls = [link['url'] for link in working_links]
    logger.info(f"✅ World IPTV: Found {len(urls)} working URLs")
    return urls