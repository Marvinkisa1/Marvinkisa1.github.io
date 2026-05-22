import re
import time
import logging
import requests
from urllib.parse import urljoin, urlparse
from collections import deque
from bs4 import BeautifulSoup

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
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Regex patterns for target links
        self.m3u_pattern = re.compile(r'https?://[^\s"\'<>]+\.(?:m3u|m3u8)(?:\?[^\s"\'<>]*)?', re.IGNORECASE)
        self.xtream_pattern = re.compile(r'https?://[^\s"\'<>]+/get\.php\?[^"\']*', re.IGNORECASE)
        self.portal_pattern = re.compile(r'https?://[^\s"\'<>]+(?::\d+)?/c/[^"\']+', re.IGNORECASE)
        
        # Track unique domains to avoid duplicates
        self.seen_domains = set()

    def get_domain_from_url(self, url):
        """Extract domain from URL (IP or hostname) for deduplication."""
        try:
            parsed = urlparse(url)
            netloc = parsed.netloc
            domain = netloc.split(':')[0]
            return domain
        except:
            return url

    def is_duplicate_domain(self, url):
        """Check if we've already seen a URL from this domain."""
        domain = self.get_domain_from_url(url)
        if domain in self.seen_domains:
            return True
        self.seen_domains.add(domain)
        return False

    def set_base_url(self, url):
        """Set the current base URL to crawl."""
        self.current_base_url = url
        self.domain = urlparse(url).netloc
        logger.info(f"Crawling: {url} (domain: {self.domain})")

    def get_page(self, url, timeout=12):
        """Fetch page HTML, return None on failure."""
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)[:100]}")
            return None

    def get_article_links(self, html, current_url):
        """Extract links to individual blog posts/articles."""
        soup = BeautifulSoup(html, 'html.parser')
        article_links = set()
        
        # Look for post titles and links
        for title_tag in soup.find_all(['h1', 'h2', 'h3'], class_=re.compile(r'title|entry-title|post-title')):
            link = title_tag.find('a', href=True)
            if link:
                full_url = urljoin(current_url, link['href'])
                parsed = urlparse(full_url)
                if parsed.netloc == self.domain and parsed.scheme in ('http', 'https'):
                    article_links.add(full_url.split('#')[0])
        
        # Look for links that look like blog posts
        for a in soup.find_all('a', href=True):
            href = a['href']
            if any(x in href.lower() for x in ['/20', '-may-', '-apr-', '-mar-', '-jun-', '-jul-', '-aug-']):
                full_url = urljoin(current_url, href)
                parsed = urlparse(full_url)
                if parsed.netloc == self.domain and parsed.scheme in ('http', 'https'):
                    article_links.add(full_url.split('#')[0])
        
        return list(article_links)

    def get_internal_links(self, html, current_url):
        """Extract all internal (same domain) links from a page."""
        soup = BeautifulSoup(html, 'html.parser')
        internal = set()
        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            if not href or href.startswith('#') or href.startswith('javascript:'):
                continue
            full_url = urljoin(current_url, href)
            parsed = urlparse(full_url)
            if parsed.netloc == self.domain and parsed.scheme in ('http', 'https'):
                clean = full_url.split('#')[0]
                internal.add(clean)
        return list(internal)

    def extract_target_links(self, html):
        """Extract M3U/M3U8, Xtream, and portal links from page content."""
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()
        links = set()

        # Find in text
        for match in self.m3u_pattern.findall(text):
            match_clean = match.split('#')[0] if '#' in match else match
            links.add(match_clean)
        for match in self.xtream_pattern.findall(text):
            links.add(match)
        for match in self.portal_pattern.findall(text):
            links.add(match)

        # Check in code blocks and pre tags
        for code_tag in soup.find_all(['pre', 'code', 'p', 'div']):
            if code_tag.string:
                for match in self.m3u_pattern.findall(code_tag.string):
                    links.add(match.split('#')[0] if '#' in match else match)
                for match in self.xtream_pattern.findall(code_tag.string):
                    links.add(match)
        
        # Check regular links
        for a in soup.find_all('a', href=True):
            href = a['href']
            if any(x in href.lower() for x in ['.m3u', '.m3u8', 'get.php', '/c/']):
                if href.startswith('http'):
                    href_clean = href.split('#')[0] if '#' in href else href
                    links.add(href_clean)

        return list(links)

    def is_m3u_working(self, url, timeout=10):
        """Check if M3U/M3U8 playlist is live."""
        try:
            url = url.split('#')[0].split('?')[0]
            resp = self.session.get(url, timeout=timeout, stream=True)
            if resp.status_code != 200:
                return False, f"HTTP {resp.status_code}"
            content = resp.raw.read(2048).decode('utf-8', errors='ignore').lower()
            if '#extm3u' in content:
                return True, "Working (valid playlist)"
            return True, "OK (200) but may not be valid playlist"
        except Exception as e:
            return False, f"Error: {str(e)[:60]}"

    def is_xtream_working(self, url, timeout=8):
        """Validate Xtream Code / get.php link."""
        try:
            test_url = url
            if 'get.php' in url and 'type=' not in url:
                if '?' in url:
                    test_url += '&type=m3u_plus'
                else:
                    test_url += '?type=m3u_plus'
            resp = self.session.get(test_url, timeout=timeout)
            if resp.status_code == 200:
                content = resp.text.lower()
                if '#extm3u' in content or 'username' in content or len(content) > 500:
                    return True, "Working (playlist returned)"
                return True, "OK (200 response)"
            return False, f"HTTP {resp.status_code}"
        except Exception as e:
            return False, f"Failed: {str(e)[:60]}"

    def crawl_site(self, links_per_site=3, max_working_total=6):
        """Crawl each site and get specified number of unique working links."""
        all_found_working = []
        total_targets_checked = 0
        pages_crawled_total = 0
        
        for base_url in self.base_urls:
            # Reset seen domains for each site
            self.seen_domains = set()
            
            if len(all_found_working) >= max_working_total:
                break
                
            self.set_base_url(base_url)
            visited_pages = set()
            queue = deque([base_url])
            pages_crawled = 0
            found_working_this_site = []
            
            needed_from_site = links_per_site
            logger.info(f"Starting deep crawl of {self.domain}...")
            logger.info(f"Target: Find {needed_from_site} working links from this site")
            
            while queue and len(found_working_this_site) < needed_from_site and len(all_found_working) < max_working_total:
                current_url = queue.popleft()
                if current_url in visited_pages:
                    continue
                
                visited_pages.add(current_url)
                pages_crawled += 1
                pages_crawled_total += 1
                logger.debug(f"[{pages_crawled}] Crawling: {current_url[:90]}...")
                
                html = self.get_page(current_url)
                if not html:
                    logger.debug("Failed to fetch")
                    continue
                
                # For main listing pages, extract article links first
                if current_url == base_url or '/page/' in current_url:
                    article_links = self.get_article_links(html, current_url)
                    if article_links:
                        logger.debug(f"Found {len(article_links)} article/post links")
                        for link in article_links:
                            if link not in visited_pages:
                                queue.append(link)
                
                # Add regular internal links
                internal_links = self.get_internal_links(html, current_url)
                new_links = [link for link in internal_links if link not in visited_pages]
                if new_links and current_url == base_url:
                    queue.extend(new_links[:30])
                    logger.debug(f"Found {len(new_links[:30])} additional internal links")
                
                # Extract M3U / Xtream links from this page
                target_links = self.extract_target_links(html)
                
                if target_links:
                    logger.debug(f"Found {len(target_links)} potential streaming links")
                    
                    for i, link in enumerate(target_links, 1):
                        if len(found_working_this_site) >= needed_from_site or len(all_found_working) >= max_working_total:
                            break
                        
                        # Check for duplicate domain
                        if self.is_duplicate_domain(link):
                            domain = self.get_domain_from_url(link)
                            logger.debug(f"[{i}/{len(target_links)}] Skipping duplicate domain: {domain}")
                            continue
                        
                        link_lower = link.lower()
                        total_targets_checked += 1
                        
                        logger.debug(f"[{i}/{len(target_links)}] Testing: {link[:80]}...")
                        
                        if '.m3u' in link_lower or '.m3u8' in link_lower:
                            is_working, status = self.is_m3u_working(link)
                            if is_working:
                                entry = {
                                    "url": link, 
                                    "type": "m3u", 
                                    "status": status, 
                                    "source": base_url, 
                                    "page": current_url,
                                    "domain": self.get_domain_from_url(link)
                                }
                                found_working_this_site.append(entry)
                                all_found_working.append(entry)
                                logger.info(f"✅ WORKING M3U! ({len(found_working_this_site)}/{needed_from_site} from this site, {len(all_found_working)}/{max_working_total} total)")
                            else:
                                logger.debug(f"Dead: {status}")
                        
                        elif 'get.php' in link_lower or any(x in link_lower for x in ['username=', 'password=']):
                            is_working, status = self.is_xtream_working(link)
                            if is_working:
                                entry = {
                                    "url": link, 
                                    "type": "xtream", 
                                    "status": status, 
                                    "source": base_url, 
                                    "page": current_url,
                                    "domain": self.get_domain_from_url(link)
                                }
                                found_working_this_site.append(entry)
                                all_found_working.append(entry)
                                logger.info(f"✅ WORKING XTREAM! ({len(found_working_this_site)}/{needed_from_site} from this site, {len(all_found_working)}/{max_working_total} total)")
                            else:
                                logger.debug(f"Dead: {status}")
                        else:
                            logger.debug("Not a streaming link")
                        
                        time.sleep(0.3)
                else:
                    logger.debug("No streaming links on this page")
                
                time.sleep(0.5)
            
            # Site summary
            if found_working_this_site:
                logger.info(f"Site '{base_url}' summary:")
                logger.info(f"  Pages crawled: {pages_crawled}")
                logger.info(f"  Working links found: {len(found_working_this_site)}/{needed_from_site}")
                logger.info(f"  Unique domains: {', '.join([item['domain'] for item in found_working_this_site])}")
            else:
                logger.warning(f"No working links found on {base_url}")
        
        # Final summary
        logger.info(f"Total pages crawled: {pages_crawled_total}")
        logger.info(f"Total links tested: {total_targets_checked}")
        logger.info(f"Total working links found: {len(all_found_working)}/{max_working_total}")
        
        if all_found_working:
            logger.info("Working links by source:")
            sources = {}
            for item in all_found_working:
                source = item['source']
                if source not in sources:
                    sources[source] = []
                sources[source].append(item)
            
            for source, links in sources.items():
                logger.info(f"  {source}: {len(links)} links")
                for link in links:
                    logger.info(f"    [{link['type'].upper()}] {link['domain']} - {link['url'][:80]}")
        
        return all_found_working


def scrape_world_iptv_channels():
    """
    Run the World IPTV scraper and return working URLs.
    
    Returns:
        list: List of working M3U/Xtream URLs as strings
        Example: ["http://example.com/playlist.m3u", "http://server.com/get.php?..."]
    """
    logger.info("🌍 Starting World IPTV Scraper...")
    
    scraper = WorldIPTVScraper()
    
    # Get working links
    working_links = scraper.crawl_site(
        links_per_site=3,  # Get 3 working links from each site
        max_working_total=6  # Maximum 6 total working links
    )
    
    # Extract just the URLs
    urls = [link['url'] for link in working_links]
    
    logger.info(f"✅ World IPTV: Found {len(urls)} working URLs")
    return urls