import re
from typing import List, Dict
import aiohttp

from aiohttp import ClientTimeout

from config import SCRAPER_HEADERS, INITIAL_TIMEOUT


class M3UProcessor:
    def parse_m3u(self, content: str) -> List[Dict]:
        channels = []
        current = {}
        for line in content.splitlines():
            line = line.strip()
            if line.startswith('#EXTINF'):
                current = self._parse_extinf(line)
            elif line and not line.startswith('#') and current:
                current['url'] = line
                channels.append(current)
                current = {}
        return channels

    def _parse_extinf(self, line: str) -> Dict:
        attrs = dict(re.findall(r'(\S+)="([^"]*)"', line))
        name = line.split(',')[-1].strip()
        return {
            'display_name': name,
            'raw_name': name,
            'group_title': attrs.get('group-title', ''),
            'tvg_id': attrs.get('tvg-ID', ''),
            'tvg_logo': attrs.get('tvg-logo', ''),
            'url': ''
        }