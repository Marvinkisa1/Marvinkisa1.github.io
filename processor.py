import re
from typing import List, Dict
import aiohttp
from aiohttp import ClientTimeout

from config import SCRAPER_HEADERS, INITIAL_TIMEOUT
from utils import normalize_name


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
        
        country_code = ''
        clean_name = name
        match = re.match(r'^(?:\|([A-Z]{2})\|)|(?:([A-Z]{2})/?)', name)
        if match:
            country_code = match.group(1) or match.group(2).strip('/ ')
            clean_name = name[match.end():].strip()

        return {
            'display_name': clean_name,
            'raw_name': name,
            'group_title': attrs.get('group-title', ''),
            'tvg_id': attrs.get('tvg-ID', ''),
            'tvg_logo': attrs.get('tvg-logo', ''),
            'country_code': country_code,
            'url': ''
        }

    async def fetch_m3u_content(self, session: aiohttp.ClientSession, url: str) -> str | None:
        try:
            async with session.get(
                url,
                headers=SCRAPER_HEADERS,
                timeout=ClientTimeout(total=INITIAL_TIMEOUT * 2)
            ) as resp:
                if resp.status == 200:
                    return await resp.text()
                else:
                    return None
        except Exception:
            return None

    def _extract_categories(self, group_title: str) -> List[str]:
        if not group_title:
            return ['general']
        parts = [p.strip().lower() for p in group_title.split('/') if p.strip()]
        if len(parts) > 1 and re.match(r'^[a-z]{2}$', parts[0]):
            return parts[1:]
        return parts or ['general']

    def format_channel_data(self, channels: List[Dict], logos_data: List[Dict]) -> List[Dict]:
        formatted_channels = []
        
        for channel in channels:
            if channel.get('tvg_id'):
                channel_id = channel['tvg_id'].lower()
            else:
                base_id = re.sub(r'[^a-zA-Z0-9]', '', channel['display_name'])
                if not base_id:
                    base_id = re.sub(r'[^a-zA-Z0-9]', '', channel['raw_name'])
                country = channel.get('country_code', '')
                channel_id = f"{base_id}.{country.lower()}" if country else base_id.lower()

            logo_url = channel.get('tvg_logo', '')
            if not logo_url and logos_data:
                matching = [l for l in logos_data if l.get("channel") == channel_id]
                if matching:
                    logo_url = matching[0].get("url", "")

            formatted_channels.append({
                'name': channel['display_name'],
                'id': channel_id,
                'logo': logo_url,
                'url': channel['url'],
                'categories': self._extract_categories(channel.get('group_title', '')),
                'country': channel.get('country_code', 'Unknown')
            })
        
        return formatted_channels