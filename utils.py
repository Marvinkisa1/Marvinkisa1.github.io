import re
import hashlib
from typing import List, Dict
from fuzzywuzzy import fuzz
from difflib import SequenceMatcher

from config import FAKE_HINTS, ADULT_KEYWORDS, UNWANTED_EXTENSIONS


def is_useless_stream(content: str) -> bool:
    """Check if the downloaded *content* contains fake hints.
       This function remains for the stream content check."""
    if not content:
        return False
    text = content.lower()
    return any(hint.lower() in text for hint in FAKE_HINTS)


# NEW: Function to block channels whose NAME contains fake or adult hints
def is_fake_name(name: str) -> bool:
    """Return True if the channel name/display_name contains any FAKE_HINTS or ADULT_KEYWORDS."""
    if not name:
        return False
    name_lower = name.lower()
    # Check fake hints first
    if any(hint.lower() in name_lower for hint in FAKE_HINTS):
        return True
    # # Also optionally check adult keywords directly in the name
    # if any(kw.lower() in name_lower for kw in ADULT_KEYWORDS):
    #     return True
    return False


def is_adult_channel(channel: Dict) -> bool:
    text = " ".join(str(channel.get(k, "")).lower() for k in ["name", "display_name", "group_title", "raw_name"])
    if channel.get("categories"):
        text += " " + " ".join(str(c).lower() for c in channel["categories"])
    return any(kw.lower() in text for kw in ADULT_KEYWORDS)


def add_channel_type(channel: Dict) -> Dict:
    ch = channel.copy()
    ch["type"] = "adult" if is_adult_channel(ch) else "all"
    return ch


def remove_duplicates(channels: List[Dict]) -> List[Dict]:
    seen = set()
    result = []
    for ch in channels:
        url = ch.get("url")
        if not url:
            continue
        key = hashlib.md5(url.lower().rstrip('/').encode()).hexdigest()
        if key not in seen:
            seen.add(key)
            result.append(ch)
    return result


def normalize_name(name: str) -> str:
    return re.sub(r'[^a-z0-9]', '', name.lower())