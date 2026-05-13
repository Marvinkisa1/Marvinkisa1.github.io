import os
import json
import shutil
from typing import List, Dict

from config import (
    WORKING_CHANNELS_BASE, 
    COUNTRIES_DIR, 
    CATEGORIES_DIR, 
    MAX_CHANNELS_PER_FILE, 
    CATEGORIES_SUMMARY_FILE
)
from utils import remove_duplicates, add_channel_type


def delete_split_files(base_name: str):
    """Delete all split files for a base name"""
    ext = '.json'
    if os.path.exists(base_name + ext):
        os.remove(base_name + ext)
    i = 1
    while True:
        part_file = f"{base_name}{i}{ext}"
        if not os.path.exists(part_file):
            break
        os.remove(part_file)
        i += 1


def load_split_json(base_name: str) -> List[Dict]:
    """Load data from split JSON files"""
    data = []
    ext = '.json'
    
    # Main file
    if os.path.exists(base_name + ext):
        try:
            with open(base_name + ext, 'r', encoding='utf-8') as f:
                data.extend(json.load(f))
        except Exception:
            pass
    
    # Split parts
    i = 1
    while True:
        part_file = f"{base_name}{i}{ext}"
        if not os.path.exists(part_file):
            break
        try:
            with open(part_file, 'r', encoding='utf-8') as f:
                data.extend(json.load(f))
        except Exception:
            pass
        i += 1
    return data


def save_split_json(base_name: str, data: List[Dict]):
    """Save data with splitting if too large"""
    if not data:
        return
    delete_split_files(base_name)
    
    if len(data) <= MAX_CHANNELS_PER_FILE:
        with open(base_name + '.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    else:
        for i in range(0, len(data), MAX_CHANNELS_PER_FILE):
            chunk = data[i:i + MAX_CHANNELS_PER_FILE]
            with open(f"{base_name}{i//MAX_CHANNELS_PER_FILE + 1}.json", 'w', encoding='utf-8') as f:
                json.dump(chunk, f, indent=4, ensure_ascii=False)


def generate_categories_summary(channels: List[Dict]):
    """Generate categories.json summary"""
    count = {}
    for ch in channels:
        cats = ch.get("categories", ["general"])
        for cat in cats:
            key = (cat or "general").strip().lower()
            count[key] = count.get(key, 0) + 1

    summary = [
        {"category": k.capitalize(), "count": v}
        for k, v in sorted(count.items(), key=lambda x: x[1], reverse=True)
    ]

    with open(CATEGORIES_SUMMARY_FILE, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=4, ensure_ascii=False)


def save_channels(channels: List[Dict], country_files: Dict = None, category_files: Dict = None, append: bool = False):
    """Main function to save channels to working + countries + categories"""
    if country_files is None:
        country_files = {}
    if category_files is None:
        category_files = {}

    if not append:
        # Clear directories
        if os.path.exists(COUNTRIES_DIR):
            shutil.rmtree(COUNTRIES_DIR)
        if os.path.exists(CATEGORIES_DIR):
            shutil.rmtree(CATEGORIES_DIR)
    
    os.makedirs(COUNTRIES_DIR, exist_ok=True)
    os.makedirs(CATEGORIES_DIR, exist_ok=True)

    channels = remove_duplicates(channels)
    channels = [add_channel_type(ch) for ch in channels]

    # Save to working_channels
    if append:
        existing = load_split_json(WORKING_CHANNELS_BASE)
        existing = [add_channel_type(ch) for ch in existing]
        existing.extend(channels)
        channels = remove_duplicates(existing)
    
    save_split_json(WORKING_CHANNELS_BASE, channels)

    # Save by country
    for country, ch_list in country_files.items():
        if not country or country == "Unknown":
            continue
        safe_name = "".join(c for c in country if c.isalnum() or c in (' ', '_', '-')).strip()
        if not safe_name:
            continue
        base_path = os.path.join(COUNTRIES_DIR, safe_name)
        save_split_json(base_path, [add_channel_type(ch) for ch in remove_duplicates(ch_list)])

    # Save by category
    for category, ch_list in category_files.items():
        if not category:
            continue
        safe_name = "".join(c for c in category if c.isalnum() or c in (' ', '_', '-')).strip()
        if not safe_name:
            continue
        base_path = os.path.join(CATEGORIES_DIR, safe_name)
        save_split_json(base_path, [add_channel_type(ch) for ch in remove_duplicates(ch_list)])

    # Generate summary
    all_channels = load_split_json(WORKING_CHANNELS_BASE)
    generate_categories_summary(all_channels)