import os
import json
import shutil
import glob
from typing import List, Dict
from datetime import datetime

from config import (
    WORKING_CHANNELS_BASE, 
    COUNTRIES_DIR, 
    CATEGORIES_DIR, 
    MAX_CHANNELS_PER_FILE, 
    CATEGORIES_SUMMARY_FILE
)
from utils import remove_duplicates, add_channel_type


def delete_split_files(base_name: str):
    """Delete all split files for a base name (main + all numbered parts)."""
    # Delete any existing files matching the pattern
    pattern = base_name + '*.json'
    for f in glob.glob(pattern):
        os.remove(f)


def load_split_json(base_name: str) -> List[Dict]:
    """Load data from split JSON files"""
    data = []
    
    # Load all numbered files: working_channels1.json, working_channels2.json, ...
    pattern = base_name + '*[0-9]*.json'
    for part_file in sorted(glob.glob(pattern)):
        try:
            with open(part_file, 'r', encoding='utf-8') as f:
                data.extend(json.load(f))
        except Exception:
            pass
    
    # Also try main file if it exists (backward compatibility)
    main_file = base_name + '.json'
    if os.path.exists(main_file):
        try:
            with open(main_file, 'r', encoding='utf-8') as f:
                data.extend(json.load(f))
        except Exception:
            pass
    
    return data


def save_split_json(base_name: str, data: List[Dict]):
    """Save data with splitting — ALWAYS use numbered files (working_channels1.json, etc.)"""
    if not data:
        return
    
    delete_split_files(base_name)
    
    if len(data) <= MAX_CHANNELS_PER_FILE:
        # Even for small data, save as working_channels1.json for consistency
        with open(f"{base_name}1.json", 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    else:
        # Split into multiple numbered files
        for i in range(0, len(data), MAX_CHANNELS_PER_FILE):
            chunk = data[i:i + MAX_CHANNELS_PER_FILE]
            part_num = (i // MAX_CHANNELS_PER_FILE) + 1
            with open(f"{base_name}{part_num}.json", 'w', encoding='utf-8') as f:
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


def save_last_update_info(total_channels: int, retained_channels: int, new_channels: int, removed_channels: int):
    """Save update information"""
    update_info = {
        "last_update": datetime.now().isoformat(),
        "total_channels": total_channels,
        "retained_channels": retained_channels,
        "new_channels": new_channels,
        "removed_channels": removed_channels,
        "status": "success"
    }
    
    try:
        with open("last_update.json", 'w', encoding='utf-8') as f:
            json.dump(update_info, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"⚠️ Could not save update info: {e}")


def save_channels(channels: List[Dict], country_files: Dict = None, category_files: Dict = None, append: bool = False):
    """Main function to save channels"""
    if country_files is None:
        country_files = {}
    if category_files is None:
        category_files = {}

    if not append:
        if os.path.exists(COUNTRIES_DIR):
            shutil.rmtree(COUNTRIES_DIR)
        if os.path.exists(CATEGORIES_DIR):
            shutil.rmtree(CATEGORIES_DIR)
    
    os.makedirs(COUNTRIES_DIR, exist_ok=True)
    os.makedirs(CATEGORIES_DIR, exist_ok=True)

    channels = remove_duplicates(channels)
    channels = [add_channel_type(ch) for ch in channels]

    # Save to working_channels (now always numbered)
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
        save_split_json(base_path, remove_duplicates(ch_list))

    # Save by category
    for category, ch_list in category_files.items():
        if not category:
            continue
        safe_name = "".join(c for c in category if c.isalnum() or c in (' ', '_', '-')).strip()
        if not safe_name:
            continue
        base_path = os.path.join(CATEGORIES_DIR, safe_name)
        save_split_json(base_path, remove_duplicates(ch_list))

    # Generate summary
    all_channels = load_split_json(WORKING_CHANNELS_BASE)
    generate_categories_summary(all_channels)