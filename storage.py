import os
import json
import shutil
import glob
from datetime import datetime
from typing import List, Dict

from config import (
    WORKING_CHANNELS_BASE, 
    COUNTRIES_DIR, 
    CATEGORIES_DIR, 
    MAX_CHANNELS_PER_FILE, 
    CATEGORIES_SUMMARY_FILE,
    LAST_UPDATE_FILE
)
from utils import remove_duplicates, add_channel_type


def delete_split_files(base_name: str):
    """Delete all split files for a base name (main + all numbered parts)."""
    # Delete main file: base_name.json
    main_file = base_name + '.json'
    if os.path.exists(main_file):
        os.remove(main_file)
    
    # Delete all numbered part files: base_nameN.json for any N
    # Pattern matches base_name followed by digits and .json
    pattern = base_name + '*[0-9]*.json'
    for part_file in glob.glob(pattern):
        if part_file != main_file:   # avoid double-deleting main if it matched pattern
            os.remove(part_file)


def load_split_json(base_name: str) -> List[Dict]:
    """Load data from split JSON files"""
    data = []
    
    # Main file
    main_file = base_name + '.json'
    if os.path.exists(main_file):
        try:
            with open(main_file, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
                if isinstance(loaded_data, list):
                    data.extend(loaded_data)
        except (json.JSONDecodeError, Exception):
            pass
    
    # All numbered part files
    pattern = base_name + '*[0-9]*.json'
    for part_file in sorted(glob.glob(pattern)):
        if part_file == main_file:
            continue
        try:
            with open(part_file, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
                if isinstance(loaded_data, list):
                    data.extend(loaded_data)
        except (json.JSONDecodeError, Exception):
            pass
    
    return data


def save_split_json(base_name: str, data: List[Dict]):
    """Save data with splitting if too large"""
    if not data:
        # Create empty file to indicate no data
        with open(base_name + '.json', 'w', encoding='utf-8') as f:
            json.dump([], f, indent=4, ensure_ascii=False)
        return
    
    delete_split_files(base_name)   # now deletes ALL old files
    
    if len(data) <= MAX_CHANNELS_PER_FILE:
        with open(base_name + '.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    else:
        for i in range(0, len(data), MAX_CHANNELS_PER_FILE):
            chunk = data[i:i + MAX_CHANNELS_PER_FILE]
            part_num = i // MAX_CHANNELS_PER_FILE + 1
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


def save_last_update_info(total_channels: int, retained_channels: int, 
                          new_channels: int, removed_channels: int):
    """Save update information to last_update.json"""
    update_info = {
        'last_updated': datetime.now().isoformat(),
        'total_channels': total_channels,
        'retained_channels': retained_channels,
        'new_channels': new_channels,
        'removed_channels': removed_channels
    }
    
    with open(LAST_UPDATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(update_info, f, indent=4, ensure_ascii=False)


def load_last_update_info() -> Dict:
    """Load last update information"""
    try:
        with open(LAST_UPDATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            'last_updated': 'Never',
            'total_channels': 0,
            'retained_channels': 0,
            'new_channels': 0,
            'removed_channels': 0
        }


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