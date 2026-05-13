import os
import json
import shutil
from typing import List, Dict

from config import WORKING_CHANNELS_BASE, COUNTRIES_DIR, CATEGORIES_DIR, MAX_CHANNELS_PER_FILE, CATEGORIES_SUMMARY_FILE


def delete_split_files(base_name: str):
    if os.path.exists(base_name + '.json'):
        os.remove(base_name + '.json')
    i = 1
    while True:
        p = f"{base_name}{i}.json"
        if not os.path.exists(p):
            break
        os.remove(p)
        i += 1


def load_split_json(base_name: str) -> List[Dict]:
    data = []
    # main file
    if os.path.exists(base_name + '.json'):
        try:
            with open(base_name + '.json', 'r', encoding='utf-8') as f:
                data.extend(json.load(f))
        except:
            pass
    # split files
    i = 1
    while True:
        p = f"{base_name}{i}.json"
        if not os.path.exists(p):
            break
        try:
            with open(p, 'r', encoding='utf-8') as f:
                data.extend(json.load(f))
        except:
            pass
        i += 1
    return data


def save_split_json(base_name: str, data: List[Dict]):
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
    count = {}
    for ch in channels:
        for cat in ch.get("categories", ["general"]):
            key = (cat or "general").strip().lower()
            count[key] = count.get(key, 0) + 1

    summary = [{"category": k.capitalize(), "count": v} 
               for k, v in sorted(count.items(), key=lambda x: x[1], reverse=True)]

    with open(CATEGORIES_SUMMARY_FILE, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=4, ensure_ascii=False)