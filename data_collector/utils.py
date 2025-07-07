import re
import logging
from datetime import datetime
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

def split_list_into_chunks(lst, chunk_size):
    """Splits a list into given chunk sizes"""
    for idx in range(0, len(lst), chunk_size):
        yield lst[idx:idx + chunk_size]

def strhash(value: Any) -> str:
    """Recursively generate a stable string hash from a nested dict or value"""
    if isinstance(value, dict):
        return ''.join(f"{key}:{strhash(value[key])}" for key in sorted(value))
    return str(value)

def parse_timerange(from_date_dt: datetime, to_dt: datetime):
    """pareses dates and returns UTC formats"""
    try:
        from_date = datetime.utcfromtimestamp(from_date_dt)
        to = datetime.utcfromtimestamp(to_dt)
    except ValueError:
        logger.info("Invalid date format")
        exit(1)
    if from_date >= to:
        logger.info("Start date must be before end date")
        exit(1)
    return from_date, to

def compile_exclude_patterns(patterns_str: str) -> List[re.Pattern]:
    """Compiles the patterns to be excluded"""
    if not patterns_str:
        return []
    return [re.compile(pattern.strip()) for pattern in patterns_str.split(",")]

def should_exclude(metric_name: str, patterns: List[re.Pattern]) -> bool:
    """Return a boolean on exclusion decision"""
    return any(p.search(metric_name) for p in patterns)

def remove_keys_by_patterns(data: Dict, patterns: List[str]) -> Dict:
    """Removes keys in a dict based on regex list"""
    regexes = [re.compile(p) for p in patterns]
    return {
        k: v for k, v in data.items()
        if not any(r.match(k) for r in regexes)
    }

def recursively_flatten_values(obj: dict) -> dict:
    """Recursively flatten the json structure"""
    if isinstance(obj, dict):
        # If the dict is exactly {"_value": ...}, reduce it to the value
        if list(obj.keys()) == ["_value"]:
            return obj["_value"]

        # Otherwise, process each item and flatten children if possible
        new_obj = {}
        for k, v in obj.items():
            flattened_v = recursively_flatten_values(v)
            new_obj[k] = flattened_v
        return new_obj

    elif isinstance(obj, list):
        return [recursively_flatten_values(elem) for elem in obj]

    else:
        return obj

def flatten_json(flattened, obj, parent_key=""):
    """Flatten a json structure"""
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}_{k}" if parent_key else k
            flatten_json(flattened, v, new_key)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            # If it's a dict with "quantileName", use that for key suffix
            if isinstance(item, dict) and "quantileName" in item:
                name = item["quantileName"]
                for k, v in item.items():
                    if k != "quantileName":
                        new_key = f"{parent_key}_{name}_{k}"
                        flattened[new_key] = v
            else:
                flatten_json(flattened, item, f"{parent_key}_{i}")
    else:
        flattened[parent_key] = obj
