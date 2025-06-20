import re
import json
from typing import Dict, List, Any


def strhash(value: Any) -> str:
    """Recursively generate a stable string hash from a nested dict or value."""
    if isinstance(value, dict):
        return ''.join(f"{key}:{strhash(value[key])}" for key in sorted(value))
    return str(value)

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

def load_json_file(filepath: str) -> dict:
    """Load a json file"""
    try:
        with open(filepath, "r") as file:
            return json.load(file)
    except json.JSONDecodeError:
        print(f"Warning: Failed to decode JSON from file: {filepath}")
        return None

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