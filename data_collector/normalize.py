import re
import logging
from typing import Dict, List
from data_collector.utils import (
    strhash,
    should_exclude,
    compile_exclude_patterns,
    recursively_flatten_values,
    remove_keys_by_patterns,
    flatten_json,
)

logger = logging.getLogger(__name__)

DROP_LIST = ['metadata','uuid','metricName','labels','query', 'value', 'jobName', 'timestamp']
LABELS_LIST = ["mode", "verb", "namespace", "resource", "container", "component", "endpoint"]
DEFAULT_HASH = "xyz"


def process_json(metric: str, entries: dict, skip_patterns: List[re.Pattern], output: Dict) -> None:
    """Processes JSON and generates a huge json with minimal data"""
    if not entries:
        return

    metric_name = entries[0].get("metricName")
    if not metric_name:
        logger.info(f"Warning: 'metricName' missing in first entry of the metric: {metric}")
        return

    if should_exclude(metric_name, skip_patterns):
        return

    grouped_metrics = {}
    for entry in entries:
        # Skip the metircs during churn phase to avoid noise
        if 'churnMetric' in entry:
            continue
        # Skip metrics during garbage collection as well to avoid noise
        if 'jobName' in entry and entry['jobName'].lower() == 'garbage-collection':
            continue
        label_hash = DEFAULT_HASH
        labels = entry.get("labels")
        if labels:
            label_hash = strhash(labels)

        if label_hash not in grouped_metrics:
            grouped_metrics[label_hash] = {"value": 0.0}
            if labels:
                grouped_metrics[label_hash]["labels"] = {k: labels[k] for k in LABELS_LIST if k in labels}

        # Drop unneeded fields
        if "value" in entry:
            # reduces value to average
            entry = {"value": entry["value"]}
            grouped_metrics[label_hash]["value"] += entry["value"]/2
        else:
            # handles cases where metrics don't have value. for example, quantiles
            for k in DROP_LIST:
                entry.pop(k,None)
            # Need to deal with this edge case as we set {"value": 0.0} as default above
            if isinstance(grouped_metrics[label_hash]["value"], (int, float)):
                grouped_metrics[label_hash].pop("value", None)
            if "value" not in grouped_metrics[label_hash]:
                grouped_metrics[label_hash]["value"] = [entry]
            else:
                grouped_metrics[label_hash]["value"].append(entry)

    # Adds up condensed data values to output json
    if metric_name in output["metrics"]:
        output["metrics"][metric_name].extend(grouped_metrics.values())
    else:
        output["metrics"][metric_name] = list(grouped_metrics.values())

def normalize_metrics(metrics: dict) -> dict:
    """Intermidiate normalization step to further reduce the json"""

    # Labels precedence order used for nesting
    nest_order = ["mode", "verb", "namespace", "component", "resource", "container", "endpoint"]
    nested_metrics = {}

    for metric, entries in metrics:
        nested_metrics.setdefault(metric, {})

        for entry in entries:
            labels = entry.get("labels", {})
            value = entry["value"]

            # Get available keys from labels, in nest_order
            label_keys = [k for k in nest_order if k in labels]
            if not label_keys:
                # No labels at all, store directly under metric
                existing = nested_metrics[metric]
                if isinstance(existing, (int, float)):
                    nested_metrics[metric] = (existing + value) / 2
                elif isinstance(existing, dict):
                    if "_value" in nested_metrics[metric]:
                        nested_metrics[metric]["_value"] = (nested_metrics[metric]["_value"] + value) / 2
                    else:
                        nested_metrics[metric]["_value"] = value
                else:
                    nested_metrics[metric] = value
                continue

            curr = nested_metrics[metric]
            for _, key in enumerate(label_keys):
                # logc to generate nested keys with labels
                key_value = labels[key]
                group_key = f"byLabel{key.capitalize()}"
                curr = curr.setdefault(group_key, {})
                if key_value in curr:
                    if isinstance(curr[key_value], (int, float)):
                        curr[key_value] = {"_value": curr[key_value]}
                    curr = curr[key_value]
                else:
                    curr = curr.setdefault(key_value, {})

            # Now we're at the leaf, insert _value
            if "_value" in curr:
                curr["_value"] = (curr["_value"] + value) / 2
            else:
                curr["_value"] = value

    return nested_metrics

def get_cluster_health(alerts: list, passed: bool) -> str:
    """Calculates and returns cluster health"""
    has_error, has_warning = False, False
    for alert in alerts:
        if alert["severity"].lower() == 'warning':
            has_warning = True
        if alert["severity"].lower() == 'error':
            has_error = True
    if has_error or not passed:
        return "Red"
    if has_warning:
        return "Yellow"
    return "Green"

def normalize(metrics_data: dict, exclude_metrics: str):
    """Driver code to triger the execution"""
    skip_patterns = compile_exclude_patterns(exclude_metrics)

    merged_output = {"metrics": {}}

    for metric, value in metrics_data["metrics"].items():
        process_json(metric, value, skip_patterns, merged_output)

    nested_metrics = normalize_metrics(merged_output["metrics"].items())

    final_output = recursively_flatten_values(nested_metrics)

    flattened = {}
    flatten_json(flattened, final_output)
    patterns_to_remove = [r"(?i).*time.*", r"uuid", r"version"]
    metadata = remove_keys_by_patterns(metrics_data["metadata"], patterns_to_remove)
    for key, value in metadata.items():
        if "jobConfig" != key:
            flattened[key] = value
        else:
            for key, value in metadata.get("jobConfig", {}).items():
                flattened[f"jobConfig.{key}"] = value
    alerts = metrics_data["metrics"]["alert"] if 'alert' in metrics_data["metrics"] else []
    flattened["cluster_health_score"] = get_cluster_health(alerts, metadata["passed"])
    return flattened
