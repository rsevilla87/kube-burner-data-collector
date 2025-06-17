from opensearchpy import OpenSearch
from opensearch_dsl import Search, Q
from datetime import datetime


class Collector:
    def __init__(self, es_server: str, es_index: str, config: dict):
        self.config = config
        self.es_index = es_index
        self.os_client = OpenSearch(es_server, verify_certs=True, http_compress=True, timeout=30)

    def collect(self, from_date: datetime, to: datetime):
        data = []
        from_timestamp = from_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_timestamp = to.strftime("%Y-%m-%dT%H:%M:%SZ")
        query = Q(
            "bool",
            must_not=[Q("term", **{"jobName.keyword": "garbage-collection"})],
            must=[Q("range", **{"timestamp": {"gte": from_timestamp, "lte": to_timestamp}})],
        )
        s = (
            Search(using=self.os_client, index=self.es_index)
            .filter("term", **{"metricName.keyword": "jobSummary"})
            .query(query)
        )
        # Use scan to get all results
        for hit in s.scan():
            metadata = {}
            uuid = hit.to_dict().get("uuid")
            if uuid not in metadata:
                metadata[uuid] = {"metadata": {}, "metrics": {}}
            for metadata_field, metadata_value in hit.to_dict().items():
                if metadata_field in self.config["metadata"]:
                    metadata[uuid]["metadata"][metadata_field] = metadata_value
            metrics = self._metrics_by_uuid(uuid)
            metadata[uuid]["metrics"] = metrics
            data.append(metadata)
        return data

    def _metrics_by_uuid(self, uuid: str):
        metrics = {}
        metric_filter = [Q("term", **{"metricName.keyword": metric}) for metric in self.config.get("metrics", {})]
        should_query = Q("bool", should=metric_filter)
        query = Q("bool", must_not=[Q("term", **{"jobName.keyword": "garbage-collection"})], should=should_query)
        s = Search(using=self.os_client, index=self.es_index).filter("term", **{"uuid.keyword": uuid}).query(query)
        for hit in s.scan():
            datapoint = hit.to_dict()
            metrics[datapoint["metricName"]] = datapoint["value"]
        return metrics
