from opensearchpy import OpenSearch
from opensearch_dsl import Search, Q
from datetime import datetime


class Collector:
    def __init__(self, es_server: str, es_index: str, config: dict):
        self.config = config
        self.es_index = es_index
        self.os_client = OpenSearch(es_server, verify_certs=False, http_compress=True, timeout=30)

    def collect(self, from_date: datetime, to: datetime):
        data = []
        from_timestamp = from_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_timestamp = to.strftime("%Y-%m-%dT%H:%M:%SZ")
        query = Q(
            "bool",
            must_not=[Q("term", **{"jobConfig.name.keyword": "garbage-collection"})],
            must=[Q("range", **{"timestamp": {"gte": from_timestamp, "lte": to_timestamp}})],
        )
        s = (
            Search(using=self.os_client, index=self.es_index)
            .filter("term", **{"metricName.keyword": "jobSummary"})
            .query(query)
        )

        # Use scan to get all results
        for hit in s.scan():
            run_data = {}
            jobSummary = hit.to_dict()
            uuid = jobSummary.get("uuid")
            if uuid not in run_data:
                run_data[uuid] = {"metadata": {}, "metrics": {}}            
            for field in self.config["metadata"]:
                if field in jobSummary:
                    run_data[uuid]["metadata"][field] = jobSummary[field]
                elif "jobConfig" in jobSummary and field in jobSummary["jobConfig"]:
                    run_data[uuid]["metadata"].setdefault("jobConfig", {})[field] = jobSummary["jobConfig"][field]
            metrics, count_verified = self._metrics_by_uuid(uuid)
            if count_verified:
                run_data[uuid]["metrics"] = metrics
            else:
                continue
            data.append(run_data)
        
        return data

    def _metrics_by_uuid(self, uuid: str):
        metrics = {}
        input_list = self.config.get("metrics", {})
        metric_filter = [Q("term", **{"metricName.keyword": metric}) for metric in input_list]
        should_query = Q("bool", should=metric_filter)
        query = Q("bool", must_not=[Q("term", **{"jobConfig.name.keyword": "garbage-collection"})], should=should_query)
        s = Search(using=self.os_client, index=self.es_index).filter("term", **{"uuid.keyword": uuid}).query(query)
        for hit in s.scan():
            datapoint = hit.to_dict()
            if datapoint["metricName"] not in metrics:
                metrics[datapoint["metricName"]] = [datapoint]
            else:
                metrics[datapoint["metricName"]].append(datapoint)
        return metrics, len(metrics) == len(input_list)
