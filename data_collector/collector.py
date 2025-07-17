import logging
import time
from opensearchpy import OpenSearch
from opensearch_dsl import Search, Q
from datetime import datetime

logger = logging.getLogger(__name__)

class Collector:
    def __init__(self, es_server: str, es_index: str, config: dict):
        """Init method for instance variables"""
        self.config = config
        self.es_index = es_index
        self.os_client = OpenSearch(es_server, verify_certs=False, http_compress=True, timeout=30)
        logging.getLogger("opensearch").setLevel(logging.WARNING)

    def collect(self, from_date: datetime, to: datetime):
        """Collects data from the elastic search"""
        start_time = time.time()
        data = []
        from_timestamp = from_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_timestamp = to.strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"Elasticsearch index: {self.es_index}, benchmark: {self.config['benchmark']}")
        query = Q(
            "bool",
            must_not=[Q("term", **{"jobConfig.name.keyword": "garbage-collection"})],
            must=[
                    Q("term", **{"jobConfig.name.keyword": self.config["benchmark"]}),
                    Q("range", **{"timestamp": {"gte": from_timestamp, "lte": to_timestamp}}),
                ],
        )
        logger.debug(f"Constructed Elasticsearch query: {query.to_dict()}")
        # Get number of query hits
        hit_count = self.os_client.count(
            index=self.es_index,
            body={
                "query": query.to_dict(),
            },
        )
        s = (
            Search(using=self.os_client, index=self.es_index)
            .filter("term", **{"metricName.keyword": "jobSummary"})
            .params(scroll="5m", size=100)
            .query(query)
        )
        # Use scan to get all results
        try:
            hits_processed = 0
            for hit in s.scan():
                hits_processed += 1
                run_data = {}
                jobSummary = hit.to_dict()
                uuid = jobSummary.get("uuid")
                if not uuid:
                    logger.warning("Missing UUID in jobSummary, skipping entry.")
                    continue
                logging.info(f"Processing  {hits_processed}/{hit_count['count']}: {uuid}")
                if uuid not in run_data:
                    logger.debug("UUID not present in run data, adding it")
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
                    logger.debug(f"No verified metrics for UUID {uuid}, skipping.")
                    continue
                data.append(run_data)

        except Exception as e:
            logger.warning(f"Scroll context lost: {e}, continuing with partial results.")
        
        elapsed = time.time() - start_time
        logger.info(f"Data collection completed in {elapsed:.2f} seconds.")
        return data

    def _metrics_by_uuid(self, uuid: str):
        """Collects the list of metrics for an uuid"""
        metrics = {}
        input_list = self.config.get("metrics", {})
        metric_filter = [Q("term", **{"metricName.keyword": metric}) for metric in input_list]
        should_query = Q("bool", should=metric_filter)
        query = Q("bool", must_not=[Q("term", **{"jobConfig.name.keyword": "garbage-collection"})], should=should_query)
        s = Search(using=self.os_client, index=self.es_index).filter("term", **{"uuid.keyword": uuid}).query(query)
        logger.info(f"Running query: {s.to_dict()}")
        for hit in s.scan():
            datapoint = hit.to_dict()
            if datapoint["metricName"] not in metrics:
                metrics[datapoint["metricName"]] = [datapoint]
            else:
                metrics[datapoint["metricName"]].append(datapoint)
        return metrics, len(metrics) == len(input_list)
