"""Console script for data_collector."""

import os
import sys
import logging
import argparse
import urllib3
import csv
from data_collector import __version__, collector
from data_collector.config import Config
from data_collector.normalize import normalize
from data_collector.s3 import upload_csv_to_s3
from data_collector.utils import split_list_into_chunks, parse_timerange
from data_collector.constants import S3_BUCKET, CHUNK_SIZE, VALID_LOG_LEVELS
from data_collector.logging import configure_logging
import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def main():
    """Console script for data_collector."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--log-level", 
                        type=str, 
                        choices=VALID_LOG_LEVELS, 
                        default=os.environ.get("LOG_LEVEL", "INFO").upper(), 
                        help="Logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL). Can also be set via LOG_LEVEL env var"
    )
    sub_parsers = parser.add_subparsers(dest="command")
    collect = sub_parsers.add_parser("collect", help="Collect ES data")
    collect.add_argument("--es-server", action="store", help="ES Server endpoint", required=True)
    collect.add_argument("--es-index", action="store", help="ES Index name", required=True)
    collect.add_argument("--config", action="store", help="Configuration file")
    collect.add_argument(
        "--from",
        action="store",
        help="Start date, in epoch seconds",
        required=True,
        type=int,
        dest="from_date",
    )
    collect.add_argument(
        "--to",
        action="store",
        help="End date, in epoch seconds",
        type=int,
        default=datetime.datetime.now(datetime.UTC).timestamp(),
    )
    args = parser.parse_args()
    configure_logging(args.log_level)
    logger = logging.getLogger(__name__)
    logger.info(f"CLI args: {args}")
    from_date, to = parse_timerange(args.from_date, args.to)
    normalized_rows = []
    if args.command == "collect":
        config = Config(args.config)
        logger.debug(f"Processing input configuration: {config}")
        input_config = config.parse()
        collector_instance = collector.Collector(args.es_server, args.es_index, input_config)
        data = collector_instance.collect(from_date, to)
        for each_run in data:
            for _, run_json in each_run.items():
                normalized_json = normalize(run_json, ",".join(input_config["exclude_normalization"]))
                normalized_rows.append(normalized_json)
    # Write to CSV
    if normalized_rows:
        fieldnames = sorted(set().union(*normalized_rows))
        for idx, chunk in enumerate(split_list_into_chunks(normalized_rows, CHUNK_SIZE), start=1):
            filename = f"{input_config['output_prefix']}_{from_date.strftime('%Y-%m-%dT%H:%M:%SZ')}_{to.strftime('%Y-%m-%dT%H:%M:%SZ')}_chunk_{idx}.csv"
            upload_csv_to_s3(chunk, fieldnames, S3_BUCKET, input_config["benchmark"], filename)
    return 0

if __name__ == "__main__":
    sys.exit(main())
