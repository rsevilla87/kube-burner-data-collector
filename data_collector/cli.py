"""Console script for data_collector."""

import argparse
import sys
from data_collector import __version__, collector
from data_collector.config import Config
from datetime import datetime


def main():
    """Console script for data_collector."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="version", version=__version__)
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
        default=datetime.utcnow().timestamp(),
    )
    args = parser.parse_args()
    from_date, to = parse_timerange(args.from_date, args.to)
    if args.command == "collect":
        config = Config(args.config)
        collector_instance = collector.Collector(args.es_server, args.es_index, config.parse())
        collector_instance.collect(from_date, to)
    return 0


def parse_timerange(from_date_dt: datetime, to_dt: datetime):
    try:
        from_date = datetime.utcfromtimestamp(from_date_dt)
        to = datetime.fromtimestamp(to_dt)
    except ValueError:
        print("Invalid date format")
        exit(1)
    if from_date >= to:
        print("Start date must be before end date")
        exit(1)
    return from_date, to


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
