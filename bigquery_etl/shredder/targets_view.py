#!/usr/bin/env python3

"""Create a view showing shredder target tables' sizes."""

from argparse import ArgumentParser
from itertools import chain, groupby
from multiprocessing.pool import ThreadPool
import logging
import warnings

from google.cloud import bigquery

from .config import DELETE_TARGETS, find_glean_targets
from ..format_sql.formatter import reformat
from ..util import standard_args
from ..util.bigquery_id import sql_table_id


parser = ArgumentParser(description=__doc__)
parser.add_argument("view")
standard_args.add_dry_run(parser)
standard_args.add_log_level(parser)
standard_args.add_parallelism(parser)


def get_query(targets):
    """Generate a query to get table metadata for targets."""
    return " UNION ALL ".join(
        f"SELECT * FROM `{dataset}.__TABLES__` WHERE table_id IN ("
        + ", ".join(f"'{target.table_id}'" for target in targets)
        + ")"
        for dataset, targets in groupby(
            sorted(targets, key=sql_table_id),
            key=lambda target: f"{target.project}.{target.dataset_id}",
        )
    )


def main():
    """Create view of metadata for shredder delete targets."""
    args = parser.parse_args()
    client = bigquery.Client()
    with ThreadPool(args.parallelism) as pool:
        targets = chain(find_glean_targets(pool, client), DELETE_TARGETS)
    ddl = reformat(f"CREATE OR REPLACE VIEW `{args.view}` AS {get_query(targets)}")
    run_tense = "Would run" if args.dry_run else "Running"
    logging.debug(f"{run_tense} query:\n{ddl}")
    client.query(ddl, bigquery.QueryJobConfig(dry_run=args.dry_run))


if __name__ == "__main__":
    warnings.filterwarnings("ignore", module="google.auth._default")
    main()
