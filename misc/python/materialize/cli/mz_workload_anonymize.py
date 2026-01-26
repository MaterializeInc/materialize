# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import re
import sys
from typing import Any

import yaml


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="mz-workload-anonymize",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Anonymize identifiers in a workload capture file",
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="Path to write the workload.yml, overrides the input file if not specified",
    )

    parser.add_argument(
        "file",
        type=str,
        help="Input workload.yml",
    )

    args = parser.parse_args()

    with open(args.file) as f:
        workload = yaml.load(f, Loader=yaml.CSafeLoader)

    new = {
        "databases": {},
        "clusters": {},
        "queries": [],
        "mz_workload_version": "1.0.0",
    }
    mapping: dict[str, str] = {}

    for i, (name, cluster) in enumerate(workload["clusters"].items()):
        new_name = f"cluster_{i}"
        mapping[name] = new_name
        new["clusters"][new_name] = cluster

    pattern = re.compile(
        "|".join(map(re.escape, sorted(mapping, key=len, reverse=True)))
    )

    def replace_substr(d: dict[str, Any], entry: str) -> None:
        d[entry] = pattern.sub(lambda m: mapping[m.group(0)], d[entry])

    for cluster in new["clusters"].values():
        replace_substr(cluster, "create_sql")
    for query in new["queries"]:
        query["cluster"] = mapping[query["cluster"]]
        query["database"] = mapping[query["database"]]
        query["search_path"] = [mapping[schema] for schema in query["search_path"]]
        replace_substr(query, "sql")

    with open(args.output or args.file) as f:
        yaml.dump(new, f, Dumper=yaml.CSafeDumper)

    return 0


if __name__ == "__main__":
    sys.exit(main())
