# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Extract metric names and label selectors from a dashboard panel-query dump.

`mcp__grafana__get_dashboard_panel_queries` returns about 62 KB across 165
panels for the compute dashboard, which overflows the tool result and is
written to a file instead. That file must never be read whole or pasted into
the conversation. Slice it with this:

    $ panel-metrics.py panels.json                 # metric -> panels that use it
    $ panel-metrics.py panels.json --selectors     # also show label selectors
    $ panel-metrics.py panels.json --names-only    # bare names, for a roster

The output is a superset of what the build exports, because a panel outlives
the metric it plots. Resolve the names against the catalog for the release
under test before trusting them.
"""

import argparse
import collections
import json
import re
import sys

# A PromQL metric name, optionally followed by a label selector. Excludes
# names immediately preceded by a word character so that `foo_bucket` inside
# an already-matched token is not matched again.
METRIC = re.compile(
    r"(?<![\w.])((?:mz_|v2_mz_|container_|kube_|kubelet_|crdb_)[a-z0-9_]+)(\{[^}]*\})?"
)

# PromQL keywords that can precede a brace and would otherwise look like a name.
NOT_METRICS = {"by", "on", "without", "group_left", "group_right", "ignoring", "offset"}


def panels(doc):
    """Yield (title, query) for each panel target, tolerating both dump shapes."""
    items = doc if isinstance(doc, list) else doc.get("panels", doc.get("targets", []))
    for item in items:
        if not isinstance(item, dict):
            continue
        title = item.get("title") or item.get("refId") or "<untitled>"
        query = item.get("query") or item.get("processedQuery") or item.get("expr")
        if query:
            yield title, query


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("dump", help="JSON file written by get_dashboard_panel_queries")
    parser.add_argument(
        "--selectors", action="store_true", help="show label selectors too"
    )
    parser.add_argument(
        "--names-only", action="store_true", help="print bare metric names"
    )
    args = parser.parse_args()

    with open(args.dump) as f:
        doc = json.load(f)

    uses = collections.defaultdict(set)
    selectors = collections.defaultdict(set)
    for title, query in panels(doc):
        for name, selector in METRIC.findall(query):
            if name in NOT_METRICS:
                continue
            uses[name].add(title)
            if selector:
                selectors[name].add(selector)

    if not uses:
        print("no metric names found; check the dump shape", file=sys.stderr)
        return 1

    for name in sorted(uses):
        if args.names_only:
            print(name)
            continue
        print(f"{name}  ({len(uses[name])} panels)")
        if args.selectors:
            for selector in sorted(selectors[name]):
                print(f"    {selector}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
