# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Build one batched PromQL range query from a roster of metrics.

Step 5 of the skill batches many metrics into a single query by tagging each
aggregate with a synthetic label and combining with `or`. Twenty of those
written by hand is where typos live, and a typo here reads as a healthy zero.

The roster is a text file, one metric per line:

    c cmd_started        mz_persist_cmd_started_count
    c columnar_invalid   mz_persist_columnar_op_count  op="validation",result="invalid"
    g arr_records        v2_mz_arrangement_record_count

The first field is the type, `c` for a counter, which becomes `rate`, or `g`
for a gauge, which becomes `avg_over_time`. The second is the tag that labels
the result series, and it is prefixed with its position so the output order is
stable. The third is the metric. Anything after that is an extra selector.
Blank lines and `#` comments are ignored.

The stacks differ in exactly one way, which is how the fleet is selected, so
that is the only thing `--stack` changes. Staging joins against the
release-candidate version, which also drops the development environments
sharing the stack. Production pins the canary namespaces by name, because they
run the plain released version alongside customer environments for part of the
week and a version filter loses them.

    $ build-range-query.py --stack staging roster.txt
    $ build-range-query.py --stack prod --namespaces environment-aaa-0,environment-bbb-0 roster.txt
"""

import argparse
import sys

RC_JOIN = (
    "and on(namespace) group by (namespace) "
    '(v2_mz_compute_cluster_status{{mz_version=~".*-rc[.].*"}})'
)


def parse_roster(text):
    """Yield (kind, tag, metric, selector) for each roster entry."""
    for lineno, raw in enumerate(text.splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        parts = line.split(None, 3)
        if len(parts) < 3:
            raise SystemExit(
                f"roster line {lineno}: expected `kind tag metric [selector]`, got {raw!r}"
            )
        kind, tag, metric = parts[0], parts[1], parts[2]
        if kind not in ("c", "g"):
            raise SystemExit(
                f"roster line {lineno}: kind must be `c` or `g`, got {kind!r}"
            )
        yield kind, tag, metric, parts[3] if len(parts) > 3 else ""


def aggregate(kind, metric, selector, window, stack, namespaces):
    selectors = [selector] if selector else []
    if stack == "prod":
        # A regex alternation keeps the canary set readable and keeps the
        # expression valid when only one namespace is pinned.
        alternation = "|".join(namespaces)
        selectors.append(f'namespace=~"{alternation}"')
    inner = f"{metric}{{{','.join(selectors)}}}" if selectors else metric
    fn = (
        f"rate({inner}[{window}])"
        if kind == "c"
        else f"avg_over_time({inner}[{window}])"
    )
    if stack == "prod":
        return f"sum({fn})"
    return f"sum({fn} {RC_JOIN.format()})"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("roster", help="roster file, or - for stdin")
    parser.add_argument("--stack", choices=("staging", "prod"), required=True)
    parser.add_argument(
        "--namespaces",
        default="",
        help="comma-separated canary namespaces, required for --stack prod",
    )
    parser.add_argument(
        "--window",
        default="6h",
        help="rate and avg_over_time window, which must match the query step (default 6h)",
    )
    args = parser.parse_args()

    namespaces = [n.strip() for n in args.namespaces.split(",") if n.strip()]
    if args.stack == "prod" and not namespaces:
        parser.error("--stack prod requires --namespaces")

    text = sys.stdin.read() if args.roster == "-" else open(args.roster).read()

    parts = []
    for i, (kind, tag, metric, selector) in enumerate(parse_roster(text), 1):
        agg = aggregate(kind, metric, selector, args.window, args.stack, namespaces)
        parts.append(f'label_replace({agg}, "m", "{i:02d}_{tag}", "", "")')

    if not parts:
        raise SystemExit("roster is empty")
    print(" or ".join(parts))


if __name__ == "__main__":
    main()
