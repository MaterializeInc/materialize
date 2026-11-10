# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Report which metrics changed between two releases.

doc/user/data/metrics.yml is checked in, so it is tagged along with each
release. Reading it at the two tags on either side of a sign-off boundary
answers whether an empty panel is a real signal or a metric that arrived or
departed with the release. The working tree's copy describes main, which is
ahead of whatever is being verified, so do not read that one during a run.

    $ bin/pyactivate .agents/skills/mz-release-signoff/scripts/catalog_diff.py v26.38.0 v26.39.0-rc.3

Covers only metrics registered by `metric!` in the Rust tree. It says nothing
about v2_mz_*, container_*, kube_* or kubelet_*, which are exported by the
promsql exporter, cAdvisor, and kube-state-metrics respectively. Confirm
those against Prometheus instead.
"""

import subprocess
import sys

import yaml

CATALOG = "doc/user/data/metrics.yml"


def catalog_names(text):
    """Return the set of metric names in a catalog's YAML text."""
    return {metric["name"] for metric in yaml.safe_load(text)["metrics"]}


def catalog_at(ref):
    return subprocess.run(
        ["git", "show", f"{ref}:{CATALOG}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def report(before_ref, before, after_ref, after):
    """Render the diff between two name sets as the lines to print."""
    lines = [
        f"{before_ref}: {len(before)} metrics",
        f"{after_ref}: {len(after)} metrics",
        "",
        f"Added in {after_ref} (a before-window on these is empty by construction):",
    ]
    lines += [f"  + {name}" for name in sorted(after - before)]
    lines += [
        "",
        f"Removed in {after_ref} (an after-window on these is empty by construction):",
    ]
    lines += [f"  - {name}" for name in sorted(before - after)]
    return lines


def main():
    if len(sys.argv) != 3:
        print("usage: catalog_diff.py <before-ref> <after-ref>", file=sys.stderr)
        return 2
    before_ref, after_ref = sys.argv[1:]
    try:
        before = catalog_names(catalog_at(before_ref))
        after = catalog_names(catalog_at(after_ref))
    except subprocess.CalledProcessError as e:
        print(e.stderr, end="", file=sys.stderr)
        return 1
    for line in report(before_ref, before, after_ref, after):
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
