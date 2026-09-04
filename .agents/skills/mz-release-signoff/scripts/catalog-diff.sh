#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# catalog-diff.sh — which metrics changed between two releases.
#
# doc/user/data/metrics.yml is checked in, so it is tagged along with each
# release. Reading it at the two tags on either side of a sign-off boundary
# answers whether an empty panel is a real signal or a metric that arrived or
# departed with the release. The working tree's copy describes main, which is
# ahead of whatever is being verified, so do not read that one during a run.
#
#     $ .agents/skills/mz-release-signoff/scripts/catalog-diff.sh v26.38.0 v26.39.0-rc.3
#
# Covers only metrics registered by `metric!` in the Rust tree. It says nothing
# about v2_mz_*, container_*, kube_* or kubelet_*, which are exported by the
# promsql exporter, cAdvisor, and kube-state-metrics respectively. Confirm
# those against Prometheus instead.

set -euo pipefail

# sort and comm must agree on collation, and the ambient locale is not C on
# every machine that runs this.
export LC_ALL=C

if [[ $# -ne 2 ]]; then
    echo "usage: $(basename "$0") <before-ref> <after-ref>" >&2
    exit 2
fi

before=$1
after=$2

# comm compares under the shell's collation, so sort here rather than relying
# on the extractor's own ordering.
names() {
    git show "$1:doc/user/data/metrics.yml" \
        | python3 "$(dirname "$0")/catalog_names.py" \
        | LC_ALL=C sort
}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

names "$before" > "$tmp/before"
names "$after" > "$tmp/after"

printf '%s: %s metrics\n' "$before" "$(wc -l < "$tmp/before")"
printf '%s: %s metrics\n\n' "$after" "$(wc -l < "$tmp/after")"

echo "Added in $after (a before-window on these is empty by construction):"
comm -13 "$tmp/before" "$tmp/after" | sed 's/^/  + /'
echo

echo "Removed in $after (an after-window on these is empty by construction):"
comm -23 "$tmp/before" "$tmp/after" | sed 's/^/  - /'
