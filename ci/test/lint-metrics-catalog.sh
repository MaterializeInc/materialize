#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Checks that the generated metrics catalog (doc/user/data/metrics.yml) is up to
# date with the `metric!` definitions in the Rust source tree. This guards
# against the checked-in catalog drifting from the source of truth.
#
# Example usages:
#
#     $ ci/test/lint-metrics-catalog.sh
#
# To rewrite the stored catalog, pass the --rewrite option.

set -euo pipefail

. misc/shlib/shlib.bash

catalog=doc/user/data/metrics.yml

if [[ "${1:-}" = --rewrite ]]; then
    ci_uncollapsed_heading "Linting metrics catalog; rewriting $catalog"
    try bin/gen-metrics-catalog
else
    ci_uncollapsed_heading "Linting metrics catalog, run $0 --rewrite to update $catalog"
    tmp=$(mktemp)
    trap 'rm -f "$tmp"' EXIT
    try bin/gen-metrics-catalog "$tmp"
    try diff "$catalog" "$tmp"
fi

try_status_report
