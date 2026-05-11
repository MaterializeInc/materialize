#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# check-antithesis-compose.sh — ensure test/antithesis/config/docker-compose.yaml
# is in sync with test/antithesis/mzcompose.py.
#
# Fingerprint refs (`mzbuild-<fp>`) shift on every materialized code change,
# so we mask them before diffing — we only want to catch composition
# (services, ports, env, deps) drift, not transient fingerprint churn.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

check_antithesis_compose() {
    local committed=test/antithesis/config/docker-compose.yaml
    local generated rc=0
    generated=$(mktemp)

    bin/pyactivate test/antithesis/export-compose.py > "$generated"

    # Mask `mzbuild-<FINGERPRINT>` so the diff is structural-only.
    local mask='s/(mzbuild-)[A-Z0-9]+/\1FINGERPRINT/g'
    if ! diff -u \
        <(sed -E "$mask" "$committed") \
        <(sed -E "$mask" "$generated"); then
        echo
        echo "$committed is out of sync with test/antithesis/mzcompose.py."
        echo "Regenerate with:"
        echo "  bin/pyactivate test/antithesis/export-compose.py > $committed"
        rc=1
    fi

    rm -f "$generated"
    return $rc
}

try check_antithesis_compose

try_status_report
