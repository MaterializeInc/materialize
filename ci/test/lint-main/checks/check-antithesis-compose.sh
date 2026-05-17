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
# check-antithesis-compose.sh — ensure every test/antithesis/configs/<group>/docker-compose.yaml
# is in sync with test/antithesis/mzcompose.py + test/antithesis/groups.yaml.
#
# Image refs in the committed YAMLs are `${MATERIALIZED_IMAGE}` style
# placeholders (resolved from `.env` at compose-parse time), so the files
# are stable across materialized source changes. A plain diff catches any
# composition (services/ports/env/deps) drift.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

check_antithesis_compose() {
    local rc=0

    mapfile -t groups < <(bin/pyactivate -c "
import sys
sys.path.insert(0, 'test/antithesis')
from groups import load_manifest
for name in sorted(load_manifest().groups):
    print(name)
")

    for group in "${groups[@]}"; do
        local committed="test/antithesis/configs/$group/docker-compose.yaml"
        local generated
        generated=$(mktemp)

        bin/pyactivate test/antithesis/export-compose.py --group="$group" > "$generated"

        if ! diff -u "$committed" "$generated"; then
            echo
            echo "$committed is out of sync with test/antithesis/mzcompose.py + groups.yaml."
            echo "Regenerate with:"
            echo "  bin/pyactivate test/antithesis/export-compose.py --group=$group > $committed"
            rc=1
        fi

        rm -f "$generated"
    done

    return $rc
}

try check_antithesis_compose

try_status_report
