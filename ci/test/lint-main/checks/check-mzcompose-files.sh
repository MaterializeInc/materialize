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
# check-mzcompose-files.sh - Make sure mzcompose files run automatically in CI

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

check() {
    RETURN=0
    COMPOSITIONS=$(find . -name mzcompose.py \
        -not -wholename "./misc/python/materialize/cli/mzcompose.py" `# Only glue code, no workflows` \
        -not -wholename "./misc/monitoring/mzcompose.py" `# Only run manually` \
        -not -wholename "./test/canary-environment/mzcompose.py" `# Only run manually` \
        -not -wholename "./test/mzcompose_examples/mzcompose.py" `# Example only` \
        | sed -e "s|.*/\([^/]*\)/mzcompose.py|\1|")
    while read -r composition; do
        if ! grep -q "composition: $composition" ci/*/pipeline.template.yml; then
            echo "mzcompose composition \"$composition\" is unused in any CI pipeline file"
            RETURN=1
        fi
    done <<< "$COMPOSITIONS"
    return $RETURN
}

try check

try_status_report
