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

check_all_files_referenced_in_ci() {
    RETURN=0
    COMPOSITIONS=$(find . -name mzcompose.py \
        -not -wholename "./misc/python/materialize/cli/mzcompose.py" `# Only glue code, no workflows` \
        -not -wholename "./misc/monitoring/mzcompose.py" `# Only run manually` \
        -not -wholename "./test/canary-environment/mzcompose.py" `# Only run manually` \
        -not -wholename "./test/console/mzcompose.py" `# Only run manually` \
        -not -wholename "./test/mzcompose_examples/mzcompose.py" `# Example only` \
        -not -wholename "./test/get-cloud-hostname/mzcompose.py" `# Utility, no test` \
        | sed -e "s|.*/\([^/]*\)/mzcompose.py|\1|")
    while read -r composition; do
        if ! grep -q "composition: $composition" ci/*/pipeline.template.yml; then
            echo "mzcompose composition \"$composition\" is unused in any CI pipeline file"
            RETURN=1
        fi
    done <<< "$COMPOSITIONS"
    return $RETURN
}

check_default_workflow_references_others() {
    RETURN=0
    MZCOMPOSE_TEST_FILES=()
    while IFS= read -r file; do
        MZCOMPOSE_TEST_FILES+=("$file")
    done < <(find ./test -name "mzcompose.py" \
        -not -wholename "./test/canary-environment/mzcompose.py" `# Only run manually` \
        -not -wholename "./test/ssh-connection/mzcompose.py" `# Handled differently` \
        -not -wholename "./test/scalability/mzcompose.py" `# Other workflows are for manual usage` \
        -not -wholename "./test/testdrive-old-kafka-src-syntax/mzcompose.py" `# Other workflow is run separately` \
        -not -wholename "./test/terraform/mzcompose.py" `# Handled differently` \
    )

    for file in "${MZCOMPOSE_TEST_FILES[@]}"; do
      MATCHES_COUNT=$(grep "def workflow_" "$file" -c)

      if (( MATCHES_COUNT > 1 )); then
        # mzcompose file contains more than one workflow

        LOOP_DETECTED=$(grep "c.workflow(name" "$file" -c)

        if (( LOOP_DETECTED < 1 )); then
          echo "$file contains more than one workflow but does not seem to loop over the workflows"
          RETURN=1
        fi

      fi
    done

    if (( RETURN > 0 )); then
      echo "Use this pattern in the default workflow:"
      echo "for name in c.workflows:"
      echo "  if name == \"default\":"
      echo "    continue"
      echo ""
      echo "  with c.test_case(name):"
      echo "    c.workflow(name)"
    fi

    return $RETURN
}

# ensure that each mzcompose file is referenced
try check_all_files_referenced_in_ci

# ensure that each mzcompose file with more than one workflow loops over workflows in the default workflow
try check_default_workflow_references_others

# ensure that we can list the compositions without requiring environment variables to be set
try env -i PATH="$PATH" MZ_DEV_CI_BUILDER="${MZ_DEV_CI_BUILDER:-local}" bin/mzcompose list-compositions > /dev/null

try_status_report
