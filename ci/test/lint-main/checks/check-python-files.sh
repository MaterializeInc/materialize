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
# check-python-files.sh — check Python files for issues.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

if [[ ! "${MZDEV_NO_PYTHON:-}" ]]; then
    pyright_version=$(sh ci/builder/pyright-version.sh)

    python_files_list=$(mktemp)
    dbt_files_list=$(mktemp)
    trap 'rm -f "$python_files_list" "$dbt_files_list"' EXIT
    { git_files 'ci/*.py' 'misc/python/*.py' 'test/*.py'; git_files '**/mzcompose.py' | grep -v -e 'test/' -e 'misc/dbt-materialize/'; } > "$python_files_list"
    git_files 'misc/dbt-materialize/*.py' > "$dbt_files_list"

    try xargs npx --yes "pyright@$pyright_version" --warnings --threads 4 < "$python_files_list"

    try xargs bin/pyactivate -m ruff < "$python_files_list"
    # We need to maintain compatibility with older Python versions for this
    try xargs bin/pyactivate -m ruff --target-version=py38 < "$dbt_files_list"
fi

try_status_report
