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
    typecheck_cmd="npx --yes pyright@$pyright_version --warnings"

    python_folders=(ci misc/python)

    compose_files=$(git_files "**/mzcompose.py" | grep -v "test/")

    # NOTE: we actually _want_ the word-splitting behavior on `typecheck_cmd` below,
    # so turn off the warning
    # shellcheck disable=SC2086
    try $typecheck_cmd "${python_folders[@]}" $compose_files "test/"

    try bin/pyactivate -m ruff --extend-exclude=misc/dbt-materialize .
    # We need to maintain compatibility with older Python versions for this
    try bin/pyactivate -m ruff --target-version=py38 misc/dbt-materialize
fi

try_status_report
