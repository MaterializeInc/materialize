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
    ruff_files_list=$(mktemp)
    trap 'rm -f "$python_files_list" "$dbt_files_list" "$ruff_files_list"' EXIT
    # Type-check every tracked Python file except two kinds. dbt-materialize
    # keeps compatibility with older Python versions and has its own check
    # below. The MCP server packages declare dependencies in their own
    # pyproject.toml that the repository venv does not install, so pyright
    # cannot resolve their imports.
    git_files '*.py' \
        | grep -v -e '^misc/dbt-materialize/' -e '^misc/mcp-materialize/mcp_materialize/' -e '^misc/mcp-materialize/tests/' -e '^misc/mcp-materialize-agents/mcp_materialize_agents/' \
        > "$python_files_list"
    git_files 'misc/dbt-materialize/*.py' > "$dbt_files_list"

    try xargs npx --yes "pyright@$pyright_version" --warnings --threads 4 < "$python_files_list"

    # Lint tracked files only, see `_ruff_cmd` in `misc/python/materialize/cli/fmt.py`.
    git_files '*.py' '*.pyi' '*.ipynb' | grep -v '^misc/dbt-materialize/' > "$ruff_files_list"
    try xargs bin/pyactivate -m ruff check < "$ruff_files_list"
    # We need to maintain compatibility with older Python versions for this
    try xargs bin/pyactivate -m ruff check --target-version=py38 < "$dbt_files_list"
fi

try_status_report
