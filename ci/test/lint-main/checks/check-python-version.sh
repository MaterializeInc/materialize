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
# check-python-version.sh — make sure Python 3.10 keeps working

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

if [[ ! "${MZDEV_NO_PYTHON:-}" ]]; then
    if ! uv --version >/dev/null 2>/dev/null; then
        echo "lint: uv is not installed"
        echo "hint: refer to https://docs.astral.sh/uv/getting-started/installation/ for install instructions"
        exit 1
    fi

    py310_venv="$(mktemp -d)/venv-py310"
    trap 'rm -rf "$py310_venv"' EXIT

    try uv venv --python 3.10 "$py310_venv"
    try uv pip compile --python-version 3.10 ci/builder/requirements.txt
    try uv pip install --python "$py310_venv/bin/python" --requirement ci/builder/requirements.txt
    # Wrap the compileall (the actual work) in `try`, not git_files, and keep it
    # out of a pipeline: `try` in a pipeline runs in a subshell and loses its
    # accounting. Guard against empty input too, otherwise compileall with no
    # file arguments compiles all of sys.path instead of the repo.
    py_files=$(git_files '*.py')
    if [[ -n "$py_files" ]]; then
        try xargs "$py310_venv/bin/python" -m compileall -q <<< "$py_files"
    fi
fi

try_status_report
