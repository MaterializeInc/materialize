#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euo pipefail

cd "$(dirname "$0")/../.."

requirements=misc/python/materialize/antithesis/requirements.txt
python_executable=$(bin/pyactivate -c 'import sys; print(sys.executable)')
if "$python_executable" - "$requirements" <<'PY'
import importlib
import importlib.metadata
from pathlib import Path
import sys

requirement = next(
    line.strip()
    for line in Path(sys.argv[1]).read_text().splitlines()
    if line.strip() and not line.startswith("#")
)
package, expected = requirement.split("==")
try:
    importlib.import_module("antithesis.assertions")
    sys.exit(importlib.metadata.version(package) != expected)
except (ImportError, importlib.metadata.PackageNotFoundError):
    sys.exit(1)
PY
then
    exit 0
fi

install_args=(--requirement "$requirements")
if "$python_executable" -c 'import sys; sys.exit(sys.prefix != sys.base_prefix)'; then
    # CI's system Python is read-only. Its user site remains visible to pydoc
    # after that command scrubs the environment.
    install_args+=(--target "$("$python_executable" -c 'import site; print(site.getusersitepackages())')")
fi
if [[ "${MZ_LINT_OFFLINE:-0}" == 1 ]]; then
    install_args+=(--no-index)
fi

if command -v uv >/dev/null 2>&1; then
    uv pip install --python "$python_executable" "${install_args[@]}"
else
    "$python_executable" -m pip install "${install_args[@]}"
fi
