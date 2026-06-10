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
# lint — complains about misformatted files and other problems.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

if [[ ! "${MZDEV_NO_SHELLCHECK:-}" ]]; then
    if ! command_exists shellcheck; then
        echo -e "lint: $(red fatal:) unable to find \`shellcheck\` command on your system" >&2
        echo -e "hint: https://github.com/koalaman/shellcheck#installing" >&2
        echo -e "hint: you can disable shellcheck locally by setting \$MZDEV_NO_SHELLCHECK=1" >&2
        exit 1
    fi
    version=$(shellcheck --version | grep version: | grep -oE "[0-9]+\.[0-9]+\.[0-9]+" || echo "0.0.0+unknown")
    if ! version_compat "0.7.0" "$version"; then
        echo -e "lint: $(red fatal:) shellcheck v0.7.0+ is required" >&2
        echo -e "hint: detected version \"$version\"" >&2
        echo -e "hint: you can disable shellcheck locally by setting \$MZDEV_NO_SHELLCHECK=1" >&2
        exit 1
    fi
fi

# Check that GNU sed is available (required by gen-completion).
if ! command_exists gsed && ! (sed --version 2>/dev/null | grep -q "GNU sed"); then
    echo -e "lint: $(red fatal:) GNU sed is required but not found" >&2
    echo -e "hint: on macOS, install it with: brew install gnu-sed" >&2
    exit 1
fi

# Check that Python dependencies are importable. A broken httpx (transitive
# dependency of confluent-kafka) is a common symptom of a stale virtualenv.
if [[ ! "${MZDEV_NO_PYTHON:-}" ]]; then
    if ! bin/pyactivate -c "import httpx" 2>/dev/null; then
        echo -e "lint: $(red fatal:) Python virtualenv has broken dependencies (cannot import \`httpx\`)" >&2
        echo -e "hint: rebuild the virtualenv: rm -rf misc/python/venv" >&2
        exit 1
    fi
fi
