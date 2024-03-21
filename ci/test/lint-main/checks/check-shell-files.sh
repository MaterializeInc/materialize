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
# check-shell-files.sh — checks shell files.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

if [[ ! "${MZDEV_NO_SHELLCHECK:-}" ]]; then
    # Only consider a shebang in the first line
    shell_files=$(sort -u <(git_files '*.sh' '*.bash') <(git grep -n '#!.*bash' -- ':!*.*' | grep ":1:" | sed -e "s/:1:.*//"))
    echo "$shell_files"
    try xargs shellcheck -P SCRIPTDIR <<< "$shell_files"
fi

try_status_report
