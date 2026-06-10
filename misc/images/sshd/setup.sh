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

if [[ "${MAX_STARTUPS:-}" ]]; then
    augtool -s <<< "set /files/etc/ssh/sshd_config/MaxStartups $MAX_STARTUPS"
fi

if [[ "${ALLOW_ANY_KEY:-}" == "true" ]]; then
    cat > /usr/local/bin/allow-any-key <<'EOF'
#!/bin/bash
echo "$1 $2"
EOF
    chmod +x /usr/local/bin/allow-any-key
    augtool -s <<EOF
set /files/etc/ssh/sshd_config/AuthorizedKeysCommand "/usr/local/bin/allow-any-key %t %k"
set /files/etc/ssh/sshd_config/AuthorizedKeysCommandUser "nobody"
save
EOF
fi
