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
# Lint generated SQL function documentation. Ensures that the checked-in
# doc/user/data/sql_func_gen.json matches the output of generate_func_doc.
#
# Example usages:
#
#     $ ci/test/lint-func-docs.sh
#
# To rewrite the stored file, pass the --rewrite option.

set -euo pipefail

. misc/shlib/shlib.bash

stored="doc/user/data/sql_func_gen.json"

if [[ "${1:-}" = --rewrite ]]; then
    cargo run --bin generate_func_doc > "$stored"
    echo "Rewrote $stored"
    exit 0
fi

ci_uncollapsed_heading "Linting function documentation"

try diff "$stored" <(cargo run --bin generate_func_doc 2>/dev/null)

try_status_report
