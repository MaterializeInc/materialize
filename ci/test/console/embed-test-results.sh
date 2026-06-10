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
# embed-test-results.sh — Pipe filter that detects Playwright attachment
# paths in test output, uploads them as Buildkite artifacts, and injects
# inline image / link escape sequences right after the path line.
#
# Usage:  yarn test:e2e:all | ../ci/test/console/embed-test-results.sh | tee run.log

has_buildkite=false
command -v buildkite-agent &>/dev/null && has_buildkite=true

declare -A uploaded_dirs

while IFS= read -r line; do
    if $has_buildkite; then
        # Suppress "attachment #N: type ────" header lines.
        [[ "$line" =~ ^[[:space:]]*attachment\ \#[0-9]+: ]] && continue
        # Suppress "────" separator lines.
        [[ "$line" =~ ^[[:space:]]*─+[[:space:]]*$ ]] && continue
    fi

    echo "$line"

    # Match lines that are just a test-results artifact path (with optional whitespace).
    if [[ "$line" =~ ^[[:space:]]*(test-results/[^[:space:]]+\.(png|webm|zip))[[:space:]]*$ ]]; then
        filepath="${BASH_REMATCH[1]}"
        [[ -f "$filepath" ]] || continue
        dir=$(dirname "$filepath")

        if $has_buildkite; then
            # Upload the entire failure directory on first encounter.
            if [[ -z "${uploaded_dirs[$dir]:-}" ]]; then
                uploaded_dirs[$dir]=1
                buildkite-agent artifact upload "${dir}/*" 2>/dev/null
            fi

            test_name=$(basename "$dir")
            case "$filepath" in
                *.png)
                    printf "\033]1338;url='artifact://%s';alt='%s'\a\n" "$filepath" "$test_name"
                    ;;
                *.webm)
                    printf "\033]1339;url='artifact://%s';content='Video: %s'\a\n" "$filepath" "$test_name"
                    ;;
                *.zip)
                    printf "\033]1339;url='artifact://%s';content='Trace: %s'\a\n" "$filepath" "$test_name"
                    ;;
            esac
        fi
    fi
done
