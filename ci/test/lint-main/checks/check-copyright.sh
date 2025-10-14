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
# check-copyright.sh — check copyright headers.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

files=$(git_files "$@")

copyright_files=$(grep -vE \
    -e '(^|/)LICENSE$' \
    -e '(^|/)\.(docker|git|vscode|bazel)ignore$' \
    -e '(^|/)\.bazelrc$' \
    -e '(^|/)\.bazelversion$' \
    -e '(^|/)\.gitattributes$' \
    -e '(^|/)\.github/(dependabot\.yml|CODEOWNERS)$' \
    -e '(^|/)\.gitmodules$' \
    -e '(^|/)go\.sum$' \
    -e '(^|/)(Cargo|askama|config)\.toml$' \
    -e '^\.cargo/config$' \
    -e '^\.config/hakari.toml$' \
    -e '^.devcontainer/.*' \
    -e '(^|/)Cargo\.lock$' \
    -e '^about\.toml$' \
    -e '^deny\.toml$' \
    -e '(^|/)Gemfile\.lock$' \
    -e '^netlify\.toml$' \
    -e '^rustfmt\.toml$' \
    -e '^clippy\.toml$' \
    -e '^\.config/nextest\.toml$' \
    -e '(^|/)yarn\.lock$' \
    -e '(^|/)requirements.*\.txt$' \
    -e '\.(md|json|asc|png|jpe?g|svg|avro|avsc|pb|ico|html|so|uxf)$' \
    -e '^doc/user/.*(\.scss|\.bnf|\.toml|\.yml)$' \
    -e '^ci/builder/(ssh_known_hosts|crosstool-.+\.defconfig)$' \
    -e '^ci/www/public/_redirects$' \
    -e '^ci/test/lint-deps/' \
    -e '^misc/bazel/c_deps/patches/snappy-config.patch' \
    -e '^misc/completions/.*' \
    -e '^misc/foundationdb/.*' \
    -e '^misc/mcp-materialize/uv.lock' \
    -e '^misc/mcp-materialize-agents/uv.lock' \
    -e '^misc/mcp-materialize-agents/mcp_materialize_agents/system_prompt.md' \
    -e '^misc/python/MANIFEST\.in' \
    -e '^test/chbench/chbench' \
    -e '^src/pgtz/tznames/.*' \
    -e '^test/sqllogictest/postgres/testdata/.*\.data' \
    -e '^test/pgtest/.*\.pt' \
    -e '^test/pgtest-mz/.*\.pt' \
    -e '^test/coordtest/.*\.ct' \
    -e '^test/ldbc-bi/.*\.sql' \
    -e '^test/ldbc-bi/.*\.log' \
    -e '^src/catalog/tests/snapshots/.*\.snap' \
    -e '^src/catalog/src/durable/upgrade/snapshots/.*' \
    -e '^src/catalog/src/durable/upgrade/persist/snapshots/.*\.snap' \
    -e '^src/expr-derive-impl/src/snapshots.*' \
    -e '^src/expr/src/scalar/func/impls/snapshots/.*' \
    -e '^src/expr/src/scalar/func/snapshots/.*' \
    -e '^src/expr/src/scalar/snapshots/.*' \
    -e '^src/license-keys/src/license_keys/.*\.pub' \
    -e '^src/storage-types/src/snapshots/.*' \
    -e '^src/repr/src/adt/snapshots/.*' \
    -e '^src/environmentd/tests/testdata/timezones/.*\.csv' \
    -e '^test/fivetran-destination/.*\/00-README$' \
    <<< "$files"
)

try xargs -n1 awk -f misc/lint/copyright.awk <<< "$copyright_files"

try_status_report
