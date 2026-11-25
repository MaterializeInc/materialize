#!/bin/sh

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Fail if a lockfile pins a package that OSV lists as malicious (MAL-*).
# Ordinary vulnerabilities are left to cargo-deny and Renovate.

set -eu

# renovate: datasource=docker depName=ghcr.io/google/osv-scanner
image=ghcr.io/google/osv-scanner:v2.7.1@sha256:a70ffffc2bfd33482953e5d6b64c6fcdb30239717a07b0f0d3ab2859ce3e4cde

out=$(mktemp)
trap 'rm -f "$out"' EXIT

# Lockfiles go in on stdin rather than a mount, so the scanner can't touch the
# checkout. osv-scanner exits 1 for any vulnerability, so only 2+ is an error.
status=0
git ls-files -z -- '*Cargo.lock' '*package-lock.json' '*yarn.lock' \
    '*requirements*.txt' '*uv.lock' '*pom.xml' '*Gemfile.lock' \
| tar --null -cf - -T - \
| docker run --rm -i --user 65534 --cap-drop ALL \
    --security-opt no-new-privileges --pids-limit 256 --entrypoint sh "$image" \
    -c 'mkdir /tmp/src && tar -xf - -C /tmp/src && exec /osv-scanner scan source -r --format json /tmp/src' \
    > "$out" || status=$?
if [ "$status" -gt 1 ]; then
    echo "osv-scanner failed with exit $status" >&2
    exit "$status"
fi

jq -e '[.results[]?.packages[]? | .package as $p | .vulnerabilities[]?
        | select([.id, (.aliases // [])[]] | any(startswith("MAL-")))
        | "\(.id): \($p.ecosystem) \($p.name) \($p.version)"]
       | (.[] | "malicious package: \(.)\n" | stderr), length == 0' "$out" > /dev/null
