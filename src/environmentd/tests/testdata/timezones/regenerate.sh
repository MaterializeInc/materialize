#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regenerates the pg_timezone_names / pg_timezone_abbrevs snapshots that
# tests/timezones.rs compares Materialize against.
#
# The contents of those catalog tables depend on the wall clock, so each
# snapshot is a dump of PostgreSQL taken while the server believes it is the
# date encoded in the filename. Rather than changing any real clock, this
# script runs PostgreSQL in a container under libfaketime.
#
# The PostgreSQL major version follows test/postgres/Dockerfile. The IANA
# data PostgreSQL reads must match the version compiled into Materialize via
# chrono-tz, else the snapshots assert a database we do not ship. Distro
# tzdata packages lag IANA on their own schedule, so instead of trusting the
# image's package the build compiles /usr/share/zoneinfo from the pinned IANA
# source release with zic (Debian's PostgreSQL reads system tzdata). Update
# EXPECTED_TZDATA when bumping chrono-tz.

set -euo pipefail

cd "$(dirname "$0")"

EXPECTED_TZDATA=2025b
IMAGE=mz-timezone-snapshot

DATES=("$@")
if [ ${#DATES[@]} -eq 0 ]; then
    DATES=(2026-06-15 2026-12-15)
fi

docker build -q -t "$IMAGE" --build-arg TZDATA_VERSION="$EXPECTED_TZDATA" -f - . <<'EOF'
FROM postgres:18.4
ARG TZDATA_VERSION
RUN apt-get update && apt-get install -y --no-install-recommends faketime curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir /tmp/tzdata && cd /tmp/tzdata \
    && curl -fsSL "https://data.iana.org/time-zones/releases/tzdata${TZDATA_VERSION}.tar.gz" | tar xz \
    && zic -d /usr/share/zoneinfo africa antarctica asia australasia \
        etcetera europe northamerica southamerica backward factory \
    && cp version /usr/share/zoneinfo/+VERSION \
    && rm -rf /tmp/tzdata
EOF

for date in "${DATES[@]}"; do
    echo "==> snapshotting at $date" >&2
    cid=$(docker run -d --rm -e POSTGRES_HOST_AUTH_METHOD=trust "$IMAGE" \
        bash -c "exec faketime '$date 00:00:00' docker-entrypoint.sh postgres")
    trap 'docker stop "$cid" >/dev/null 2>&1 || true' EXIT

    for _ in $(seq 60); do
        if docker exec "$cid" pg_isready -U postgres >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    # The official image restarts the server once during initialization, so a
    # single pg_isready success can race the restart. Settle, then re-check.
    sleep 3
    docker exec "$cid" pg_isready -U postgres >/dev/null

    tzdata=$(docker exec "$cid" cat /usr/share/zoneinfo/+VERSION)
    if [ "$tzdata" != "$EXPECTED_TZDATA" ]; then
        echo "error: container tzdata is $tzdata, expected $EXPECTED_TZDATA" >&2
        exit 1
    fi

    faked_now=$(docker exec "$cid" psql -U postgres -X -q -t -A -c "SELECT now()::date")
    if [ "$faked_now" != "$date" ]; then
        echo "error: server believes it is $faked_now, expected $date" >&2
        exit 1
    fi

    docker exec "$cid" psql -U postgres -X -q -t -A -F, \
        -c "SELECT * FROM pg_catalog.pg_timezone_names ORDER BY name" \
        >"names-$date.csv"
    docker exec "$cid" psql -U postgres -X -q -t -A -F, \
        -c "SELECT * FROM pg_catalog.pg_timezone_abbrevs ORDER BY abbrev" \
        >"abbrevs-$date.csv"

    docker stop "$cid" >/dev/null
    trap - EXIT
    echo "==> wrote names-$date.csv, abbrevs-$date.csv" >&2
done
