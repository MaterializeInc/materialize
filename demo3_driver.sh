#!/usr/bin/env bash
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Driver for the third recorder-prototype asciinema demo: Seth's multi-action
# recorder — record (key, a) changes with b frozen at processing time,
# maintain a latest-per-key view, prune superseded log rows.
set -euo pipefail

HOST=127.0.0.1

type_out() {
    local s=$1
    local i
    for ((i = 0; i < ${#s}; i++)); do
        printf '%s' "${s:i:1}"
        sleep 0.010
    done
    printf '\n'
}

say() {
    printf '\n'
    type_out "-- $1"
    sleep 0.4
}

run() { # run <port> <user> <sql>
    local port=$1 user=$2 sql=$3
    printf '\n'
    type_out "$user> $sql"
    sleep 0.3
    PGOPTIONS="-c welcome_message=off" psql -P pager=off -h "$HOST" -p "$port" -U "$user" \
        materialize -c "$sql"
    sleep 1.0
}

run_file() { # run_file <port> <user> <file>: type and run a multi-line statement
    local port=$1 user=$2 file=$3
    printf '\n'
    printf '%s> ' "$user"
    printf '\n'
    while IFS= read -r line; do
        type_out "$line"
    done <"$file"
    sleep 0.3
    PGOPTIONS="-c welcome_message=off" psql -P pager=off -h "$HOST" -p "$port" -U "$user" \
        materialize -f "$file"
    sleep 1.0
}

sys() { run 6877 mz_system "$1"; }
usr() { run 6875 materialize "$1"; }

clear
type_out '# RECORDERS (prototype): a multi-action recorder.'
type_out '# Record (key, a) changes with b frozen at processing time, maintain a'
type_out '# latest-per-key view, and prune superseded log rows -- one object,'
type_out '# three composed actions: RECORD / INTEGRATE / DELETE.'
sleep 1.5

say 'The base table, and the driver: differentiate the projection that drops b,'
say 'so a b-only edit consolidates to zero and produces no delta.'
usr "CREATE TABLE foo (key bigint, a bigint, b bigint) WITH (RETAIN HISTORY = FOR '1 hour');"
usr "CREATE MATERIALIZED VIEW foo_ka WITH (RETAIN HISTORY = FOR '1 hour') AS SELECT key, a FROM foo;"

say 'The durable log of (key, a) changes, with the frozen b.'
usr "CREATE DELTA TABLE a_log (key bigint, a bigint, b bigint) WITH (RETAIN HISTORY = FOR '1 hour');"

say 'The recorder: three named relations, three actions, one object.'
cat >/tmp/recorder3.sql <<'SQL'
CREATE RECORDER foo_a_recorder WITH
  -- Driver: fires only when (key, a) changes. Joining foo back as a bare
  -- TVC reference freezes the current b at processing time.
  recorded AS (
    SELECT c.key, c.a, f.b, c.mz_timestamp, c.mz_diff
    FROM CHANGES(foo_ka AS OF AT LEAST 0) c
    JOIN foo f ON c.key = f.key
    WHERE c.mz_diff > 0
  ),
  -- Most recent recorded row per key.
  latest AS (
    SELECT DISTINCT ON (key) key, a, b
    FROM a_log
    WHERE mz_diff > 0
    ORDER BY key, mz_timestamp DESC
  ),
  -- Rows that are NOT the per-key latest: a newer recorded row exists.
  superseded AS (
    SELECT l.*
    FROM a_log l
    WHERE l.mz_diff > 0
      AND EXISTS (
        SELECT 1 FROM a_log l2
        WHERE l2.key = l.key
          AND l2.mz_diff > 0
          AND l2.mz_timestamp > l.mz_timestamp
      )
  )
AS
  RECORD    recorded   INTO a_log,             -- append frozen (key, a, b) deltas
  INTEGRATE latest     AS   foo_with_frozen_b, -- the maintained view you want
  DELETE    superseded FROM a_log;             -- bound the log: one row per key
SQL
run_file 6877 mz_system /tmp/recorder3.sql

say 'Two rows arrive.'
usr "INSERT INTO foo VALUES (1, 10, 100), (2, 20, 200);"
sleep 4
usr "SELECT * FROM a_log ORDER BY key;"
usr "SELECT * FROM foo_with_frozen_b ORDER BY key;"

say 'A b-only edit: the projection consolidates to zero deltas -- the recorder'
say 'does not fire, and the frozen b stays exactly as recorded.'
usr "UPDATE foo SET b = 999 WHERE key = 1;"
sleep 4
usr "SELECT * FROM a_log ORDER BY key;"
usr "SELECT * FROM foo_with_frozen_b ORDER BY key;"

say 'An edit to a DOES fire, freezing the b of *now* (999).'
usr "UPDATE foo SET a = 11 WHERE key = 1;"
sleep 4
usr "SELECT * FROM a_log ORDER BY key, mz_timestamp;"
usr "SELECT * FROM foo_with_frozen_b ORDER BY key;"

say 'And the DELETE action prunes the superseded row: the log stays bounded,'
say 'one row per key.'
sleep 4
usr "SELECT * FROM a_log ORDER BY key;"

say 'RECORD, INTEGRATE, DELETE: compositions of one calculus, not three features.'
sleep 2
