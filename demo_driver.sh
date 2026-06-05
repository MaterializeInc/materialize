#!/usr/bin/env bash
# Driver for the recorder-prototype asciinema demo.
set -euo pipefail

export PGOPTIONS="-c welcome_message=off"
HOST=127.0.0.1

type_out() {
    local s=$1
    local i
    for ((i = 0; i < ${#s}; i++)); do
        printf '%s' "${s:i:1}"
        sleep 0.012
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
    psql -P pager=off -h "$HOST" -p "$port" -U "$user" materialize -c "$sql"
    sleep 1.0
}

sys() { run 6877 mz_system "$1"; }
usr() { run 6875 materialize "$1"; }

clear
type_out '# RECORDERS (prototype) -- record processing-time decisions, never recompute them'
type_out '# Design: doc/developer/design/20260604_recorders.md (PR #36909)'
sleep 1

say 'An event stream and a dimension table.'
usr "CREATE TABLE events (id int, fk text) WITH (RETAIN HISTORY = FOR '1 hour');"
usr "CREATE TABLE dim (key text, val text) WITH (RETAIN HISTORY = FOR '1 hour');"
usr "INSERT INTO dim VALUES ('a', 'EUR 0.92');"

say 'A DELTA TABLE: a durable changelog, with implicit mz_timestamp / mz_diff columns.'
usr "CREATE DELTA TABLE enriched (id int, fk text, val text) WITH (RETAIN HISTORY = FOR '1 hour');"
usr "SHOW COLUMNS FROM enriched;"

say 'A RECORDER: on each event delta, look up dim AT PROCESSING TIME and record the result.'
say 'The bare "dim" reference is a frozen lookup -- freeze is typing, not a keyword.'
sys "CREATE RECORDER rec AS RECORD (SELECT e.id, e.fk, d.val, e.mz_timestamp, e.mz_diff FROM CHANGES(events AS OF AT LEAST 0) e JOIN dim d ON e.fk = d.key) INTO enriched;"

say 'An event arrives ...'
usr "INSERT INTO events VALUES (1, 'a');"
sleep 2.5
say '... and is recorded with the dim value of *now*.'
usr "SELECT * FROM enriched;"

say 'The dimension changes ...'
usr "UPDATE dim SET val = 'EUR 0.95' WHERE key = 'a';"
sleep 2.5
say '... but the recorded row does NOT: the value is frozen. No backfill, ever.'
usr "SELECT * FROM enriched;"

say 'A new event freezes the new value.'
usr "INSERT INTO events VALUES (2, 'a');"
sleep 2.5
usr "SELECT * FROM enriched ORDER BY mz_timestamp;"

say 'Deletes are recorded too, as retraction deltas (mz_diff = -1).'
usr "DELETE FROM events WHERE id = 1;"
sleep 2.5
usr "SELECT * FROM enriched ORDER BY mz_timestamp;"

say 'INTEGRATE by hand: the current state is the integral of the recorded deltas.'
usr "CREATE VIEW current_enriched AS SELECT DISTINCT ON (id, fk) id, fk, val FROM (SELECT id, fk FROM enriched GROUP BY id, fk HAVING SUM(mz_diff) > 0) live JOIN enriched USING (id, fk) WHERE mz_diff > 0 ORDER BY id, fk, mz_timestamp DESC;"
usr "SELECT * FROM current_enriched;"

say 'Only the live event remains -- with its frozen, never-recomputed value.'
sleep 2
