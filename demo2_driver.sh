#!/usr/bin/env bash
# Driver for the second recorder-prototype asciinema demo: arrival-time
# stamping for Postgres CDC data, replacing a Kafka loopback.
#
# Expects: bin/environmentd running; Postgres on 25432 with table
# public.orders (one seed row) in publication mz_source; secret pgpass and
# connection pg_conn already created in Materialize.
set -euo pipefail

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

run() { # run <port> <user> <sql> [password]
    local port=$1 user=$2 sql=$3 pass=${4:-}
    printf '\n'
    type_out "$user> $sql"
    sleep 0.3
    local db=materialize opts="-c welcome_message=off"
    if [ "$port" = 25432 ]; then
        db=postgres
        opts=""
    fi
    PGPASSWORD="$pass" PGOPTIONS="$opts" psql -P pager=off -h "$HOST" -p "$port" -U "$user" \
        "$db" -c "$sql"
    sleep 1.0
}

sys() { run 6877 mz_system "$1"; }
usr() { run 6875 materialize "$1"; }
pg()  { run 25432 postgres "$1" postgres; }

clear
type_out '# RECORDERS (prototype): stamp CDC data with its arrival time -- as data.'
type_out '# Today this customer loops data through Kafka just to add an ingestion'
type_out '# timestamp, then reingests it. A recorder does it in one hop.'
sleep 1.5

say 'The external system: a Postgres database with an orders table.'
pg "SELECT * FROM orders;"

say 'Ingest it with a regular Postgres CDC source.'
usr "CREATE SOURCE pg_src FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_source');"
usr "CREATE TABLE orders FROM SOURCE pg_src (REFERENCE orders) WITH (RETAIN HISTORY = FOR '1 hour');"
sleep 3
usr "SELECT * FROM orders;"

say 'A DELTA TABLE for the stamped changelog, and a RECORDER that records each'
say 'change with now() frozen at processing time: the arrival timestamp.'
usr "CREATE DELTA TABLE orders_log (id int, item text, qty int, arrived_at timestamptz) WITH (RETAIN HISTORY = FOR '1 hour');"
sys "CREATE RECORDER order_arrivals AS RECORD (SELECT c.id, c.item, c.qty, now() AS arrived_at, c.mz_timestamp, c.mz_diff FROM CHANGES(orders AS OF AT LEAST 0) c) INTO orders_log;"
sleep 3
usr "SELECT * FROM orders_log;"

say 'A new order lands in Postgres ...'
pg "INSERT INTO orders VALUES (2, 'monitor', 1);"
sleep 5
say '... and arrives stamped. No Kafka, no reingestion.'
usr "SELECT * FROM orders_log ORDER BY mz_timestamp;"

say 'Updates are stamped too: retract old version, record new version.'
pg "UPDATE orders SET qty = 5 WHERE id = 2;"
sleep 5
usr "SELECT * FROM orders_log ORDER BY mz_timestamp;"

say 'Present it as data: the current orders, each with its arrival time.'
usr "CREATE VIEW orders_with_arrival AS SELECT DISTINCT ON (id) id, item, qty, arrived_at FROM (SELECT id FROM orders_log GROUP BY id HAVING SUM(mz_diff) > 0) live JOIN orders_log USING (id) WHERE mz_diff > 0 ORDER BY id, mz_timestamp DESC;"
usr "SELECT * FROM orders_with_arrival ORDER BY id;"

say 'The arrival time is recorded once, at ingestion -- never recomputed,'
say 'stable across restarts and replicas. The Kafka loopback is gone.'
sleep 2
