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
# demo.sh: the user-provided LIR plans demo, against a running local environmentd.
#
# Runs unattended. It drops and recreates the objects it uses in the
# `materialize.public` schema. See README.md for how to start the environment and
# how to record the demo.
#
# Environment: MZ_HOST (default localhost), MZ_PORT (default 6875), MZ_SYSTEM_PORT
# (default 6877), DEMO_PAUSE (seconds after each caption, default 3).

set -euo pipefail

demo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
lir_dir=$(dirname "$demo_dir")
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

host=${MZ_HOST:-localhost}
port=${MZ_PORT:-6875}
system_port=${MZ_SYSTEM_PORT:-6877}
pause_secs=${DEMO_PAUSE:-3}

# The connection banner is a notice.
export PGOPTIONS="-c client_min_messages=warning"
psql_user=(psql -h "$host" -p "$port" -U materialize -X -q -P pager=off -v ON_ERROR_STOP=1)
psql_system=(psql -h "$host" -p "$system_port" -U mz_system -X -q -v ON_ERROR_STOP=1)

bold=$'\e[1m'
dim=$'\e[2m'
cyan=$'\e[36m'
yellow=$'\e[33m'
reset=$'\e[0m'

pause() { sleep "${1:-$pause_secs}"; }

step() {
    printf '\n%s━━ Step %s: %s %s\n' "$bold$yellow" "$1" "$2" "$reset"
    pause 1
}

say() {
    printf '%s# %s%s\n' "$dim" "$*" "$reset"
    pause
}

# Prints a statement as typed at a prompt.
prompt() {
    printf '%s%s%s\n' "$cyan" "materialize=> $1" "$reset" | sed '2,$s/^/              /'
}

# Prints a shell command as typed at a prompt. The caller runs it, on the files in
# the scratch directory.
shell() {
    printf '%s$ %s%s\n' "$cyan" "$1" "$reset"
}

# Runs SQL statements, with timing, after printing them. Exits the demo if one
# fails.
sql() {
    prompt "$1"
    printf '\\timing on\n%s\n' "$1" | "${psql_user[@]}" -f - | grep -v '^$'
    pause 1
}

# Runs a SQL file after printing its statements.
sql_file() {
    prompt "$(grep -v '^--' "$demo_dir/$1" | sed '/^$/d')"
    "${psql_user[@]}" -f "$demo_dir/$1"
    pause 1
}

# Runs a SQL file after printing its statements, and exits the demo unless the
# last two columns of its single result row, the rows that only one side
# returns, are both 0.
compare_file() {
    local out
    prompt "$(grep -v '^--' "$demo_dir/$1" | sed '/^$/d')"
    out=$("${psql_user[@]}" -f "$demo_dir/$1")
    printf '%s\n' "$out"
    if ! printf '%s\n' "$out" \
        | awk -F'|' 'NR == 3 && $(NF-1) + 0 == 0 && $NF + 0 == 0 { ok = 1 } END { exit !ok }'; then
        echo "demo: $1 found rows that only one side returns" >&2
        exit 1
    fi
    pause 1
}

# Runs a statement whose `:'plan'` is the contents of the file `$2`. Exits the
# demo if it fails.
sql_plan() {
    local shown="${1//:\'plan\'/\'<$(basename "$2"), $(wc -c <"$2") bytes>\'}"
    prompt "$shown"
    printf '\\timing on\n%s\n' "$1" | "${psql_user[@]}" -v plan="$(cat "$2")" -f - 2>&1 \
        | grep -v '^$' | sed 's/^psql:<stdin>:[0-9]*: //'
    pause 1
}

# Runs a statement whose `:'plan'` is the contents of the file `$2`, and exits
# the demo unless it fails with an error that contains `$3`.
sql_plan_rejected() {
    local shown="${1//:\'plan\'/\'<$(basename "$2"), $(wc -c <"$2") bytes>\'}"
    local out
    prompt "$shown"
    if out=$(printf '%s\n' "$1" | "${psql_user[@]}" -v plan="$(cat "$2")" -f - 2>&1); then
        printf '%s\n' "$out"
        echo "demo: the statement succeeded, but it should have been rejected" >&2
        exit 1
    fi
    printf '%s\n' "$out" | grep -v '^$' | sed 's/^psql:<stdin>:[0-9]*: //'
    if ! grep -qF -- "$3" <<<"$out"; then
        echo "demo: the error does not contain \"$3\"" >&2
        exit 1
    fi
    pause 1
}

"${psql_system[@]}" -c "ALTER SYSTEM SET enable_lir_plans = true" >/dev/null
"${psql_user[@]}" -c "DROP TABLE IF EXISTS transfers, flagged, edges CASCADE" >/dev/null

printf '%sUser-provided LIR plans%s\n' "$bold" "$reset"
say "Materialize can install a materialized view from a physical (LIR) plan written as JSON."
say "The feature flag enable_lir_plans is on."
sql_file setup.sql

step 1 "a SQL view whose plan the optimizer cannot fix"
sql_file flagged_transfers.sql
# The query waits until the view has hydrated.
sql "SELECT count(*) FROM flagged_transfers;"
say "That took a while. The OR in the correlation leaves no equality to join on, so the plan"
say "is a cross join of 50000 account pairs with 1000 flagged accounts, filtered afterwards."
sql "EXPLAIN PHYSICAL PLAN FOR MATERIALIZED VIEW flagged_transfers;"

step 2 "extract the dataflow as JSON"
shell "psql -c 'EXPLAIN PHYSICAL PLAN AS LIR JSON FOR MATERIALIZED VIEW flagged_transfers' > plan.json"
"${psql_user[@]}" -At -c "EXPLAIN PHYSICAL PLAN AS LIR JSON FOR MATERIALIZED VIEW flagged_transfers" \
    >"$work/plan.json"
say "The envelope names its imports by SQL name and holds the plan of each built object:"
python3 - "$work/plan.json" <<'EOF'
import json, sys
envelope = json.load(open(sys.argv[1]))
print(f"lir_version: {envelope['lir_version']}")
for i in envelope["imports"]:
    print(f"import {json.dumps(i['id'])}: {json.dumps(i['source'])}")
text = json.dumps(envelope["objects"])
print(f"objects: {len(envelope['objects'])}, with {text.count('lir_id')} plan nodes in {len(text)} bytes")
print(f"output: {envelope['output']}")
EOF
pause

step 3 "edit the JSON into a better plan with a script"
say "improve_plan.py replaces the cross join with a union of two equi-joins,"
say "one per equality. The distinct above it removes pairs that match both."
shell "python3 misc/lir-datalog/demo/improve_plan.py < plan.json > improved.json"
python3 "$demo_dir/improve_plan.py" <"$work/plan.json" >"$work/improved.json"
pause
say "EXPLAIN validates a plan without installing it:"
sql_plan "EXPLAIN PHYSICAL PLAN FOR CREATE MATERIALIZED VIEW flagged_transfers_fast
    (id int8 NOT NULL, src int8 NOT NULL, dst int8 NOT NULL, amount int8 NOT NULL)
    FROM PLAN :'plan';" "$work/improved.json"
pause

step 4 "a broken plan is rejected"
say "With --broken, the script forgets to arrange the pairs by dst, which the second join reads."
shell "python3 misc/lir-datalog/demo/improve_plan.py --broken < plan.json > broken.json"
python3 "$demo_dir/improve_plan.py" --broken <"$work/plan.json" >"$work/broken.json"
sql_plan_rejected "CREATE MATERIALIZED VIEW flagged_transfers_broken
    (id int8 NOT NULL, src int8 NOT NULL, dst int8 NOT NULL, amount int8 NOT NULL)
    FROM PLAN :'plan';" "$work/broken.json" "(Join): Arrangement"
pause

step 5 "install the improved plan and compare"
sql_plan "CREATE MATERIALIZED VIEW flagged_transfers_fast
    (id int8 NOT NULL, src int8 NOT NULL, dst int8 NOT NULL, amount int8 NOT NULL)
    FROM PLAN :'plan';" "$work/improved.json"
sql "SELECT count(*) FROM flagged_transfers_fast;"
say "Both views return the same rows:"
compare_file compare.sql
say "And the plan does a fraction of the work, at the price of arranging the pairs twice:"
sql_file cost.sql
pause

step 6 "swap the plan under the SQL view"
say "flagged_transfers has no keys, so a plan can replace its dataflow in place."
sql_plan "CREATE REPLACEMENT MATERIALIZED VIEW flagged_transfers_v2
    (id int8 NOT NULL, src int8 NOT NULL, dst int8 NOT NULL, amount int8 NOT NULL)
    FOR flagged_transfers FROM PLAN :'plan';" "$work/improved.json"
sql "ALTER MATERIALIZED VIEW flagged_transfers APPLY REPLACEMENT flagged_transfers_v2;"
sql "SELECT name, create_sql LIKE '%FROM PLAN%' AS from_plan
    FROM mz_materialized_views WHERE name LIKE 'flagged_transfers%' ORDER BY name;"
sql "SELECT count(*) FROM flagged_transfers;"
say "New data flows through the swapped plan:"
sql "INSERT INTO flagged VALUES (42); INSERT INTO transfers VALUES (100001, 42, 7, 500);"
say "The swapped view returns the rows of the SQL query, run directly over the tables:"
compare_file compare_query.sql
say "The cross join dataflow is gone. flagged_transfers now runs the dataflow built as flagged_transfers_v2:"
sql_file cost.sql

step 7 "compile a recursive Datalog program to LIR"
shell "grep -v '^%' misc/lir-datalog/examples/tc.dl"
grep -v '^%' "$lir_dir/examples/tc.dl"
sql "CREATE TABLE edges (src int8 NOT NULL, dst int8 NOT NULL);
INSERT INTO edges VALUES (1, 2), (2, 3), (3, 4), (10, 11), (11, 12), (12, 10);"
shell "python3 misc/lir-datalog/datalog2lir.py misc/lir-datalog/examples/tc.dl --output reachable --import edges=materialize.public.edges > tc.json"
python3 "$lir_dir/datalog2lir.py" "$lir_dir/examples/tc.dl" --output reachable \
    --import edges=materialize.public.edges >"$work/tc.json"
sql_plan "CREATE MATERIALIZED VIEW reachable (src int8 NOT NULL, dst int8 NOT NULL)
    FROM PLAN :'plan';" "$work/tc.json"
sql "SELECT src, array_agg(dst ORDER BY dst) AS reaches FROM reachable GROUP BY src ORDER BY src;"
say "The view stays up to date. Closing the chain into a cycle:"
sql "INSERT INTO edges VALUES (4, 1);"
sql "SELECT src, array_agg(dst ORDER BY dst) AS reaches FROM reachable GROUP BY src ORDER BY src;"

printf '\n%sDone.%s\n' "$bold" "$reset"
