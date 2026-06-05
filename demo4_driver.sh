#!/usr/bin/env bash
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Driver for the fourth recorder-prototype asciinema demo: the progress
# watermark — how the targets' frontier advances with the recorder's inputs.
# Expects the demo-3 objects (foo, foo_ka, a_log) to exist and no recorder
# running.
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

sys() { run 6877 mz_system "$1"; }
usr() { run 6875 materialize "$1"; }

clear
type_out '# RECORDERS (prototype): the progress watermark.'
type_out '# The recorded data carries timestamps, but a reader also needs to know how'
type_out '# far the recorder has CONSUMED its inputs -- the frontier of the targets.'
type_out '# In the full design the commit at T advances the output uppers, including'
type_out '# empty commits. The prototype expresses that frontier as data instead.'
sleep 2

say 'A recorder is running (demo 3). Its watermark advances every tick,'
say 'even though the input is completely idle:'
usr "SELECT * FROM mz_recorder_progress;"
sleep 2
usr "SELECT * FROM mz_recorder_progress;"

say 'The frontier is AHEAD of the newest recorded delta: "no delta at t" is a'
say 'statement about the input, not about the recorder lagging.'
usr "SELECT p.consumed_through > max(l.mz_timestamp) AS nothing_new_through_frontier FROM mz_recorder_progress p, a_log l GROUP BY p.consumed_through;"

say 'Now the recorder disappears ...'
sys "DROP RECORDER foo_a_recorder;"
sleep 3

say '... and the frontier STALLS while wall clock moves on. The targets still'
say 'serve, but a reader can see exactly through when they are complete.'
usr "SELECT consumed_through, mz_now() AS wall_clock, consumed_through < mz_now() AS stalled FROM mz_recorder_progress;"
sleep 2
usr "SELECT consumed_through, mz_now() AS wall_clock, consumed_through < mz_now() AS stalled FROM mz_recorder_progress;"

say 'Without the watermark, a dead recorder is indistinguishable from a quiet'
say 'input. With it, completeness-through-F is part of the recorded state.'
sleep 2
