#!/usr/bin/env bash
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
set -euo pipefail

GIT_ROOT="$(git rev-parse --show-toplevel)"

OUTPUT="idle_stat.csv"

echo "workers,pid,comm,state,ppid,pgrp,session,tty_nr,tpgid,flags,minflt,cminflt,majflt,cmajflt,utime,stime,cutime,cstime,priority,nice,num_threads,itrealvalue,starttime,vsize,rss,rsslim,startcode,endcode,startstack,kstkesp,kstkeip,signal,blocked,sigignore,sigcatch,wchan,nswap,cnswap,exit_signal,processor,rt_priority,policy,delayacct_blkio_ticks,guest_time,cguest_time,start_data,end_data,start_brk,arg_start,arg_end,env_start,env_end,exit_code
" > "$OUTPUT"

cargo build --release --bin materialized
NPROC="$(nproc)"
#NPROC=128

MZDATA="$GIT_ROOT/mzdata"

for repetition in {1..8}; do
    for ((i=1;i<="$NPROC";i=2*i)); do
        echo "${i}" >&2
        [ -d "$MZDATA" ] && rm -r "$MZDATA"
        cargo run --release --bin materialized -- --workers "${i}" &
        PID=$!
        sleep 120
        echo -n "${i}," >> "$OUTPUT"
        sed 's/ /,/g' "/proc/$PID/stat" >> "$OUTPUT"
        kill "$PID"
        wait "$PID" || echo "$?"
    done
done
