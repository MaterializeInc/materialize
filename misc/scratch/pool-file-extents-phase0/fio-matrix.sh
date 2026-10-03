#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Device envelope for the file-extent I/O candidates: synchronous O_DIRECT
# (psync), io_uring, and buffered I/O, at the extent sizes the pool produces.
# Usage: fio-matrix.sh DIR OUTDIR
set -euo pipefail
dir=$1
out=$2
mkdir -p "$out"
runtime=${RUNTIME:-30}
size=${SIZE:-64G}

common=(--directory="$dir" --filename=extents --size="$size" --fallocate=native
    --time_based --runtime="$runtime" --ramp_time=5 --group_reporting
    --randrepeat=0 --norandommap --output-format=json)

# Lay out the file once so runs measure overwrite of allocated blocks, the
# steady state after the store has grown.
fio --name=layout --directory="$dir" --filename=extents --size="$size" \
    --fallocate=native --rw=write --bs=4M --direct=1 --ioengine=psync >/dev/null

run() {
    local name=$1
    shift
    echo "== $name"
    fio "${common[@]}" --name="$name" "$@" >"$out/$name.json"
}

# Extent sizes: ~0.36 MiB lz4 extents land in the 384 KiB class, identity
# extents of ~2 MiB bodies in the 2 MiB or 3 MiB class.
for bs in 384k 2m; do
    # Demotion writes: spill-thread counts.
    for jobs in 1 2 4; do
        run "w-psync-direct-bs$bs-j$jobs" --rw=randwrite --bs=$bs --direct=1 --ioengine=psync --numjobs=$jobs
        for qd in 4 16; do
            run "w-uring-direct-bs$bs-j$jobs-qd$qd" --rw=randwrite --bs=$bs --direct=1 --ioengine=io_uring --iodepth=$qd --numjobs=$jobs
        done
        run "w-psync-buffered-bs$bs-j$jobs" --rw=randwrite --bs=$bs --direct=0 --ioengine=psync --numjobs=$jobs --fdatasync=1
    done
    # Cold reads: worker counts, synchronous per caller.
    for jobs in 1 16 64; do
        run "r-psync-direct-bs$bs-j$jobs" --rw=randread --bs=$bs --direct=1 --ioengine=psync --numjobs=$jobs
        run "r-uring-direct-bs$bs-j$jobs" --rw=randread --bs=$bs --direct=1 --ioengine=io_uring --iodepth=1 --numjobs=$jobs
    done
done

# Mixed: 2 spill threads writing while 16 workers read, per engine.
for eng in psync io_uring; do
    cat >"$out/mixed-$eng.fio" <<EOF
[global]
directory=$dir
filename=extents
size=$size
time_based
runtime=$runtime
ramp_time=5
direct=1
bs=384k
randrepeat=0
norandommap
ioengine=$eng

[writers]
rw=randwrite
numjobs=2
iodepth=$([ "$eng" = io_uring ] && echo 16 || echo 1)

[readers]
rw=randread
numjobs=16
iodepth=1
EOF
    echo "== mixed-$eng"
    fio --output-format=json "$out/mixed-$eng.fio" >"$out/mixed-$eng.json"
done
