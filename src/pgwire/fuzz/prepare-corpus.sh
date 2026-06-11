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
# prepare-corpus.sh — populate the `codec_decode` corpus with hand-crafted
# valid pgwire frontend frames. libFuzzer mutates these into adjacent
# malformed variants while keeping enough structure to reach the deeper
# parsing paths.

set -euo pipefail

cd "$(dirname "$0")"

corpus=corpus/codec_decode
mkdir -p "$corpus"
find "$corpus" -maxdepth 1 -name 'seed_*.bin' -delete

python3 - "$corpus" <<'PY'
import os, struct, sys
corpus = sys.argv[1]

def frame(tag: bytes, payload: bytes) -> bytes:
    # Standard pgwire frame: 1-byte type tag + 4-byte BE length (incl
    # itself) + payload.
    return tag + struct.pack(">I", 4 + len(payload)) + payload

def cstr(s: str) -> bytes:
    return s.encode() + b"\x00"

def startup() -> bytes:
    # Startup messages have no tag, just length + version + key/value pairs.
    payload = struct.pack(">I", 0x0003_0000)
    payload += cstr("user") + cstr("mz")
    payload += cstr("database") + cstr("materialize")
    payload += cstr("application_name") + cstr("fuzz")
    payload += b"\x00"
    return struct.pack(">I", 4 + len(payload)) + payload

seeds = {
    "01_startup": startup(),
    "02_ssl_request": struct.pack(">II", 8, 0x04D2_162F),
    "03_cancel_request": struct.pack(">IIII", 16, 0x04D2_162E, 12345, 67890),
    "04_query_select_1": frame(b"Q", cstr("SELECT 1")),
    "05_query_empty": frame(b"Q", cstr("")),
    "06_parse_named": frame(b"P", cstr("stmt1") + cstr("SELECT $1::int4") + struct.pack(">H", 0)),
    "07_bind_unnamed": frame(b"B", cstr("") + cstr("") + struct.pack(">HHH", 0, 0, 0) + struct.pack(">H", 0)),
    "08_execute": frame(b"E", cstr("") + struct.pack(">I", 0)),
    "09_sync": frame(b"S", b""),
    "10_terminate": frame(b"X", b""),
    "11_describe_portal": frame(b"D", b"P" + cstr("")),
    "12_describe_statement": frame(b"D", b"S" + cstr("")),
    "13_close_portal": frame(b"C", b"P" + cstr("")),
    "14_flush": frame(b"H", b""),
    "15_copy_data": frame(b"d", b"some-bytes"),
    "16_copy_done": frame(b"c", b""),
    "17_copy_fail": frame(b"f", cstr("client gave up")),
    "18_password": frame(b"p", cstr("hunter2")),
    "19_sasl_initial": frame(b"p", cstr("SCRAM-SHA-256") + struct.pack(">I", 5) + b"hello"),
    "20_function_call": frame(b"F", struct.pack(">IHH", 100, 0, 0) + struct.pack(">H", 0) + struct.pack(">H", 0)),
}

for name, blob in seeds.items():
    with open(os.path.join(corpus, f"seed_{name}.bin"), "wb") as f:
        f.write(blob)
PY

echo "Seeded:"
count=$(find "$corpus" -maxdepth 1 -name '*.bin' | wc -l)
printf "  %-40s %4d seeds\n" "$corpus" "$count"
