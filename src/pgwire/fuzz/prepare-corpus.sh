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
# prepare-corpus.sh populates the `codec_decode` corpus with hand-crafted
# valid pgwire frontend frames. libFuzzer mutates these into adjacent
# malformed variants while keeping enough structure to reach the deeper
# parsing paths.
#
# The target drives `Codec::decode`, which dispatches only post-startup
# frontend messages. The startup handshake (StartupMessage, SSLRequest,
# CancelRequest) is parsed by a separate `decode_startup` path that this
# target does not exercise, so no startup-phase seeds are included.
#
# Every seed carries the two-byte control prefix the target reads off the front
# of the input before anything else (see `run` in codec_decode.rs): the first
# byte selects the raw-bytes branch, the second selects chunked feeding, and only
# the bytes after them reach the decoder. Without the prefix a seed's own type
# tag is eaten as a control byte, the decoder reads the length field's second
# byte as the message tag, and the frame parks in the await-more-bytes state
# without ever reaching the body parser the seed is named after.

set -euo pipefail

cd "$(dirname "$0")"

corpus=corpus/codec_decode
mkdir -p "$corpus"
find "$corpus" -maxdepth 1 -name 'seed_*.bin' -delete

python3 - "$corpus" <<'PY'
import os, struct, sys
corpus = sys.argv[1]

# Steers `run` onto the raw-bytes branch, feeding the rest of the seed to the
# decoder verbatim: `data[0] % 4 == 0` picks raw, `data[1] % 2 == 0` picks
# chunked.
CONTROL_PREFIX = b"\x00\x00"

def frame(tag: bytes, payload: bytes) -> bytes:
    # Standard pgwire frame: 1-byte type tag + 4-byte BE length (incl
    # itself) + payload.
    return CONTROL_PREFIX + tag + struct.pack(">I", 4 + len(payload)) + payload

def cstr(s: str) -> bytes:
    return s.encode() + b"\x00"

def sasl_initial(mechanism: str, client_first: str) -> bytes:
    # `decode_sasl_initial_response` reads the mechanism cstring, then a declared
    # response length it only rejects when negative, then parses the rest as a
    # client-first-message.
    resp = client_first.encode()
    return cstr(mechanism) + struct.pack(">I", len(resp)) + resp

seeds = {
    "01_query_select_1": frame(b"Q", cstr("SELECT 1")),
    "02_query_empty": frame(b"Q", cstr("")),
    "03_parse_named": frame(b"P", cstr("stmt1") + cstr("SELECT $1::int4") + struct.pack(">H", 0)),
    "04_bind_unnamed": frame(b"B", cstr("") + cstr("") + struct.pack(">HHH", 0, 0, 0) + struct.pack(">H", 0)),
    "05_execute": frame(b"E", cstr("") + struct.pack(">I", 0)),
    "06_sync": frame(b"S", b""),
    "07_terminate": frame(b"X", b""),
    "08_describe_portal": frame(b"D", b"P" + cstr("")),
    "09_describe_statement": frame(b"D", b"S" + cstr("")),
    "10_close_portal": frame(b"C", b"P" + cstr("")),
    "11_flush": frame(b"H", b""),
    "12_copy_data": frame(b"d", b"some-bytes"),
    "13_copy_done": frame(b"c", b""),
    "14_copy_fail": frame(b"f", cstr("client gave up")),
    "15_password": frame(b"p", cstr("hunter2")),
    # The auth seeds carry payloads that parse, so the sub-parsers the target
    # runs on `RawAuthentication` get real starting coverage. A SCRAM message
    # is comma-delimited and the parsers abort on the first unexpected byte, so
    # an approximate payload would never reach the later fields.
    "16_sasl_initial": frame(b"p", sasl_initial("SCRAM-SHA-256", "n,,n=user,r=rOprNGfwEbeRWgbNEkqO")),
    "17_sasl_initial_cbind": frame(
        b"p", sasl_initial("SCRAM-SHA-256-PLUS", "p=tls-server-end-point,a=admin,n=user,r=rOprNGfwEbeRWgbNEkqO")
    ),
    "18_sasl_response": frame(b"p", b"c=biws,r=rOprNGfwEbeRWgbNEkqO,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="),
}

for name, blob in seeds.items():
    with open(os.path.join(corpus, f"seed_{name}.bin"), "wb") as f:
        f.write(blob)
PY

echo "Seeded:"
count=$(find "$corpus" -maxdepth 1 -name '*.bin' | wc -l)
printf "  %-40s %4d seeds\n" "$corpus" "$count"
