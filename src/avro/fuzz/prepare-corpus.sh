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
# prepare-corpus.sh — populate cargo-fuzz seed corpora with valid Avro
# container files so the fuzzer doesn't waste cycles bouncing off the
# magic-header check. libFuzzer mutates these into deeper structural
# variants while still hitting real decoder code paths.

set -euo pipefail

cd "$(dirname "$0")"

mkdir -p corpus/reader_decode
find corpus/reader_decode -maxdepth 1 -name 'seed_*.avro' -delete
cp ../benches/quickstop-null.avro corpus/reader_decode/seed_01_quickstop_null.avro

echo "Seeded:"
for d in corpus/*/; do
    count=$(find "$d" -maxdepth 1 -name '*.avro' | wc -l)
    printf "  %-40s %4d seeds\n" "$d" "$count"
done
