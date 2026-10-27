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
# prepare-corpus.sh: populate cargo-fuzz seed corpora with valid Avro
# container files so the fuzzer doesn't waste cycles bouncing off the
# magic-header check. libFuzzer mutates these into deeper structural
# variants while still hitting real decoder code paths.
#
# `reader_decode` does not read its input as a container file. It reads it as an
# `arbitrary::Unstructured` recipe, and only the branch selected by the first
# byte feeds the remaining bytes to `Reader` verbatim. A container file dropped
# in as-is is therefore consumed as generator choices and never reaches the
# magic-header check at all, so each seed is prefixed with the byte that selects
# that raw branch. `int_in_range(0..=3)` takes one byte from the front and
# returns it modulo 4, so a leading NUL selects branch 0.

set -euo pipefail

cd "$(dirname "$0")"

mkdir -p corpus/reader_decode
find corpus/reader_decode -maxdepth 1 -name 'seed_*.avro' -delete
{ printf '\0'; cat ../benches/quickstop-null.avro; } \
    > corpus/reader_decode/seed_01_quickstop_null.avro

echo "Seeded:"
for d in corpus/*/; do
    count=$(find "$d" -maxdepth 1 -name '*.avro' | wc -l)
    printf "  %-40s %4d seeds\n" "$d" "$count"
done
