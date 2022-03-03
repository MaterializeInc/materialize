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
# generate.sh — generates railroad diagrams for the SQL grammar.

set -euo pipefail

cd "$(dirname "$0")"

dest=../layouts/partials/sql-grammar

# Clean up files from last run.
rm -rf scratch

# Run the railroad diagram generator, using a pinned version from our custom
# fork.
docker run --rm -i materialize/rr:v0.0.5 -nostyles -svg -noinline -width:600 - < sql-grammar.bnf > diagrams.zip

# Extract the SVGs we care about and move them into place.
mkdir scratch
(
    cd scratch
    unzip -j ../diagrams.zip
    rm ../diagrams.zip Railroad-Diagram-Generator.svg index.html
    for f in *; do
        # Rewrite any underscores in filenames to hyphens, for consistency with
        # other Hugo partials.
        if [[ $f = *_* ]]; then
            mv -f "$f" "${f//_/-}"
        fi
    done
)

rm -rf $dest
mv scratch $dest

# ping hugo again
sleep 3
touch "$dest"/*
