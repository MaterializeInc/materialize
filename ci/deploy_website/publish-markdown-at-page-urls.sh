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
# publish-markdown-at-page-urls.sh — copies each docs page's Markdown next to
# its HTML.
#
# Usage: publish-markdown-at-page-urls.sh SITE_DIR BASE_PATH
#
# SITE_DIR is a built docs site, served at BASE_PATH (such as "/docs"), whose
# Markdown pass wrote SITE_DIR/markdown-docs/. Each
# SITE_DIR/markdown-docs/<path>/index.md is copied to SITE_DIR/<path>/index.md,
# so a page's Markdown is also served at its page URL plus "index.md". An alias
# directory, whose index.html is a Hugo alias redirect, gets a copy of its
# target page's Markdown.
#
# Run it before `hugo deploy`, which deletes every object under the deployment
# target's include pattern that has no file in the local tree.

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 SITE_DIR BASE_PATH" >&2
    exit 1
fi
site=${1%/}
base=${2%/}
markdown=$site/markdown-docs

copy() {
    if [[ -e "$2" ]]; then
        echo "error: $2 already exists" >&2
        exit 1
    fi
    mkdir -p "$(dirname "$2")"
    cp "$1" "$2"
}

pages=0
while IFS= read -r -d '' src; do
    copy "$src" "$site/${src#"$markdown"/}"
    pages=$((pages + 1))
done < <(find "$markdown" -name index.md -type f -print0)

aliases=0
skipped=0
while IFS= read -r -d '' alias; do
    # The alias target is the page's permalink, such as "/docs/sql/select/", or
    # a full URL when the build's baseURL has a host.
    target=$(sed -nE 's/.*<!-- #HUGOALIAS# (.*) -->.*/\1/p' "$alias" | sed -E 's|^[a-z]+://[^/]+||')
    target=${target#"$base"}
    src=$markdown${target}index.md
    if [[ "$target" == /* && "$target" == */ && -f "$src" ]]; then
        copy "$src" "$(dirname "$alias")/index.md"
        aliases=$((aliases + 1))
    else
        skipped=$((skipped + 1))
    fi
done < <(grep -lr --null --exclude-dir=markdown-docs --include=index.html -F '#HUGOALIAS#' "$site")

echo "Published the Markdown of $pages pages and $aliases aliases at their page URLs."
echo "Skipped $skipped aliases whose target has no Markdown."
