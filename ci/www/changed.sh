#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# changed.sh — determines whether Netlify should rebuild the website.

set -euo pipefail

cd "$(dirname "$0")/../.."

branch=$(git rev-parse --abbrev-ref HEAD)
if [[ "$branch" = master ]]; then
    spec=HEAD^
else
    spec=master...
fi

# Netlify doesn't follow symlinks as of 03 Feb 2020, so we need to explicitly
# check for changes in doc/user too.
exec git diff --quiet "$spec" -- doc/user www
