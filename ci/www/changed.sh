#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# changed.sh — determines whether Netlify should rebuild the website.

set -euo pipefail

cd "$(dirname "$0")/../.."

if [[ "$BRANCH" = master ]]; then
    spec=HEAD^
else
    git fetch https://"$DEPLOY_KEY"@github.com/MaterializeInc/materialize.git master
    spec=FETCH_HEAD...
fi

# The www build doesn't depend on submodules, so remove them from the working
# tree so they don't take up ~500MB of space in the Netlify cache.
# shellcheck disable=SC2016
git submodule foreach 'git -C $toplevel submodule deinit $sm_path'

# Netlify doesn't follow symlinks as of 03 Feb 2020, so we need to explicitly
# check for changes in doc/user too.
exec git diff --quiet "$spec" -- doc/user www 1>&2
