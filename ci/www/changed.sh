#!/usr/bin/env bash

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
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
