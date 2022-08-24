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
# bump-change-date — updates the change date in the LICENSE file.

set -euo pipefail

git_date=$(date "+%B %d, %Y 00:00:00 UTC")
change_date=$(date -d "+4 years" "+%B %d, %Y")
version_date=$(date "+%Y%m%d")
year=$(date "+%Y")

export GIT_AUTHOR_DATE=$git_date
export GIT_COMMITTER_DATE=$git_date
export GIT_AUTHOR_NAME=Materialize Bot
export GIT_AUTHOR_EMAIL=infra+bot@materialize.com
export GIT_COMMITTER_NAME=$GIT_AUTHOR_NAME
export GIT_COMMITTER_EMAIL=$GIT_AUTHOR_EMAIL

git checkout main
git pull
sed -Ei \
    -e "s/Licensed Work:.*/Licensed Work:             Materialize Version $version_date/g" \
    -e "s/Change Date:.*/Change Date:               $change_date/g" \
    -e "s/The Licensed Work is © [0-9]{4}/The Licensed Work is © $year/g" \
    LICENSE
git add LICENSE
git commit -m "LICENSE: update change date"
git push "https://materializebot:$GITHUB_TOKEN@github.com/MaterializeInc/materialize.git" main
