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
# lint.sh — checks for broken links and other HTML errors.

set -euo pipefail
# Save current branch so we can temp checkout main and switch back
export CURRENT_BRANCH=(git rev-parse --abbrev-ref HEAD)
git clean -ffdX ci/www/public
hugo --gc --baseURL https://ci.materialize.com/docs --source doc/user --destination ../../ci/www/public/docs
# To avoid breaking URLs that already exist, pull the production sitemap
# and convert it to HTML. htmltest will scan the current prod paths and
# complain if any have been broken in this build.
git checkout main
git pull
hugo --gc --source doc/user --destination tmp
echo "<!doctype html>" > ci/www/public/index.html
sed ... tmp/sitemap.xml >> ci/www/public/index.html
sed 's/<loc>\(.*\)<\/loc>/<a href="\1"><\/a>/g' ci/www/sitemap.xml >> ci/www/public/index.html
htmltest -s ci/www/public -c doc/user/.htmltest.yml
git checkout $(CURRENT_BRANCH)
