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

git clean -ffdX ci/www/public
hugo --gc --baseURL https://ci.materialize.com/docs --source doc/user --destination ../../ci/www/public/docs
# To avoid breaking URLs that already exist, pull the production sitemap
# and convert it to HTML. htmltest will scan the current prod paths and
# complain if any have been broken in this build.
curl https://materialize.com/docs/sitemap.xml -o ci/www/sitemap.xml
echo "<!doctype html>" > ci/www/public/index.html
sed 's/<loc>\(.*\)<\/loc>/<a href="\1"><\/a>/g' ci/www/sitemap.xml >> ci/www/public/index.html
htmltest -s ci/www/public -c doc/user/.htmltest.yml
