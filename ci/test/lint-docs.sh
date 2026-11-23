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

. misc/shlib/shlib.bash

git clean -ffdX ci/www/public ci/www/preview
try hugo --gc --baseURL https://ci.materialize.com/docs --source doc/user --destination ../../ci/www/public/docs
try hugo --gc --baseURL https://ci.materialize.com/docs --source doc/user --config config.toml,config.skill.toml --disableKinds 404,sitemap,robotsTXT,taxonomy --destination ../../ci/www/public/docs/markdown-docs
try python3 ci/test/test_check_docs_markdown.py
try python3 ci/test/check-docs-markdown.py ci/www/public/docs/markdown-docs https://ci.materialize.com/docs/markdown-docs/ --fail-on-links
# A base path other than /docs, like a preview's, catches link rewrites that
# confuse the base path with the production /docs/ prefix.
try hugo --gc --baseURL https://ci.materialize.com/materialize/lint --source doc/user --config config.toml,config.skill.toml --disableKinds 404,sitemap,robotsTXT,taxonomy --destination ../../ci/www/preview/markdown-docs
try python3 ci/test/check-docs-markdown.py ci/www/preview/markdown-docs https://ci.materialize.com/materialize/lint/markdown-docs/ --fail-on-links
echo "<!doctype html>" > ci/www/public/index.html
try htmltest -s ci/www/public -c doc/user/.htmltest.yml
try ci/test/lint-docs-catalog.sh
try ci/test/lint-metrics-catalog.sh
try ci/test/lint-skill-metrics.sh

try_status_report
