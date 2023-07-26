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

git clean -ffdX ci/www/public
ci_try hugo --gc --baseURL https://ci.materialize.com/docs --source doc/user --destination ../../ci/www/public/docs
echo "<!doctype html>" > ci/www/public/index.html
ci_try htmltest -s ci/www/public -c doc/user/.htmltest.yml
ci_try ci/test/lint-docs-catalog.sh

ci_status_report
