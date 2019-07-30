#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# deploy.sh — deploys docs to mtrlz.dev in CI.

set -euo pipefail

bin/doc --no-rustup
rsync misc/www/index.html buildkite@mtrlz.dev:/var/www/html/index.html
rsync --archive target/doc/ buildkite@mtrlz.dev:/var/www/html/api
