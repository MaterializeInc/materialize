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
# devsite.sh — deploys docs and apps to mtrlz.dev in CI.

set -euo pipefail

rsync misc/www/index.html buildkite@mtrlz.dev:/var/www/html/index.html

bin/doc
rsync --archive target/doc/ buildkite@mtrlz.dev:/var/www/html/api/rust

bin/doc --document-private-items
rsync --archive target/doc/ buildkite@mtrlz.dev:/var/www/html/api/rust-private

bin/pydoc
rsync --archive target/pydoc/ buildkite@mtrlz.dev:/var/www/html/api/python

ssh -A buildkite@mtrlz.dev <<'EOF'
set -euxo pipefail
cd /var/www/materialize/misc/civiz
setfacl -m www-data:x $(dirname "$SSH_AUTH_SOCK")
setfacl -m www-data:rwx "$SSH_AUTH_SOCK"
sudo -u www-data --preserve-env=SSH_AUTH_SOCK git fetch origin
sudo -u www-data --preserve-env=SSH_AUTH_SOCK git checkout main
sudo -u www-data --preserve-env=SSH_AUTH_SOCK git merge --ff-only origin/main
sudo -u www-data bash -c ". venv/bin/activate && python setup.py develop && alembic upgrade head"
sudo service civiz restart
EOF
