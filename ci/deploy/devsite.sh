#!/usr/bin/env bash

# Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# devsite.sh — deploys docs and apps to mtrlz.dev in CI.

set -euo pipefail

bin/doc --no-rustup
rsync misc/www/index.html buildkite@mtrlz.dev:/var/www/html/index.html
rsync --archive target/doc/ buildkite@mtrlz.dev:/var/www/html/api

ssh -A buildkite@mtrlz.dev <<'EOF'
set -euxo pipefail
cd /var/www/materialize/misc/civiz
setfacl -m www-data:x $(dirname "$SSH_AUTH_SOCK")
setfacl -m www-data:rwx "$SSH_AUTH_SOCK"
sudo -u www-data --preserve-env=SSH_AUTH_SOCK git pull
sudo -u www-data bash -c ". venv/bin/activate && python setup.py develop && alembic upgrade head"
sudo service civiz restart
EOF
