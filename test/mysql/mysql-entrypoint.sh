#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euo pipefail

export MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-p@ssw0rd}"

if [ ! -d /var/lib/mysql/mysql ]; then
    mkdir -p /var/lib/mysql
    cp -a /var/lib/mysql-seed/. /var/lib/mysql/
    chown -R mysql:mysql /var/lib/mysql
fi

exec docker-entrypoint.sh "$@"
