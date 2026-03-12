#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -eo pipefail

export MYSQL_ALLOW_EMPTY_PASSWORD="${MYSQL_ALLOW_EMPTY_PASSWORD:-}"
export MYSQL_DATABASE="${MYSQL_DATABASE:-}"
export MYSQL_ONETIME_PASSWORD="${MYSQL_ONETIME_PASSWORD:-}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"
export MYSQL_RANDOM_ROOT_PASSWORD="${MYSQL_RANDOM_ROOT_PASSWORD:-}"
export MYSQL_ROOT_HOST="${MYSQL_ROOT_HOST:-%}"
export MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-p@ssw0rd}"
export MYSQL_USER="${MYSQL_USER:-}"

# shellcheck disable=SC1091
source /usr/local/bin/docker-entrypoint.sh

mysql_args=(
    mysqld
    --datadir=/var/lib/mysql-seed
    --socket=/tmp/mysql.sock
    --pid-file=/tmp/mysql.pid
    --secure-file-priv=/var/lib/mysql-files
    --log-bin=mysql-bin
    --gtid_mode=ON
    --enforce_gtid_consistency=ON
    --binlog-format=row
    --binlog-row-image=full
    --server-id=1
    --max-connections=500
)

mysql_check_config "${mysql_args[@]}"
docker_setup_env "${mysql_args[@]}"
docker_create_db_directories "${mysql_args[@]}"
docker_verify_minimum_env
docker_init_database_dir "${mysql_args[@]}"
docker_temp_server_start "${mysql_args[@]}"
mysql_socket_fix
docker_setup_db
docker_process_init_files /docker-entrypoint-initdb.d/*
mysql_expire_root_user
mysql --protocol=socket --socket=/tmp/mysql.sock -uroot -p"${MYSQL_ROOT_PASSWORD}" \
    -e "RESET BINARY LOGS AND GTIDS;"
docker_temp_server_stop

# Remove per-instance state so each container regenerates it on first start.
rm -f /var/lib/mysql-seed/auto.cnf
rm -f /var/lib/mysql-seed/mysql-bin.*
rm -f /var/lib/mysql-seed/*.pem
rm -f /tmp/mysql.pid /tmp/mysql.sock
