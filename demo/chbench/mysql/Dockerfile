# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM mysql/mysql-server:8.0.27

COPY mysql.cnf /etc/mysql/conf.d/
COPY replication.sql /docker-entrypoint-initdb.d/

# The caching_sha2_password plugin requires the Python `cryptography` package
# on the client, which is a somewhat onerous dependency.
CMD ["mysqld", "--default-authentication-plugin=mysql_native_password"]
