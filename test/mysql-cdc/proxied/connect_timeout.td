# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


> DROP SOURCE IF EXISTS mz_source CASCADE;
> DROP CONNECTION IF EXISTS mysql_conn CASCADE;
> DROP SECRET IF EXISTS mysql_pass;

# Override MySQL connect timeout to 3s
$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
$ postgres-execute connection=mz_system
ALTER SYSTEM SET mysql_source_connect_timeout = '3s';

# block connectivity to mysql
$ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
{
  "name": "mysql",
  "listen": "0.0.0.0:3306",
  "upstream": "mysql:3306",
  "enabled": true
}
$ http-request method=POST url=http://toxiproxy:8474/proxies/mysql/toxics content-type=application/json
{
  "name": "mysql",
  "type": "timeout",
  "attributes": { "delay": 0 }
}

> CREATE SECRET mysql_pass AS '${arg.mysql-root-password}';
! CREATE CONNECTION mysql_conn TO MYSQL (
    HOST "toxiproxy",
    USER root,
    PASSWORD SECRET mysql_pass
  );
contains:connection attempt timed out after 3s

$ http-request method=DELETE url=http://toxiproxy:8474/proxies/mysql
