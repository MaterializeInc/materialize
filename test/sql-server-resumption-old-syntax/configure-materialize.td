# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> DROP SECRET IF EXISTS sql_server_pass CASCADE;
> DROP CONNECTION IF EXISTS sql_server_test_connection CASCADE;

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}';
> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST toxiproxy,
    DATABASE test,
    PORT 1433,
    USER '${arg.default-sql-server-user}',
    PASSWORD SECRET sql_server_pass
  );

> DROP SOURCE IF EXISTS mz_source CASCADE;

> CREATE SOURCE mz_source
  FROM SQL SERVER
  CONNECTION sql_server_test_connection
  FOR ALL TABLES;
