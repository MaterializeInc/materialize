# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Each added table re-renders the source and reseeds every export's
# offset_committed while the inserter job keeps t1 ahead of t2.

$ set-sql-timeout duration=60s

> CREATE TABLE t3 FROM SOURCE mz_source (REFERENCE dbo.t3)

> SELECT count(*) FROM t3
1

> CREATE TABLE t4 FROM SOURCE mz_source (REFERENCE dbo.t4)

> SELECT count(*) FROM t4
1

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
EXEC msdb.dbo.sp_stop_job N'Inserter';
