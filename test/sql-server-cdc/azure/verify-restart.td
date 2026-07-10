# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Run after Materialize is restarted, with --no-reset so the happy-path source
# survives. This exercises the runtime CDC re-validation on Azure SQL Database:
# the replication operator re-checks the SQL Server Agent and the progress
# operator re-queries restore history, both of which must be skipped for the
# Azure SQL Database engine edition or the source would error out on restart.

$ sql-server-connect name=sql-server
server=tcp:${arg.azure-sql-host},${arg.azure-sql-port};Encrypt=true;TrustServerCertificate=true;User ID={${arg.azure-sql-user}};Password={${arg.azure-sql-password}};Database=${arg.azure-sql-database}

# The source and its snapshot data survived the restart.
> SELECT * FROM mz_azure_t1;
a updated
c "anotha one"
d streamed

# A new upstream change must still propagate after the restart.
$ sql-server-execute name=sql-server
INSERT INTO mz_azure_t1 VALUES ('e', 'after restart');

> SELECT val_col FROM mz_azure_t1 WHERE key_col = 'e';
"after restart"
