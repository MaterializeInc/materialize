# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Signal the background ticker to stop. Its loop checks this flag once per
# second, so the ticker process exits shortly after.

$ sql-server-connect name=sql-server
server=tcp:${arg.azure-sql-host},${arg.azure-sql-port};Encrypt=true;TrustServerCertificate=true;User ID={${arg.azure-sql-user}};Password={${arg.azure-sql-password}};Database=${arg.azure-sql-database}

$ sql-server-execute name=sql-server
UPDATE dbo.mz_azure_ticker_control SET keep_running = 0;
