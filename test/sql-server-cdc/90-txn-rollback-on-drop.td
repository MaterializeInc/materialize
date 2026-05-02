# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test: Transaction::drop must send ROLLBACK.
#
# The Drop impl for Transaction previously called the async
# simple_query("ROLLBACK TRANSACTION") without .await. Since Rust async
# functions do nothing until polled, the ROLLBACK was never sent and the
# SQL Server session remained in an open transaction after drop.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

# Create a temp table scoped to this session.
$ sql-server-execute name=sql-server
CREATE TABLE #_test_txn_drop (val INT)

# Insert inside a transaction that is abandoned (exercises Transaction::drop).
$ sql-server-execute name=sql-server abandon-txn=true
INSERT INTO #_test_txn_drop VALUES (42)

# If Transaction::drop correctly sent ROLLBACK, @@TRANCOUNT is 0 and the
# insert was rolled back. If not, @@TRANCOUNT is 1 and the row is visible.
$ sql-server-execute name=sql-server split-lines=false
IF (SELECT @@TRANCOUNT) > 0
    THROW 50001, 'Transaction::drop did not send ROLLBACK! @@TRANCOUNT > 0', 1;
IF (SELECT COUNT(*) FROM #_test_txn_drop) > 0
    THROW 50002, 'Transaction::drop did not rollback! Rows still visible', 1;
