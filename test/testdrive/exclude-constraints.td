# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# EXCLUDE CONSTRAINTS / EXCLUDE ALL CONSTRAINTS on `CREATE TABLE .. FROM
# SOURCE` is only supported for Postgres, MySQL, and SQL Server sources; other
# source types reject the options during purification. The per-source behavior
# is covered in test/pg-cdc/, test/mysql-cdc/, and test/sql-server-cdc/.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_exclude_constraints_option = true

> CREATE SOURCE auction_house
  FROM LOAD GENERATOR AUCTION (AS OF 300, UP TO 301) FOR ALL TABLES;

! CREATE TABLE bids_tbl FROM SOURCE auction_house (REFERENCE "auction"."bids")
  WITH (EXCLUDE ALL CONSTRAINTS);
contains:EXCLUDE CONSTRAINTS is not supported for load-generator sources

! CREATE TABLE bids_tbl FROM SOURCE auction_house (REFERENCE "auction"."bids")
  WITH (EXCLUDE CONSTRAINTS ('some_constraint'));
contains:EXCLUDE CONSTRAINTS is not supported for load-generator sources

# An empty exclusion list is equivalent to omitting the option, so it is
# accepted even on source types that do not support constraint exclusion.
> CREATE TABLE bids_tbl FROM SOURCE auction_house (REFERENCE "auction"."bids")
  WITH (EXCLUDE CONSTRAINTS ());

> SELECT count(*) > 0 FROM bids_tbl;
true

> DROP SOURCE auction_house CASCADE;
