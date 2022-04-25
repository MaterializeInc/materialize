# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that mz_logical_timestamp and other functions with special
# handling whose code has been refactored repeatedly are not mangled
# during a restart
#

> CREATE TABLE special_functions (f1 BIGINT);

> CREATE MATERIALIZED VIEW special_functions_view AS SELECT * FROM special_functions where mz_logical_timestamp() > f1;

# Do not allow NOW / CURRENT_TIMESTAMP to be persisted at all
# if they ever are allowed, add an upgrade test for them

! CREATE MATERIALIZED VIEW foo AS SELECT NOW();
regex:(cannot be used in static queries|cannot materialize call to current_timestamp)

! CREATE MATERIALIZED VIEW bar AS SELECT CURRENT_TIMESTAMP();
regex:(cannot be used in static queries|cannot materialize call to current_timestamp)
