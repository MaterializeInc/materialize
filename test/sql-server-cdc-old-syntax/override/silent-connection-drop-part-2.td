# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# IMPORTANT: The Postgres server has a custom pg_hba.conf that only
# accepts connections from specific users. You will have to update
# pg_hba.conf if you modify the existing user names or add new ones.


$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO table_1 VALUES (4, 'four');

# re-enable cluster
> ALTER CLUSTER sconn_drop_cluster SET (REPLICATION FACTOR 1)

> SELECT * FROM table_1;
1 one
2 two
3 three
4 four
