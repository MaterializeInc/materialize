# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO upgrade_pg_cdc_table VALUES (11),(12),(13),(14),(15);

> SELECT * FROM upgrade_pg_cdc_table;
1
2
3
4
5
11
12
13
14
15

# Drop source so that the replication slot can be reclaimed.
> DROP SOURCE upgrade_pg_cdc_source CASCADE
