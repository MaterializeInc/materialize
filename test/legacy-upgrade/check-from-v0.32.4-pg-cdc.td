# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT * FROM cdc_empty_on_boot_table

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO cdc_int_table VALUES (11),(12),(13),(14),(15);
INSERT INTO cdc_enum_table VALUES ('var1'), ('var0');
INSERT INTO cdc_empty_on_boot_table VALUES (1),(2),(3);

> SELECT * FROM cdc_enum_table;
var0
var0
var1
var1

> SELECT * FROM cdc_int_table_for_tables;
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

> SELECT * FROM cdc_empty_on_boot_table;
1
2
3

# Drop source so that the replication slot can be reclaimed.
> DROP SOURCE upgrade_pg_cdc_source_for_all_tables CASCADE
> DROP SOURCE upgrade_pg_cdc_source_for_tables CASCADE
