# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Basic test to verify GCS blob storage is working

> CREATE TABLE gcs_basic_test (id INT, name TEXT);

> INSERT INTO gcs_basic_test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');

> SELECT * FROM gcs_basic_test ORDER BY id;
1 alice
2 bob
3 charlie

> CREATE MATERIALIZED VIEW gcs_basic_mv AS SELECT id, upper(name) as name_upper FROM gcs_basic_test;

> SELECT * FROM gcs_basic_mv ORDER BY id;
1 ALICE
2 BOB
3 CHARLIE

> DROP MATERIALIZED VIEW gcs_basic_mv;

> DROP TABLE gcs_basic_test;
