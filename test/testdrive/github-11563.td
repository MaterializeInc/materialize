# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that a distracted compute instance will not panic if asked to construct
# an index for a now-invalid source.

> CREATE MATERIALIZED VIEW test1 AS SELECT * FROM generate_series(1, 100000);

> CREATE TABLE foo (a int);

> CREATE INDEX baz ON foo (a);

> DROP INDEX baz;

> DROP TABLE foo;
