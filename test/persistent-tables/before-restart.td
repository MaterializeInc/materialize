# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# The simplest smoke test
#

> CREATE TABLE t1 (f1 TIMESTAMP, f2 BYTEA);

> INSERT INTO t1 VALUES (NULL, NULL);

> INSERT INTO t1 VALUES ('2011-11-11 11:11:11', decode('1234', 'hex'));

#
# Make sure dropping a table does not break persistence of other tables
#

> CREATE TABLE to_be_dropped (f1 INTEGER);

> INSERT INTO to_be_dropped VALUES (1),(2),(3);

> DROP TABLE to_be_dropped;

#
# Make sure persisting the same table in different schema does not cause interference
#

> CREATE SCHEMA schema1;

> CREATE SCHEMA schema2;

> CREATE TABLE schema1.t1 (f1 TEXT);

> INSERT INTO schema1.t1 VALUES ('schema1');

> CREATE TABLE schema2.t1 (f1 TEXT);

> INSERT INTO schema2.t1 VALUES ('schema2');
