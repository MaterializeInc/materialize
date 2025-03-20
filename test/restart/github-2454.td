# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Ensure Materialize can restart with this dependency graph
#

> CREATE TABLE t1 (f1 int4, f2 int4);
> CREATE VIEW v2 AS SELECT * FROM t1;
> CREATE INDEX i1 ON v2 (f1, f2);
> CREATE INDEX i2 ON t1 (f2);
