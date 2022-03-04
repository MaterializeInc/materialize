# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/materialize/issues/6305

> CREATE TABLE t1 (i1 INT8);

> BEGIN

> DECLARE c CURSOR FOR TAIL (SELECT * FROM t1) AS OF now() + '1hr'

> FETCH 0 c

> FETCH 0 c WITH (timeout='1h')

> COMMIT
