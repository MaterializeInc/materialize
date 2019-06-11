# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

# Test that a dependency chain with multiple links is properly maintained
# across creation and deletion.

> CREATE MATERIALIZED VIEW test1 AS SELECT 1;

> CREATE MATERIALIZED VIEW test2 AS SELECT * FROM test1;

> CREATE MATERIALIZED VIEW test3a AS SELECT * FROM test2;

> CREATE MATERIALIZED VIEW test3b AS SELECT * FROM test2;

! DROP VIEW test1;
cannot delete test1: still depended upon by dataflow 'test2'

! DROP VIEW test2;
cannot delete test2: still depended upon by dataflow 'test3a'

> DROP VIEW test3a;

! DROP VIEW test1;
cannot delete test1: still depended upon by dataflow 'test2'

! DROP VIEW test2;
cannot delete test2: still depended upon by dataflow 'test3b'

> DROP VIEW test3b;

! DROP VIEW test1;
cannot delete test1: still depended upon by dataflow 'test2'

> DROP VIEW test2;

> DROP VIEW test1;

# Test that CASCADE causes all dependent views to be dropped along with the
# named view.

> CREATE MATERIALIZED VIEW test1 AS SELECT 1;

> CREATE MATERIALIZED VIEW test2 AS SELECT * FROM test1;

> DROP VIEW test1 CASCADE;

# TODO(benesch): when we have a system stream that has the names of all
# available views, we can verify there that views are actually dropped,
# rather than verifying the drop by checking whether DROP VIEW fails.

! DROP VIEW test1;
dataflow test1 does not exist

! DROP VIEW test2;
dataflow test2 does not exist

# Test that DROP VIEW IF EXISTS succeeds even if the view does not exist.

> DROP VIEW IF EXISTS test1;

# Test that sinks cannot be depended upon.

> CREATE MATERIALIZED VIEW v1 AS SELECT 1;

> CREATE SINK s1 FROM v1 INTO 'kafka://${testdrive.kafka-addr}/v'

! CREATE MATERIALIZED VIEW v2 AS SELECT * FROM s1;
dataflow s1 is a sink and cannot be depended upon
