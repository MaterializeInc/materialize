# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the `max_query_heap_size` session variable.

$ set-regex match=\d{13,20} replacement=<TIMESTAMP>

> CREATE TABLE t1 (a int4, b text)

> INSERT INTO t1 SELECT * FROM generate_series(1, 10000), repeat('a', 100);

> SET max_query_heap_size = '1kB';

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE (SELECT max(a) FROM t1);

# The first timestamp is computed without detecting that the limit has been
# exceeded.
> FETCH 1 c;
<TIMESTAMP> 1 10000

# No output should be produced. Instead an error .. notice?
! FETCH 1 c;
contains:Dataflow limit exceeded

> ROLLBACK;

> SET max_query_heap_size = '128MB';

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE (SELECT max(a) FROM t1);

> FETCH 1 c;
<TIMESTAMP> 1 10000

> FETCH 1 FROM c WITH (TIMEOUT = '1s');

> ROLLBACK;
