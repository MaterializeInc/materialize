# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that sources for which the timestamp field is zero can be TAIL-ed
#

> CREATE VIEW v1 AS SELECT 123;

> CREATE VIEW v2 AS VALUES (123);

> SELECT * FROM v1;
123

> SELECT * FROM v2;
123

> BEGIN

> DECLARE c1 CURSOR FOR TAIL v1 WITH (PROGRESS = TRUE);

> FETCH 2 FROM c1 WITH (timeout = '60s')
18446744073709551615 false 1 123

> COMMIT;

> BEGIN;

> DECLARE c2 CURSOR FOR TAIL v2 WITH (PROGRESS = TRUE);

> FETCH 2 FROM c2 WITH (timeout = '60s')
18446744073709551615 false 1 123

> COMMIT;

> BEGIN;

> DECLARE c1 CURSOR FOR TAIL v1 WITH (SNAPSHOT = FALSE);

> FETCH ALL FROM c1 WITH (timeout = '3s')
