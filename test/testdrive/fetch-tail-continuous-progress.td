# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that TAIL WITH (progress=true) emits periodic progress
# messages even if there's no new data.
#

$ set-regex match=\d{13} replacement=<TIMESTAMP>

> CREATE TABLE t1 (f1 INTEGER);

> INSERT INTO t1 VALUES (123);

> BEGIN

> DECLARE c CURSOR FOR TAIL t1 WITH (progress=true);

# Verify there is a progress row available in the first batch.
> FETCH 2 c
<TIMESTAMP> false 1 123
<TIMESTAMP> true <null> <null>

# Now ask (and possibly wait) for another progress row.
> FETCH 1 c
<TIMESTAMP> true <null> <null>

> COMMIT
