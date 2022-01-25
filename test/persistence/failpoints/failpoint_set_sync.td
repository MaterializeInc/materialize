# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE TABLE t1 (f1 INTEGER);

#
# Introduce an I/O error and expect transactions to fail
#

> SET failpoints = 'fileblob_set_sync=return';

> BEGIN

> INSERT INTO t1 VALUES (1);

! COMMIT
contains:FileBlob::set sync fail point reached

> BEGIN

> INSERT INTO t1 VALUES (1);

! COMMIT
contains:FileBlob::set sync fail point reached

#
# Remove the I/O error and recover
#

> SET failpoints = 'fileblob_set_sync=off';

> BEGIN

> INSERT INTO t1 VALUES (1);

> COMMIT
