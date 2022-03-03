# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
USE test;
CREATE TABLE t1 (f1 INTEGER PRIMARY KEY, f2 INTEGER);
INSERT INTO t1 VALUES (123, 123);
INSERT INTO t1 VALUES (234, 234);
COMMIT;
