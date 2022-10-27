# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test adding even more data after we have to reconnect to the bastion.

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 VALUES (3);

> SELECT f1 FROM t1 ORDER BY f1 ASC;
1
1
2
3
