# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

> SELECT COUNT(*) = 500000 FROM t1;
true
> SELECT COUNT(*) = 500000 FROM t2;
true
> SELECT COUNT(*) = 1000000 FROM t3;
true
> SELECT COUNT(*) = 2000010 FROM mz_source;
true
