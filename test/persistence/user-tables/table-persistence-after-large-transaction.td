# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT LENGTH(f1) FROM wide_transaction;
104857600

> SELECT COUNT(*), COUNT(DISTINCT f1) FROM long_transaction;
1000000 1000000
