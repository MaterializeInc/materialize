# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT LENGTH(f1) = 100 * 1024 * 1024 FROM wide_transaction;
true

> SELECT COUNT(*) = 1000000, COUNT(DISTINCT f1) = 1000000 FROM long_transaction;
true true
