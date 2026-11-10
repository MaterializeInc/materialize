# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# How `duckdb-query` prints DuckDB values. Decimals keep their declared scale.

$ duckdb-query name=duckdb
SELECT -0.001::DECIMAL(4,3), 0::DECIMAL(10,2), 1.50::DECIMAL(9,2), 5::DECIMAL(10,0), 7::DECIMAL(38,0), 340282366920938463463374607431768211455::UHUGEINT
-0.001 0.00 1.50 5 7 340282366920938463463374607431768211455
